//! # Simulazione 050 — SegmentedWal (capstone)
//!
//! Nei motori di storage e nei DBMS (RocksDB, Kafka, SQLite WAL), il Write-Ahead Log
//! registra le mutazioni di stato in modo sequenziale suddividendole in segmenti ordinati.
//!
//! Quando il segmento attivo raggiunge `segment_capacity`, viene sigillato (`Sealed`)
//! e ne viene aperto uno nuovo. Lettori concorrenti possono richiedere `ReaderLease`
//! che pinnano i segmenti storici per lettura sequenziale.
//!
//! La compattazione (`compact`) può bonificare fisicamente i segmenti sigillati solo se
//! il loro LSN massimo è strettamente inferiore al `low_watermark` (calcolato dinamicamente
//! come il minimo LSN dei lease attivi) e se il loro conteggio di pin è pari a zero.
//! Se i segmenti sigillati accumulati raggiungono `max_sealed_segments`, le scritture
//! entrano in Write-Stall e si sospendono senza consumare cicli di CPU finché la compattazione
//! non libera spazio.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `ReaderLease` e `SegmentedWal`.

use std::{collections::{HashMap, VecDeque}, sync::{Arc, Condvar, Mutex}};

pub type Lsn = u64;
pub type SegmentId = u64;

/// Record immagazzinato nel WAL contenente il proprio LSN e il payload arbitrario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub lsn: Lsn,
    pub payload: Vec<u8>,
}

/// Possibili errori restituiti durante le operazioni sul WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
    /// Il WAL è stato chiuso: non accetta nuove operazioni.
    WalClosed,
    /// L'LSN richiesto è già stato compattato ed eliminato fisicamente.
    LsnCompacted,
}

/// Tratto che rappresenta un lease di lettura sequenziale su uno o più segmenti del WAL.
pub trait ReaderLease: Send {
    /// Legge il prossimo record sequenziale a partire dalla posizione corrente del lease.
    /// Restituisce:
    /// - `Ok(Some(record))` se il record successivo è disponibile, avanzando il cursore.
    /// - `Ok(None)` se il reader ha raggiunto la fine attuale del log (senza bloccare).
    /// - `Err(WalError::WalClosed)` se il WAL è stato chiuso.
    fn read_next(&self) -> Result<Option<Record>, WalError>;

    /// Restituisce l'LSN dell'ultimo record letto con successo (oppure l'LSN iniziale
    /// con cui il lease è stato aperto, se nessun record è stato ancora letto).
    fn current_lsn(&self) -> Lsn;

    /// Restituisce l'ID del segmento attualmente pinnato da questo lease.
    fn pinned_segment_id(&self) -> SegmentId;
}

/// Tratto che rappresenta il gestore centrale del Write-Ahead Log segmentato.
pub trait SegmentedWal: Clone + Send + Sync {
    /// Accoda un payload nel segmento attivo, assegnando un nuovo LSN strettamente crescente.
    /// Se il segmento attivo raggiunge `segment_capacity`, viene sigillato e ne viene creato uno nuovo.
    /// Se il numero di segmenti sigillati ha raggiunto `max_sealed_segments`, questa chiamata
    /// si blocca (senza consumare cicli di CPU) per Write-Stall finché una compattazione non
    /// riduce i segmenti sigillati sotto la soglia.
    /// Restituisce l'LSN assegnato al record.
    fn append(&self, payload: &[u8]) -> Result<Lsn, WalError>;

    /// Apre un nuovo lease di lettura a partire dall'LSN specificato.
    /// Restituisce `Err(WalError::LsnCompacted)` se quell'LSN è già stato eliminato dalla compattazione.
    /// Altrimenti pinna il segmento contenente `from_lsn` e restituisce il `ReaderLease`.
    fn open_lease(&self, from_lsn: Lsn) -> Result<impl ReaderLease + 'static, WalError>;

    /// Esegue la compattazione dei segmenti sigillati.
    /// Calcola il Low Watermark (minimo LSN tra tutti i `ReaderLease` attivi, oppure l'LSN di inizio
    /// del segmento attivo se non vi sono lease).
    /// Elimina fisicamente tutti i segmenti sigillati il cui LSN massimo è strettamente inferiore
    /// al Low Watermark e il cui `pin_count` è pari a 0.
    /// Se la compattazione riduce i segmenti sigillati sotto `max_sealed_segments`, risveglia
    /// eventuali scrittori bloccati in Write-Stall.
    /// Restituisce il numero di segmenti fisicamente bonificati.
    fn compact(&self) -> usize;

    /// Restituisce il Low Watermark corrente.
    fn low_watermark(&self) -> Lsn;

    /// Restituisce il numero di segmenti sigillati attualmente residenti nel WAL.
    fn sealed_segment_count(&self) -> usize;

    /// Restituisce l'ID del segmento attivo corrente.
    fn active_segment_id(&self) -> SegmentId;

    /// Chiude il WAL: risveglia con errore eventuali scrittori bloccati in Write-Stall
    /// e impedisce ulteriori operazioni.
    fn close(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// ---------------------------------------------------------------------------------
// 📐 ARCHITETTURA DEL SISTEMA E RELAZIONI TRA LE STRUTTURE (SEGMENTED WAL PATTERN):
//
// 1. `Segment`:
//    - Unità atomica e immutabile (una volta sigillata) di memorizzazione sequenziale.
//    - Memorizza:
//      * `id: SegmentId`: identificativo univoco crescente (0, 1, 2, ...).
//      * `records: Vec<Record>`: buffer lineare dei record scritti nel segmento.
//      * `pin_count: usize`: contatore di quanti `ReaderLease` stanno attualmente
//        scandendo record appartenenti a questo segmento. Finché `pin_count > 0`,
//        il segmento NON può essere deallocato o compattato.
//
// 2. `WalState`:
//    - Stato centrale protetto dal lock unico del WAL (`Mutex<WalState>`):
//      * `active_segment: Segment`: l'unico segmento attualmente aperto in scrittura.
//      * `sealed_segments: VecDeque<Segment>`: coda ordinata dei segmenti storici
//        sigillati, ordinati dal più vecchio al più recente.
//      * `active_leases: HashMap<usize, Lsn>`: registro dei lease in vita con il loro
//        cursore corrente, fondamentale per calcolare istantaneamente il Low Watermark.
//      * `next_lsn: Lsn`: contatore strettamente crescente dei sequence number (parte da 1).
//      * `next_segment_id: SegmentId`: contatore per l'ID del prossimo segmento.
//      * `closed: bool`: flag atomico di chiusura del log.
//
// 3. `MySegmentedWal`:
//    - Coordinatore centrale thread-safe (`Clone + Send + Sync`).
//    - Incapsula `Arc<(Mutex<WalState>, Condvar, Condvar)>`:
//      * `cvar_readers`: per future estensioni o notifiche ai lettori.
//      * `cvar_writers`: per gestire la contropressione (Write-Stall) quando
//        `sealed_segments.len() >= max_sealed_segments`.
//
// 4. `MyLease`:
//    - Handle client-facing leggero che implementa `ReaderLease` e `Drop` (RAII).
//    - Quando il cursore attraversa il confine tra due segmenti, rilascia
//      atomicamente il pin dal vecchio segmento e acquisisce il pin sul nuovo.
//    - Nel distruttore `Drop`: deregistra il lease e decremente il pin del segmento
//      corrente, sbloccando l'avanzamento del Low Watermark per la compattazione.
// ---------------------------------------------------------------------------------

/// Struttura interna che rappresenta un singolo segmento del Write-Ahead Log.
pub struct Segment {
    pub id: SegmentId,
    pub records: Vec<Record>,
    pub pin_count: usize,
}

impl Segment {
    /// Inizializza un nuovo segmento vuoto con l'ID specificato.
    pub fn new(id: SegmentId) -> Self {
        Self {
            id,
            records: Vec::new(),
            pin_count: 0,
        }
    }

    /// Restituisce l'LSN del primo record contenuto nel segmento, se presente.
    pub fn min_lsn(&self) -> Option<Lsn> {
        self.records.first().map(|r| r.lsn)
    }

    /// Restituisce l'LSN dell'ultimo record contenuto nel segmento, se presente.
    pub fn max_lsn(&self) -> Option<Lsn> {
        self.records.last().map(|r| r.lsn)
    }
}

/// Stato globale sincronizzato del WAL gestito sotto Mutex.
pub struct WalState {
    pub active_segment: Segment,
    pub sealed_segments: VecDeque<Segment>,
    pub active_leases: HashMap<usize, Lsn>,
    pub next_lsn: Lsn,
    pub next_segment_id: SegmentId,
    pub next_lease_id: usize,
    pub closed: bool,
}

impl WalState {
    /// Inizializza lo stato iniziale del WAL con il segmento 0 attivo e vuoto.
    pub fn new() -> Self {
        Self {
            active_segment: Segment::new(0),
            sealed_segments: VecDeque::new(),
            active_leases: HashMap::new(),
            next_lsn: 1,
            next_segment_id: 1,
            next_lease_id: 0,
            closed: false,
        }
    }

    /// Calcola dinamicamente il Low Watermark del log:
    /// corrisponde al minimo LSN tra tutti i `ReaderLease` attivi nel sistema;
    /// se non vi è alcun lease aperto, corrisponde all'LSN di inizio del segmento attivo.
    pub fn compute_low_watermark(&self) -> Lsn {
        if let Some(&min_lease_lsn) = self.active_leases.values().min() {
            min_lease_lsn
        } else {
            self.active_segment
                .min_lsn()
                .unwrap_or(self.next_lsn)
        }
    }
}

/// Handle del lettore che implementa `ReaderLease`. Mantiene il pinning sui segmenti
/// e rilascia le risorse automaticamente tramite distruttore RAII (`Drop`).
pub struct MyLease {
    lease_id: usize,
    current_lsn: Arc<Mutex<Lsn>>,
    pinned_segment_id: Arc<Mutex<SegmentId>>,
    shared_wal: MySegmentedWal,
}

impl ReaderLease for MyLease {
    /// Legge il prossimo record sequenziale avanzando il cursore e migrando
    /// il pin di segmento in caso di attraversamento di confine.
    fn read_next(&self) -> Result<Option<Record>, WalError> {
        let (mutex, _, _) = &*self.shared_wal.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            return Err(WalError::WalClosed);
        }

        let mut cur_lsn_guard = self.current_lsn.lock().unwrap();
        let target_lsn = *cur_lsn_guard;

        // Se target_lsn non è ancora stato scritto nel WAL, abbiamo raggiunto la fine del log
        if target_lsn >= guard.next_lsn {
            return Ok(None);
        }

        // Cerchiamo il record nei segmenti sigillati o nel segmento attivo
        let mut found_record = None;
        let mut found_segment_id = None;

        for seg in guard.sealed_segments.iter() {
            if let (Some(min), Some(max)) = (seg.min_lsn(), seg.max_lsn()) {
                if target_lsn >= min && target_lsn <= max {
                    if let Some(rec) = seg.records.iter().find(|r| r.lsn == target_lsn) {
                        found_record = Some(rec.clone());
                        found_segment_id = Some(seg.id);
                        break;
                    }
                }
            }
        }

        if found_record.is_none() {
            if let Some(rec) = guard.active_segment.records.iter().find(|r| r.lsn == target_lsn) {
                found_record = Some(rec.clone());
                found_segment_id = Some(guard.active_segment.id);
            }
        }

        if let (Some(rec), Some(new_seg_id)) = (found_record, found_segment_id) {
            let mut pin_seg_guard = self.pinned_segment_id.lock().unwrap();
            let old_seg_id = *pin_seg_guard;

            // Se il record appartiene a un segmento successivo, migriamo il pin
            if old_seg_id != new_seg_id {
                // Rilascia pin dal vecchio segmento
                for seg in guard.sealed_segments.iter_mut() {
                    if seg.id == old_seg_id {
                        seg.pin_count = seg.pin_count.saturating_sub(1);
                        break;
                    }
                }
                if guard.active_segment.id == old_seg_id {
                    guard.active_segment.pin_count = guard.active_segment.pin_count.saturating_sub(1);
                }

                // Acquisisce pin sul nuovo segmento
                for seg in guard.sealed_segments.iter_mut() {
                    if seg.id == new_seg_id {
                        seg.pin_count += 1;
                        break;
                    }
                }
                if guard.active_segment.id == new_seg_id {
                    guard.active_segment.pin_count += 1;
                }

                *pin_seg_guard = new_seg_id;
            }

            // Avanza il cursore LSN e aggiorna il registro dei lease
            *cur_lsn_guard += 1;
            guard.active_leases.insert(self.lease_id, *cur_lsn_guard);

            Ok(Some(rec))
        } else {
            // L'LSN cercato è già stato compattato ed eliminato fisicamente
            Err(WalError::LsnCompacted)
        }
    }

    /// Restituisce l'LSN del prossimo record da leggere o dell'ultimo record letto.
    fn current_lsn(&self) -> Lsn {
        *self.current_lsn.lock().unwrap()
    }

    /// Restituisce l'ID del segmento attualmente trattenuto dal pin di questo lease.
    fn pinned_segment_id(&self) -> SegmentId {
        *self.pinned_segment_id.lock().unwrap()
    }
}

impl Drop for MyLease {
    /// Distruttore RAII: unpinna il segmento attualmente trattenuto e deregistra
    /// il lease, permettendo al Low Watermark di avanzare durante la compattazione.
    fn drop(&mut self) {
        let (mutex, _, _) = &*self.shared_wal.inner;
        let mut guard = mutex.lock().unwrap();

        let pinned_seg = *self.pinned_segment_id.lock().unwrap();

        // Rilascia il pin dal segmento
        for seg in guard.sealed_segments.iter_mut() {
            if seg.id == pinned_seg {
                seg.pin_count = seg.pin_count.saturating_sub(1);
                break;
            }
        }
        if guard.active_segment.id == pinned_seg {
            guard.active_segment.pin_count = guard.active_segment.pin_count.saturating_sub(1);
        }

        // Rimuove il lease dal registro
        guard.active_leases.remove(&self.lease_id);
    }
}

/// Gestore principale del Write-Ahead Log segmentato con rotazione e contropressione.
#[derive(Clone)]
pub struct MySegmentedWal {
    inner: Arc<(Mutex<WalState>, Condvar, Condvar)>, // (state, cvar_readers, cvar_writers)
    segment_capacity: usize,
    max_sealed_segments: usize,
}

impl SegmentedWal for MySegmentedWal {
    /// Accoda un nuovo payload nel log assegnandogli un LSN strettamente crescente.
    /// Se il segmento attivo raggiunge la capacità prefissata, attiva la rotazione
    /// e si blocca in Write-Stall se la soglia di segmenti sigillati è satura.
    fn append(&self, payload: &[u8]) -> Result<Lsn, WalError> {
        let (mutex, _, cvar_writers) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            return Err(WalError::WalClosed);
        }

        // Se il segmento attivo ha raggiunto la capacità, dobbiamo sigillarlo
        while guard.active_segment.records.len() >= self.segment_capacity {
            // Write-Stall: se abbiamo già raggiunto max_sealed_segments, attendiamo senza CPU spin
            if guard.sealed_segments.len() >= self.max_sealed_segments {
                guard = cvar_writers
                    .wait_while(guard, |c| {
                        !c.closed && c.sealed_segments.len() >= self.max_sealed_segments
                    })
                    .unwrap();

                if guard.closed {
                    return Err(WalError::WalClosed);
                }
            } else {
                // Rotazione del segmento: il vecchio attivo diventa sigillato
                let next_id = guard.next_segment_id;
                guard.next_segment_id += 1;
                let old_active = std::mem::replace(&mut guard.active_segment, Segment::new(next_id));
                guard.sealed_segments.push_back(old_active);
                break;
            }
        }

        let lsn = guard.next_lsn;
        guard.next_lsn += 1;

        guard.active_segment.records.push(Record {
            lsn,
            payload: payload.to_vec(),
        });

        Ok(lsn)
    }

    /// Apre un nuovo lease di lettura a partire da un LSN specifico.
    /// Restituisce `Err(WalError::LsnCompacted)` se il record è già stato bonificato.
    fn open_lease(&self, from_lsn: Lsn) -> Result<impl ReaderLease + 'static, WalError> {
        let (mutex, _, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            return Err(WalError::WalClosed);
        }

        // Verifichiamo a quale segmento appartiene from_lsn
        let mut found_seg_id = None;

        for seg in guard.sealed_segments.iter_mut() {
            if let (Some(min), Some(max)) = (seg.min_lsn(), seg.max_lsn()) {
                if from_lsn >= min && from_lsn <= max {
                    seg.pin_count += 1;
                    found_seg_id = Some(seg.id);
                    break;
                }
            }
        }

        if found_seg_id.is_none() {
            if let (Some(min), _) = (guard.active_segment.min_lsn(), guard.active_segment.max_lsn()) {
                if from_lsn >= min {
                    guard.active_segment.pin_count += 1;
                    found_seg_id = Some(guard.active_segment.id);
                }
            } else if from_lsn == guard.next_lsn {
                // Segmento attivo vuoto, ma from_lsn corrisponde al prossimo LSN
                guard.active_segment.pin_count += 1;
                found_seg_id = Some(guard.active_segment.id);
            }
        }

        if let Some(pinned_seg_id) = found_seg_id {
            let lease_id = guard.next_lease_id;
            guard.next_lease_id += 1;
            guard.active_leases.insert(lease_id, from_lsn);

            Ok(MyLease {
                lease_id,
                current_lsn: Arc::new(Mutex::new(from_lsn)),
                pinned_segment_id: Arc::new(Mutex::new(pinned_seg_id)),
                shared_wal: self.clone(),
            })
        } else {
            Err(WalError::LsnCompacted)
        }
    }

    /// Esegue la compattazione dei segmenti sigillati più vecchi.
    /// Bonifica fisicamente solo i segmenti con `max_lsn < low_watermark` e `pin_count == 0`.
    /// Risveglia gli scrittori bloccati in Write-Stall se lo spazio si libera.
    fn compact(&self) -> usize {
        let (mutex, _, cvar_writers) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            return 0;
        }

        let lwm = guard.compute_low_watermark();
        let mut reclaimed = 0;

        while let Some(front) = guard.sealed_segments.front() {
            if let Some(max_lsn) = front.max_lsn() {
                if max_lsn < lwm && front.pin_count == 0 {
                    guard.sealed_segments.pop_front();
                    reclaimed += 1;
                    continue;
                }
            }
            break;
        }

        // Se abbiamo bonificato segmenti e siamo scesi sotto la soglia, sblocchiamo gli scrittori
        if reclaimed > 0 && guard.sealed_segments.len() < self.max_sealed_segments {
            cvar_writers.notify_all();
        }

        reclaimed
    }

    /// Restituisce il Low Watermark corrente calcolato dal WAL.
    fn low_watermark(&self) -> Lsn {
        let (mutex, _, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.compute_low_watermark()
    }

    /// Restituisce il numero di segmenti sigillati attualmente mantenuti in memoria.
    fn sealed_segment_count(&self) -> usize {
        let (mutex, _, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.sealed_segments.len()
    }

    /// Restituisce l'ID del segmento attualmente attivo in scrittura.
    fn active_segment_id(&self) -> SegmentId {
        let (mutex, _, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.active_segment.id
    }

    /// Chiude il log: risveglia con `WalError::WalClosed` tutti i thread in Write-Stall
    /// e impedisce l'accodamento di ulteriori record.
    fn close(&self) {
        let (mutex, _, cvar_writers) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        cvar_writers.notify_all();
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza un nuovo Write-Ahead Log segmentato con capacità di segmento e soglia di Write-Stall.
pub fn make_segmented_wal(
    segment_capacity: usize,
    max_sealed_segments: usize,
) -> impl SegmentedWal {
    MySegmentedWal {
        inner: Arc::new((Mutex::new(WalState::new()), Condvar::new(), Condvar::new())),
        segment_capacity,
        max_sealed_segments,
    }
}