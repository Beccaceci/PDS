# Simulazione 050 — SegmentedWal (capstone)

*Esattamente due tratti pubblici, come `TaskGraph` (046), `LockGraph` (048) e `SupervisorTree` (049) — ma qui si tocca l'apice della complessità nei motori di storage a basso livello (stile RocksDB, Apache Kafka e SQLite WAL): un Write-Ahead Log segmentato con rotazione automatica per capacità, reader lease concorrenti con conteggio dei pin, gestione della contropressione a soglia (Write-Stall) e compattazione a filigrana mobile (Low Watermark). La sfida architetturale non è solo sincronizzare letture e scritture, ma scoprire come la vita dei reader lease influenza attivamente la capacità del log di bonificare i segmenti obsoleti e come il loro distruttore RAII (`Drop`) debba risvegliare gli scrittori bloccati dalla contropressione.*

---

## SegmentedWal

Nei motori di storage ad alte prestazioni e nei database distribuiti, le mutazioni di stato vengono registrate in modo sequenziale e duraturo su un **Write-Ahead Log (WAL)** prima di essere applicate alle strutture dati in memoria. Per evitare che il file di log cresca all'infinito e per consentire la bonifica dello spazio obsoleto, il WAL è suddiviso in una sequenza ordinata di **segmenti**:

1. **Segmento Attivo**: è l'unico segmento in cui vengono accodate le nuove scritture (`append`). A ogni record viene assegnato un Log Sequence Number (**LSN**) strettamente crescente. Quando il segmento attivo raggiunge la capacità massima prefissata (`segment_capacity`), viene **sigillato** (`Sealed`) e ne viene istanziato uno nuovo come attivo.
2. **Segmenti Sigillati (`Sealed`)**: sono segmenti storici di sola lettura, memorizzati in ordine sequenziale.
3. **Reader Lease (`ReaderLease`)**: repliche di rete o consumatori analitici possono richiedere l'apertura di un lease di lettura a partire da un determinato LSN. Finchè un `ReaderLease` è in vita, esso **pinna** (tramite un conteggio di riferimenti o pin) il segmento su cui sta attualmente leggendo, impedendone la bonifica fisica.
4. **Compattazione e Low Watermark**:
   - Man mano che i dati vengono trasferiti su disco o consolidati, un thread di compattazione (`compact`) rimuove i segmenti sigillati più vecchi per liberare memoria.
   - Tuttavia, un segmento sigillato può essere fisicamente eliminato **solo se**:
     - Il suo LSN massimo è strettamente inferiore al **Low Watermark** (definito come il minimo LSN corrente tra tutti i `ReaderLease` attivi nel sistema);
     - Il suo conteggio di pin è pari a zero (nessun reader lease lo sta attivamente scandendo).
5. **Contropressione (Write-Stall)**:
   - Se il numero di segmenti sigillati non ancora compattati raggiunge la soglia di sicurezza `max_sealed_segments`, la memoria o lo storage rischiano la saturazione.
   - Il WAL attiva la contropressione: ogni successiva chiamata ad `append()` che richiederebbe la creazione di un nuovo segmento oltre la soglia **si blocca senza consumare cicli di CPU** finché una compattazione non bonifica almeno un segmento sigillato.

Si scrivano in Rust le strutture che implementano i tratti `ReaderLease` e `SegmentedWal` definiti di seguito.

---

### API richiesta

```rust
pub type Lsn = u64;
pub type SegmentId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub lsn: Lsn,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
    /// Il WAL è stato chiuso: non accetta nuove operazioni.
    WalClosed,
    /// L'LSN richiesto è già stato compattato ed eliminato fisicamente.
    LsnCompacted,
}

pub trait ReaderLease: Send {
    // Legge il prossimo record sequenziale a partire dalla posizione corrente del lease.
    // Restituisce:
    // - Ok(Some(record)) se il record successivo è disponibile, avanzando il cursore.
    // - Ok(None) se il reader ha raggiunto la fine attuale del log (senza bloccare).
    // - Err(WalError::WalClosed) se il WAL è stato chiuso.
    fn read_next(&self) -> Result<Option<Record>, WalError>;

    // Restituisce l'LSN dell'ultimo record letto con successo (oppure l'LSN iniziale
    // con cui il lease è stato aperto, se nessun record è stato ancora letto).
    fn current_lsn(&self) -> Lsn;

    // Restituisce l'ID del segmento attualmente pinnato da questo lease.
    fn pinned_segment_id(&self) -> SegmentId;
}

pub trait SegmentedWal: Clone + Send + Sync {
    // Accoda un payload nel segmento attivo, assegnando un nuovo LSN strettamente crescente.
    // Se il segmento attivo raggiunge segment_capacity, viene sigillato e ne viene creato uno nuovo.
    // Se il numero di segmenti sigillati ha raggiunto max_sealed_segments, questa chiamata
    // si blocca (senza consumare cicli di CPU) per Write-Stall finché una compattazione non
    // riduce i segmenti sigillati sotto la soglia.
    // Restituisce l'LSN assegnato al record.
    fn append(&self, payload: &[u8]) -> Result<Lsn, WalError>;

    // Apre un nuovo lease di lettura a partire dall'LSN specificato.
    // Restituisce Err(WalError::LsnCompacted) se quell'LSN è già stato eliminato dalla compattazione.
    // Altrimenti pinna il segmento contenente from_lsn e restituisce il ReaderLease.
    fn open_lease(&self, from_lsn: Lsn) -> Result<impl ReaderLease + 'static, WalError>;

    // Esegue la compattazione dei segmenti sigillati.
    // Calcola il Low Watermark (minimo LSN tra tutti i ReaderLease attivi, oppure l'LSN di inizio
    // del segmento attivo se non vi sono lease).
    // Elimina fisicamente tutti i segmenti sigillati il cui LSN massimo è strettamente inferiore
    // al Low Watermark e il cui pin_count è pari a 0.
    // Se la compattazione riduce i segmenti sigillati sotto max_sealed_segments, risveglia
    // eventuali scrittori bloccati in Write-Stall.
    // Restituisce il numero di segmenti fisicamente bonificati.
    fn compact(&self) -> usize;

    // Restituisce il Low Watermark corrente.
    fn low_watermark(&self) -> Lsn;

    // Restituisce il numero di segmenti sigillati attualmente residenti nel WAL.
    fn sealed_segment_count(&self) -> usize;

    // Restituisce l'ID del segmento attivo corrente.
    fn active_segment_id(&self) -> SegmentId;

    // Chiude il WAL: risveglia con errore eventuali scrittori bloccati in Write-Stall
    // e impedisce ulteriori operazioni.
    fn close(&self);
}

pub fn make_segmented_wal(
    segment_capacity: usize,
    max_sealed_segments: usize,
) -> impl SegmentedWal {
    ...
}
```

---

### Requisiti

- **Struttura dei Segmenti & Rotazione**:
  - Il segmento attivo iniziale ha `SegmentId = 0`.
  - Gli LSN partono da `1` e crescono strettamente in modo monotono (`1, 2, 3, ...`).
  - Quando il segmento attivo contiene esattamente `segment_capacity` record, la successiva `append()` deve prima sigillarlo (aggiungendolo alla coda dei segmenti sigillati) e avviare un nuovo segmento attivo con `SegmentId` incrementato.
- **Contropressione (Write-Stall)**:
  - Se, al momento di dover sigillare un segmento attivo per crearne uno nuovo, il numero di segmenti sigillati già presenti è $\ge$ `max_sealed_segments`, il thread chiamante di `append()` **deve bloccarsi senza consumare cicli di CPU**.
  - Lo sblocco deve avvenire non appena una chiamata a `compact()` rimuove uno o più segmenti sigillati riportando il conteggio sotto `max_sealed_segments`.
- **Reader Lease & Pinning**:
  - Più `ReaderLease` possono essere aperti contemporaneamente a LSN arbitrari (anche sovrapposti).
  - Un segmento con `pin_count > 0` non può MAI essere rimosso dalla compattazione, anche se il suo LSN massimo fosse inferiore al Low Watermark.
  - Quando un lease avanza nella lettura e passa a un segmento successivo, il pin sul segmento precedente viene rilasciato e viene acquisito il pin sul nuovo segmento.
- **Gestione RAII (`Drop`) del `ReaderLease`**:
  - Quando un `ReaderLease` esce dallo scope, il suo distruttore (`Drop`) deve:
    1. Rilasciare il pin sul segmento attualmente detenuto;
    2. Rimuovere il lease dal registro dei lease attivi del WAL;
    3. Ricalcolare il Low Watermark;
    4. Segnalare eventuali thread bloccati in attesa (compattatori o scrittori) se la chiusura del lease ha sbloccato condizioni critiche.
- **Chiusura Pulita (`close`)**:
  - Alla chiamata di `close()`, il WAL non accetta più nuove `append()` né `open_lease()`, e risveglia immediatamente tutti i thread bloccati in Write-Stall restituendo `Err(WalError::WalClosed)`.
- **Thread-Safety & Assenza di Busy-Waiting**:
  - Thread-safe, condivisibile (`Clone + Send + Sync`).
  - Nessuna attesa attiva (`Condvar::wait_while`).
  - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
  - Se il codice consegnato non compila, non verrà valutato.

---

### Suggerimenti implementativi

1. Ciascun segmento può essere rappresentato da una struttura interna contenente: `id: SegmentId`, `records: Vec<Record>`, `min_lsn: Lsn`, `max_lsn: Lsn`, e `pin_count: usize`.
2. Il gestore centrale del WAL può mantenere:
   - Il segmento attivo;
   - Una `VecDeque` di segmenti sigillati ordinati per LSN;
   - Una tabella dei lease attivi `HashMap<LeaseId, Lsn>` per ricalcolare istantaneamente il Low Watermark;
   - Una `Condvar` dedicata per gli scrittori in Write-Stall (`cvar_write_stall`).
3. La funzione `compact()` scorre i segmenti sigillati dal più vecchio: se `segment.max_lsn < low_watermark` e `segment.pin_count == 0`, il segmento viene rimosso con `pop_front()`. Appena terminata la rimozione, se `sealed_segments.len() < max_sealed_segments`, viene invocato `cvar_write_stall.notify_all()`.
4. Nel `ReaderLease`, memorizzare un `lease_id` univoco, l'LSN corrente, il `pinned_segment_id` e un `Arc` al manager condiviso. In `Drop`, accedere allo stato protetto per deregistrare il lease ed effettuare le notifiche.

---

## Meta-commentario

**Perché `SegmentedWal` rappresenta il culmine della complessità architetturale:**
Mentre `TaskGraph` (046) coordinava dipendenze statiche e `LockGraph` (048) gestiva contese dinamiche con cicli, `SegmentedWal` richiede di coordinare **tre assi ortogonali contemporaneamente**:
1. **Flusso dei dati in ingresso**: append sequenziale, generazione monotona di LSN e segmentazione;
2. **Flusso dei dati in uscita con cursori concorrenti**: letture asincrone indipendenti con pinning a conteggio di riferimenti e avanzamento dinamico del cursore;
3. **Ciclo di vita della memoria fisica**: contropressione con blocco degli scrittori (Write-Stall) e bonifica condizionale subordinata sia al Low Watermark dinamico sia ai pin dei lease.

**La scoperta architetturale chiave: il Low Watermark come stato emergente, non memorizzato:**
Il Low Watermark non può essere una semplice variabile aggiornata a comando. È un **valore emergente** calcolato sull'insieme di tutti i `ReaderLease` attivi. Se un lettore lento rimane fermo a LSN 10, l'intero sistema non può bonificare nessun segmento contenente LSN $\ge 10$, anche se mille altri lettori sono già a LSN 100.000. Non appena quel lettore lento termina ed esce dallo scope chiamando `Drop`, il Low Watermark compie un balzo istantaneo in avanti, sbloccando potenzialmente la compattazione di centinaia di segmenti e liberando gli scrittori bloccati dalla contropressione.

**Disaccoppiamento tra allocazione e contropressione:**
L'errore più comune consiste nel bloccare `append()` all'interno del lock di scrittura mentre si tenta di compattare. La contropressione e la compattazione devono rimanere separate: lo scrittore si sospende sulla `Condvar` della contropressione, permettendo a lettori e compattatori di acquisire il lock, fare progredire lo stato e risvegliarlo.

**Strutture cooperanti stimate (senza prescriverle):**
1. `WalInner`: contiene il segmento attivo, la coda dei segmenti sigillati, la mappa dei lease, il contatore LSN e il flag di chiusura.
2. `WalManager`: wrapper condivisibile `Arc<(Mutex<WalInner>, Condvar)>`.
3. `Segment`: contenitore dati del singolo blocco con metadati di LSN e contatore dei pin.
4. `MyReaderLease`: handle leggero con implementazione di `Drop` reattiva.

**Difficoltà stimata:** 5.0 / 5.0 (massimo livello della serie). Budget stimato: 130–160 minuti.
