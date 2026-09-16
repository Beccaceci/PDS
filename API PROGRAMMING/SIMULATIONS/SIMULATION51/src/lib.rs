//! # Simulazione 051 — CacheBus (capstone)
//!
//! Nelle architetture multi-core simmetriche (SMP), la coerenza tra le memorie cache L1
//! private dei core viene garantita a livello hardware tramite il protocollo MESI
//! (Modified, Exclusive, Shared, Invalid) e lo snooping continuo sul bus di memoria condiviso.
//!
//! Le letture in Cache Miss (`Invalid`) emettono `BusRd`, provocando il flush di eventuali linee
//! sporche (`Modified`) e il downgrade a `Shared`. Le scritture emettono `BusUpgr` o `BusRdX`,
//! forzando l'invalidazione (`Invalid`) di tutte le copie condivise negli altri core.
//!
//! Gli store buffer locali consentono scritture asincrone ad alta velocità: l'invocazione
//! di una barriera di memoria (`memory_barrier` / `smp_mb`) sospende il chiamante senza
//! consumare cicli di CPU finché tutte le scritture pendenti non sono state drenate e confermate.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `CoreCache` e `MemoryBus`.

use std::{collections::{HashMap, HashSet, VecDeque}, sync::{Arc, Condvar, Mutex, atomic::{AtomicUsize, Ordering::SeqCst}}};

use crate::MesiState::{Exclusive, Invalid, Modified, Shared};

pub type CoreId = u64;
pub type Address = u64;
pub type Value = u64;

/// Stato di una linea di cache secondo il protocollo di coerenza MESI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MesiState {
    /// Linea modificata solo in questa cache (dirty, possesso esclusivo).
    Modified,
    /// Linea pulita presente solo in questa cache (clean, possesso esclusivo).
    Exclusive,
    /// Linea pulita presente in questa cache e potenzialmente in altre (clean, condivisa).
    Shared,
    /// Linea non valida (dati obsoleti o assenti).
    Invalid,
}

/// Errori operativi restituiti dalle interfacce di cache e bus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// Il core è stato disconnesso o il bus è stato arrestato.
    CoreDisconnected,
}

/// Tratto che rappresenta la cache L1 locale associata a un core.
pub trait CoreCache: Send + Sync {
    /// Legge il valore associato all'indirizzo specificato.
    /// - Cache Hit (M, E, S): restituisce immediatamente il valore locale senza transazioni di bus.
    /// - Cache Miss (I): emette BusRd sul bus, gestisce il flush/downgrade degli altri core e installa la linea come Shared o Exclusive.
    fn read(&self, addr: Address) -> Result<Value, BusError>;

    /// Scrive il valore all'indirizzo specificato.
    /// - M: sovrascrive localmente.
    /// - E: transita silenziosamente a M e scrive.
    /// - S: emette BusUpgr sul bus invalidando gli altri core (S -> I) e transita a M.
    /// - I: emette BusRdX sul bus invalidando gli altri core (con flush se dirty) e transita a M.
    fn write(&self, addr: Address, val: Value) -> Result<(), BusError>;

    /// Blocca il chiamante, senza consumare cicli di CPU, finché tutte le scritture
    /// pendenti nello store buffer locale non sono state completamente drenate e confermate sul bus (smp_mb).
    fn memory_barrier(&self);

    /// Restituisce lo stato MESI attuale della linea di cache contenente l'indirizzo.
    fn line_state(&self, addr: Address) -> MesiState;

    /// Restituisce il numero di scritture attualmente pendenti nello store buffer locale.
    fn pending_store_count(&self) -> usize;
}

/// Tratto che rappresenta il coordinatore del bus di memoria a broadcast con snooping.
pub trait MemoryBus: Clone + Send + Sync {
    /// Connette un nuovo core al bus di memoria condiviso, restituendo l'ID univoco
    /// assegnato e il rispettivo handle CoreCache.
    fn attach_core(&self) -> (CoreId, impl CoreCache + 'static);

    /// Restituisce il valore attualmente memorizzato nella memoria principale (RAM)
    /// per quell'indirizzo.
    fn read_main_memory(&self, addr: Address) -> Value;

    /// Restituisce il conteggio cumulativo di transazioni di broadcast trasmesse sul bus (BusRd, BusRdX, BusUpgr).
    fn total_bus_transactions(&self) -> usize;

    /// Restituisce il conteggio cumulativo di linee invalidate negli altri core tramite snooping.
    fn total_invalidations(&self) -> usize;

    /// Restituisce il numero di core attualmente connessi e attivi.
    fn active_core_count(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// ---------------------------------------------------------------------------------
// 📐 ARCHITETTURA DEL SISTEMA E PROTOCOLLO MESI (CACHE BUS PATTERN):
//
// 1. `CoreState`:
//    - Stato privato interno di ciascun core protetto dal lock locale `Mutex<CoreState>`:
//      * `map: HashMap<Address, (Value, MesiState)>`: le linee di cache locali con il
//        loro stato di coerenza (`Modified`, `Exclusive`, `Shared`, `Invalid`).
//      * `store_buffer: VecDeque<(Address, Value)>`: buffer FIFO delle scritture asincrone
//        in attesa di consolidamento sul bus alla barriera di memoria (`smp_mb`).
//
// 2. `MyCore`:
//    - Handle client-facing che implementa il tratto `CoreCache` e `Drop`.
//    - Contiene il proprio `core_id`, il riferimento `Arc` al proprio `CoreState`
//      e il clone del bus condiviso `MyMemoryBus`.
//    - Gerarchia dei lock per evitare DEADLOCK (Lock Ordering):
//      * Per evitare cicli ABBA tra il lock locale del core e il lock globale del bus:
//        non si invoca MAI un metodo del bus tenendo acquisito il lock del core!
//      * Durante `read()` e `memory_barrier()`, si estrae il lavoro localmente,
//        si rilascia il lock del core, si esegue la transazione sul bus, e infine
//        si riacquisisce il lock locale per salvare i risultati.
//    - Gestione RAII (`Drop`):
//      * Drena preventivamente lo store buffer tramite `memory_barrier()`.
//      * Effettua il flush delle linee rimaste in stato `Modified` nella memoria centrale.
//      * Deregistra il core da `BusState::cores`, riducendo `active_core_count()`.
//
// 3. `BusState` & `MyMemoryBus`:
//    - Coordinatore centrale del bus di memoria multi-core:
//      * `main_memory: HashMap<Address, Value>`: la memoria centrale (RAM fisica).
//      * `cores: HashMap<CoreId, Arc<(Mutex<CoreState>, Condvar)>>`: registro dei core
//        attivi per effettuare lo snooping concorrente.
//      * Contatori atomici `num_bus_transactions` e `num_invalidations`.
//
// 🔄 TRANSAZIONI DEL BUS E PROTOCOLLO MESI:
//    - `BusRd`: Read Miss emesso su linea `Invalid`. Lo snooping verifica se altri core
//      hanno la linea: se uno la possedeva in `Modified`, la flussha in RAM e degrada a `Shared`.
//      Se un altro la possedeva in `Exclusive`, degrada a `Shared`. Il richiedente
//      installa in `Shared` se condivisa, oppure `Exclusive` se unico possessore.
//    - `BusUpgr`: Upgrade Write emesso su linea `Shared`. Invalida le copie in tutti gli altri
//      core (`Shared -> Invalid`) senza rileggere la memoria. Il core passa a `Modified`.
//    - `BusRdX`: Read-with-Intent-to-Modify su linea `Invalid`. Invalida tutti gli altri core
//      (con flush in RAM se uno era in `Modified`) e il richiedente installa in `Modified`.
// ---------------------------------------------------------------------------------

/// Stato interno privato della cache di un singolo core.
pub struct CoreState {
    map: HashMap<Address, (Value, MesiState)>,
    store_buffer: VecDeque<(Address, Value)>,
}

impl CoreState {
    /// Inizializza un nuovo stato di cache vuoto.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            store_buffer: VecDeque::new(),
        }
    }
}

/// Handle del core della CPU che implementa `CoreCache`.
pub struct MyCore {
    core_id: CoreId,
    state: Arc<(Mutex<CoreState>, Condvar)>,
    shared_bus: MyMemoryBus,
}

impl CoreCache for MyCore {
    /// Legge il valore associato all'indirizzo specificato secondo il protocollo MESI.
    /// 1. Store Forwarding: se lo store buffer locale contiene una scrittura pendente, la restituisce subito.
    /// 2. Cache Hit (M, E, S): restituisce immediatamente il valore presente in cache locale.
    /// 3. Cache Miss (I): emette BusRd sul bus rilasciando il lock locale per prevenire deadlock,
    ///    installa la linea come Shared o Exclusive e restituisce il valore aggiornato.
    fn read(&self, addr: Address) -> Result<Value, BusError> {
        let (mutex, _) = &*self.state;

        // 1. Store Forwarding: controlliamo se c'è una scrittura pendente nello store buffer locale
        {
            let guard = mutex.lock().unwrap();
            for &(a, val) in guard.store_buffer.iter().rev() {
                if a == addr {
                    return Ok(val);
                }
            }

            // Cache Hit su linea valida (Modified, Exclusive, Shared)
            if let Some(&(val, state)) = guard.map.get(&addr) {
                if state != MesiState::Invalid {
                    return Ok(val);
                }
            }
        }

        // 2. Cache Miss: dobbiamo emettere BusRd sul bus condiviso.
        // Rilasciamo il lock locale del core prima di invocare il bus per evitare deadlock con lo snooping!
        let (does_anyone_else, val) = self.shared_bus.bus_rd(self.core_id, addr);

        // 3. Riprendiamo il lock locale e installiamo la linea con il suo nuovo stato
        let mut guard = mutex.lock().unwrap();
        let new_state = if does_anyone_else {
            MesiState::Shared
        } else {
            MesiState::Exclusive
        };

        guard.map.insert(addr, (val, new_state));
        Ok(val)
    }

    /// Accoda la scrittura nello Store Buffer locale in modo asincrono.
    /// La scrittura verrà consolidata nella cache e propagata sul bus alla memory barrier.
    fn write(&self, addr: Address, val: Value) -> Result<(), BusError> {
        let (mutex, _) = &*self.state;
        let mut guard = mutex.lock().unwrap();
        guard.store_buffer.push_back((addr, val));
        Ok(())
    }

    /// Barriera di memoria (`smp_mb`): drena completamente lo Store Buffer locale,
    /// emettendo le transazioni di bus richieste (`BusUpgr` o `BusRdX`) e consolidando
    /// i valori nella cache locale in stato `Modified`.
    fn memory_barrier(&self) {
        let (mutex, cvar) = &*self.state;

        loop {
            // Estraiamo la prossima operazione rilasciando il lock prima di chiamare il bus
            let next_op = {
                let mut guard = mutex.lock().unwrap();
                guard.store_buffer.pop_front()
            };

            match next_op {
                Some((addr, new_value)) => {
                    // Controlliamo lo stato attuale della linea
                    let current_state = {
                        let guard = mutex.lock().unwrap();
                        guard
                            .map
                            .get(&addr)
                            .map(|&(_, state)| state)
                            .unwrap_or(MesiState::Invalid)
                    };

                    match current_state {
                        MesiState::Modified | MesiState::Exclusive => {
                            // Aggiornamento silenzioso: siamo già gli unici possessori
                        }
                        MesiState::Shared => {
                            // Emettiamo BusUpgr senza tenere il lock locale
                            self.shared_bus.bus_upgr(self.core_id, addr);
                        }
                        MesiState::Invalid => {
                            // Emettiamo BusRdX senza tenere il lock locale
                            self.shared_bus.bus_rdx(self.core_id, addr);
                        }
                    }

                    // Consolidiamo il valore aggiornato in stato Modified nella cache locale
                    let mut guard = mutex.lock().unwrap();
                    guard.map.insert(addr, (new_value, MesiState::Modified));
                }
                None => break,
            }
        }

        cvar.notify_all();
    }

    /// Restituisce lo stato MESI attuale della linea di cache contenente l'indirizzo.
    fn line_state(&self, addr: Address) -> MesiState {
        let (mutex, _) = &*self.state;
        let guard = mutex.lock().unwrap();
        guard
            .map
            .get(&addr)
            .map(|&(_, state)| state)
            .unwrap_or(MesiState::Invalid)
    }

    /// Restituisce il numero di scritture attualmente pendenti nello store buffer locale.
    fn pending_store_count(&self) -> usize {
        let (mutex, _) = &*self.state;
        let guard = mutex.lock().unwrap();
        guard.store_buffer.len()
    }
}

impl Clone for MyCore {
    /// Clona l'handle del core condividendo lo stesso stato locale e bus.
    fn clone(&self) -> Self {
        Self {
            core_id: self.core_id,
            state: self.state.clone(),
            shared_bus: self.shared_bus.clone(),
        }
    }
}

impl PartialEq for MyCore {
    fn eq(&self, other: &Self) -> bool {
        self.core_id == other.core_id
    }
}

impl Eq for MyCore {}

impl std::hash::Hash for MyCore {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.core_id.hash(state);
    }
}

impl Drop for MyCore {
    /// Distruttore RAII del Core:
    /// Drena lo store buffer, flussha le linee `Modified` in memoria centrale
    /// e deregistra il core dal bus.
    fn drop(&mut self) {
        // Se rimangono solo 2 strong count (uno è self, uno è nel registro del bus)
        // significa che questo è l'ultimo handle esterno attivo del core che viene droppato!
        if Arc::strong_count(&self.state) <= 2 {
            self.memory_barrier();

            let (mutex, _) = &*self.state;
            let guard = mutex.lock().unwrap();
            let mut modified_lines = Vec::new();
            for (&addr, &(val, state)) in guard.map.iter() {
                if state == MesiState::Modified {
                    modified_lines.push((addr, val));
                }
            }
            drop(guard);

            // Scriviamo le linee sporche nella memoria centrale e deregistriamo il core
            let mut guard_bus = self.shared_bus.inner.lock().unwrap();
            for (addr, val) in modified_lines {
                guard_bus.main_memory.insert(addr, val);
            }
            guard_bus.cores.remove(&self.core_id);
        }
    }
}

/// Stato sincronizzato del bus di memoria protetto da Mutex.
pub struct BusState {
    main_memory: HashMap<Address, Value>,
    cores: HashMap<CoreId, Arc<(Mutex<CoreState>, Condvar)>>,
    next_core_id: CoreId,
}

impl BusState {
    /// Inizializza un nuovo bus vuoto.
    pub fn new() -> Self {
        Self {
            main_memory: HashMap::new(),
            cores: HashMap::new(),
            next_core_id: 0,
        }
    }

    /// Esegue lo snooping per una transazione `BusRd`:
    /// Se un altro core detiene la linea in `Modified`, ne fa il flush in RAM e degrada a `Shared`.
    /// Se un altro core la detiene in `Exclusive`, degrada a `Shared`.
    /// Restituisce `(true, valore)` se almeno un altro core detiene la linea, `(false, valore)` altrimenti.
    pub fn apply_busrd(&mut self, source_core_id: CoreId, address: Address) -> (bool, Value) {
        let mut does_anyone = false;
        let core_entries: Vec<_> = self
            .cores
            .iter()
            .filter(|(&id, _)| id != source_core_id)
            .map(|(_, state)| state.clone())
            .collect();

        for state in core_entries {
            let (mutex_core, _) = &*state;
            let mut guard_core = mutex_core.lock().unwrap();
            if let Some((value, state)) = guard_core.map.get_mut(&address) {
                if *state != MesiState::Invalid {
                    does_anyone = true;
                    if *state == MesiState::Modified {
                        self.main_memory.insert(address, *value);
                        *state = MesiState::Shared;
                    } else if *state == MesiState::Exclusive {
                        *state = MesiState::Shared;
                    }
                }
            }
        }

        let val = self.main_memory.get(&address).copied().unwrap_or(0);
        (does_anyone, val)
    }

    /// Esegue lo snooping per una transazione `BusUpgr`:
    /// Invalida le copie della linea presenti negli altri core (`Shared -> Invalid`).
    /// Restituisce il numero di invalidazioni effettuate.
    pub fn apply_busupgr(&mut self, source_core_id: CoreId, address: Address) -> usize {
        let mut num_invalidations = 0usize;
        let core_entries: Vec<_> = self
            .cores
            .iter()
            .filter(|(&id, _)| id != source_core_id)
            .map(|(_, state)| state.clone())
            .collect();

        for state in core_entries {
            let (mutex_core, _) = &*state;
            let mut guard_core = mutex_core.lock().unwrap();
            if let Some((_, state)) = guard_core.map.get_mut(&address) {
                if *state != MesiState::Invalid {
                    num_invalidations += 1;
                    *state = MesiState::Invalid;
                }
            }
        }

        num_invalidations
    }

    /// Esegue lo snooping per una transazione `BusRdX`:
    /// Invalida le copie in tutti gli altri core; se un core era `Modified`, flussha prima in RAM.
    /// Restituisce il numero di invalidazioni effettuate.
    pub fn apply_busrdx(&mut self, source_core_id: CoreId, address: Address) -> usize {
        let mut num_invalidations = 0usize;
        let core_entries: Vec<_> = self
            .cores
            .iter()
            .filter(|(&id, _)| id != source_core_id)
            .map(|(_, state)| state.clone())
            .collect();

        for state in core_entries {
            let (mutex_core, _) = &*state;
            let mut guard_core = mutex_core.lock().unwrap();
            if let Some((value, state)) = guard_core.map.get_mut(&address) {
                if *state == MesiState::Modified {
                    self.main_memory.insert(address, *value);
                }

                if *state != MesiState::Invalid {
                    num_invalidations += 1;
                    *state = MesiState::Invalid;
                }
            }
        }

        num_invalidations
    }
}

/// Coordinatore centrale thread-safe del bus di memoria coerente.
pub struct MyMemoryBus {
    inner: Arc<Mutex<BusState>>,
    num_bus_transactions: Arc<AtomicUsize>,
    num_invalidations: Arc<AtomicUsize>,
}

impl MyMemoryBus {
    /// Inizializza un nuovo bus di memoria.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BusState::new())),
            num_bus_transactions: Arc::new(AtomicUsize::new(0)),
            num_invalidations: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Emette una transazione `BusRd` e restituisce `(is_shared, value)`.
    pub fn bus_rd(&self, source_core_id: CoreId, addr: Address) -> (bool, Value) {
        self.num_bus_transactions.fetch_add(1, SeqCst);
        let mut guard_bus = self.inner.lock().unwrap();
        guard_bus.apply_busrd(source_core_id, addr)
    }

    /// Emette una transazione `BusUpgr` per invalidare i core che detengono la linea in `Shared`.
    pub fn bus_upgr(&self, source_core_id: CoreId, addr: Address) {
        self.num_bus_transactions.fetch_add(1, SeqCst);
        let mut guard_bus = self.inner.lock().unwrap();
        let num_inv = guard_bus.apply_busupgr(source_core_id, addr);
        self.num_invalidations.fetch_add(num_inv, SeqCst);
    }

    /// Emette una transazione `BusRdX` per ottenere l'accesso esclusivo e invalidare gli altri core.
    pub fn bus_rdx(&self, source_core_id: CoreId, addr: Address) {
        self.num_bus_transactions.fetch_add(1, SeqCst);
        let mut guard_bus = self.inner.lock().unwrap();
        let num_inv = guard_bus.apply_busrdx(source_core_id, addr);
        self.num_invalidations.fetch_add(num_inv, SeqCst);
    }
}

impl MemoryBus for MyMemoryBus {
    /// Collega un nuovo core al bus, assegnando un CoreId univoco crescente.
    fn attach_core(&self) -> (CoreId, impl CoreCache + 'static) {
        let mut guard = self.inner.lock().unwrap();
        let core_id = guard.next_core_id;
        guard.next_core_id += 1;

        let core_state = Arc::new((Mutex::new(CoreState::new()), Condvar::new()));
        guard.cores.insert(core_id, core_state.clone());

        let new_core = MyCore {
            core_id,
            state: core_state,
            shared_bus: self.clone(),
        };

        (core_id, new_core)
    }

    /// Restituisce il valore attualmente presente nella memoria principale per quell'indirizzo.
    fn read_main_memory(&self, addr: Address) -> Value {
        let guard = self.inner.lock().unwrap();
        guard.main_memory.get(&addr).copied().unwrap_or(0)
    }

    /// Restituisce il conteggio cumulativo di transazioni trasmesse sul bus.
    fn total_bus_transactions(&self) -> usize {
        self.num_bus_transactions.load(SeqCst)
    }

    /// Restituisce il conteggio cumulativo di invalidazioni di linee effettuate per effetto dello snoop.
    fn total_invalidations(&self) -> usize {
        self.num_invalidations.load(SeqCst)
    }

    /// Restituisce il numero di core attualmente connessi e attivi sul bus.
    fn active_core_count(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.cores.len()
    }
}

impl Clone for MyMemoryBus {
    /// Clona il bus condividendo la memoria e i contatori atomici.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            num_bus_transactions: self.num_bus_transactions.clone(),
            num_invalidations: self.num_invalidations.clone(),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza un nuovo bus di memoria coerente con snooping distribuito e protocollo MESI.
pub fn make_memory_bus() -> impl MemoryBus {
    MyMemoryBus::new()
}
