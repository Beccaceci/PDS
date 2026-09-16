//! # Simulazione 036 — Reactor (capstone)
//!
//! Un ciclo di eventi che deve sorvegliare più sorgenti di I/O contemporaneamente (descrittori di file, socket)
//! non può bloccarsi su ciascuna singolarmente: deve invece attendere che *almeno una* tra tutte quelle registrate
//! diventi pronta, restituendo l'insieme di quelle pronte in quel momento — dando priorità, quando presenti,
//! alle sorgenti marcate come urgenti rispetto alle altre.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Registration` e `Reactor` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! pub trait Registration {
//!     fn deregister(self);
//! }
//!
//! pub trait Reactor: Clone + Send + Sync {
//!     fn register(&self, id: u64, urgent: bool) -> impl Registration;
//!     fn mark_ready(&self, id: u64);
//!     fn poll(&self) -> Vec<u64>;
//!     fn poll_timeout(&self, timeout: Duration) -> Vec<u64>;
//! }
//!
//! pub fn make_reactor() -> impl Reactor {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - `register()`/`deregister()` possono avvenire in qualunque momento, anche mentre una o più chiamate a `poll()`/`poll_timeout()` sono già bloccate in attesa.
//! - Un id deregistrato non deve mai comparire in un risultato di `poll()` successivo, nemmeno se era già segnalato pronto al momento della deregistrazione.
//! - `poll()` consuma esattamente e soltanto gli id che restituisce: se restituisce solo gli urgenti (perché ve n'erano di pronti), quelli non urgenti eventualmente pronti restano tali per una chiamata successiva.
//! - Se più chiamate a `poll()`/`poll_timeout()` sono bloccate concorrentemente su thread diversi, ciascun id pronto deve essere consegnato a una sola di esse.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, Ordering::SeqCst}}, time::Duration};

/// Trait che rappresenta la registrazione attiva di un interesse presso il `Reactor`.
pub trait Registration {
    /// Annulla questa registrazione. Se l'interesse corrispondente era già
    /// segnalato come pronto, quella segnalazione viene scartata: non
    /// comparirà in nessuna chiamata a `poll()` successiva.
    fn deregister(self);
}

/// Trait che rappresenta il reattore multiplexer di eventi di I/O con priorità urgente.
pub trait Reactor: Clone + Send + Sync {
    /// Registra un nuovo interesse, identificato da `id` (univoco tra le registrazioni attive).
    /// Se `urgent` è true, questo interesse ha precedenza sugli altri in `poll()`. Inizialmente non pronto.
    fn register(&self, id: u64, urgent: bool) -> impl Registration;

    /// Segnala che l'interesse `id` è pronto. Se `id` non è attualmente registrato, non ha alcun effetto.
    /// Non blocca mai il chiamante. Resta pronto finché non viene consumato da `poll()` o annullato da `deregister()`.
    fn mark_ready(&self, id: u64);

    /// Blocca il chiamante finché almeno un interesse registrato non è pronto.
    /// Se almeno un interesse pronto è urgente, restituisce e consuma SOLO gli id urgenti pronti in quel momento.
    /// Altrimenti restituisce e consuma tutti gli id non urgenti pronti.
    fn poll(&self) -> Vec<u64>;

    /// Variante con attesa limitata nel tempo: come `poll()`, ma se nessun interesse diventa pronto entro `timeout`
    /// rinuncia e restituisce un vettore vuoto senza consumare cicli CPU.
    fn poll_timeout(&self, timeout: Duration) -> Vec<u64>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyRegistration {
    id: u64,
    shared_manager: MyManager
}

impl Registration for MyRegistration {
    fn deregister(self) {
        let (mutex, _) = &*self.shared_manager.inner;
        let mut guard = mutex.lock().unwrap();
        guard.registrations.retain(|reg| reg.id != self.id);
    }
}

pub struct RegistrationState {
    id: u64,
    urgent: bool,
    ready: AtomicBool
}

pub struct ManagerState {
    registrations: Vec<RegistrationState>
}

impl ManagerState {
    pub fn new () -> Self {
        Self {
            registrations: Vec::new()
        }
    }

    pub fn someone_ready (&self) -> bool {
        if let Some(_) = self.registrations.iter().find(|reg| {
            reg.ready.load(SeqCst)
        }) {
            return true;
        }
        false
    }

    pub fn someone_urgent_ready (&self) -> bool {
        if let Some(_) = self.registrations.iter().find(|reg| {
            reg.urgent && reg.ready.load(SeqCst)
        }) {
            return true;
        }
        false
    }
}

pub struct MyManager {
    inner: Arc<(Mutex<ManagerState>, Condvar)>
}

impl MyManager {
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(ManagerState::new()), Condvar::new()))
        }
    }
}

impl Reactor for MyManager {
    fn register(&self, id: u64, urgent: bool) -> impl Registration {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        guard.registrations.push(RegistrationState {
            id,
            urgent,
            ready: AtomicBool::new(false)
        });

        MyRegistration {
            id,
            shared_manager: self.clone()
        }
    }

    fn mark_ready(&self, id: u64) {
        let (mutex, cvar) = &*self.inner;
        let guard = mutex.lock().unwrap();

        if let Some(registration) = guard.registrations.iter().find(|reg| {
            reg.id == id
        }) {
            registration.ready.store(true, SeqCst);
            drop(guard);
            cvar.notify_all();
        }
    }

    fn poll(&self) -> Vec<u64> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard = cvar.wait_while(guard, |c| {
            !c.someone_ready()
        }).unwrap();

        let someone_urgent = guard.someone_urgent_ready();

        let mut index_set = Vec::new();
        for reg in guard.registrations.iter() {
            if (someone_urgent && reg.urgent) || (!someone_urgent && !reg.urgent) {
                if reg.ready.swap(false, SeqCst) {
                    index_set.push(reg.id);
                } 
            }
        }
        index_set
    }

    fn poll_timeout(&self, timeout: Duration) -> Vec<u64> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        (guard, _) = cvar.wait_timeout_while(guard, timeout,|c| {
            !c.someone_ready()
        }).unwrap();

        let mut index_set = Vec::new();

        if guard.someone_ready() {
            let someone_urgent = guard.someone_urgent_ready();
            for reg in guard.registrations.iter() {
                if (someone_urgent && reg.urgent) || (!someone_urgent && !reg.urgent) {
                    if reg.ready.swap(false, SeqCst) {
                        index_set.push(reg.id);
                    } 
                }
            }
        }
        index_set
    }
}

impl Clone for MyManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `Reactor`.
pub fn make_reactor() -> impl Reactor {
    MyManager::new()
}
