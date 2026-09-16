//! # Simulazione 024 — LeaseLockManager (Capstone)
//!
//! Un servizio di lock distribuito non può permettere che un lock resti bloccato per sempre
//! se chi lo detiene si blocca o dimentica di rilasciarlo: ogni acquisizione ha una scadenza (*lease*),
//! che il possessore deve rinnovare esplicitamente per continuare a detenere il lock;
//! se non lo fa in tempo, il lock si libera automaticamente, anche senza alcuna azione da parte sua.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `Lease` e `LeaseLockManager` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust
//! use std::time::Duration;
//!
//! pub trait Lease {
//!     fn renew(&self, duration: Duration) -> bool;
//! }
//!
//! pub trait LeaseLockManager: Clone {
//!     fn acquire(&self, name: &str, lease_duration: Duration) -> impl Lease;
//! }
//!
//! pub fn make_lease_lock_manager() -> impl LeaseLockManager {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone`); ogni nome di lock è indipendente dagli altri.
//! - Se l'oggetto `Lease` esce dallo scope prima della propria scadenza, il lock corrispondente si libera immediatamente (RAII, tramite `Drop`), risvegliando eventuali thread in attesa di acquisire lo stesso nome.
//! - Se nessuno rinnova né lascia uscire dallo scope l'oggetto `Lease` in tempo, il lock deve liberarsi comunque, entro un tempo ragionevole dopo la scadenza, senza che nessuno lo richieda esplicitamente.
//! - `renew()` chiamato dopo che il lock è già stato liberato (per scadenza) non deve avere alcun effetto, e deve restituire `false` — in particolare, non deve mai rinnovare per errore una lease che nel frattempo è stata riassegnata a un altro chiamante che ha acquisito lo stesso nome.
//! - `Drop` di un `Lease` la cui lease era già scaduta al momento della distruzione (e quindi il lock già riassegnato ad altri) non deve liberare il lock del *nuovo* possessore.
//! - Nessuna attesa attiva in nessun punto, incluso il meccanismo che rileva le scadenze.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}, thread::{self, JoinHandle}, time::{Duration, Instant}};

/// Trait che rappresenta un lease attivo su una risorsa nominata.
pub trait Lease {
    /// Rinnova la lease corrente, estendendola di `duration` a partire da questo momento.
    /// Restituisce `false` se la lease era già scaduta (e quindi il lock già rilasciato automaticamente)
    /// prima di questa chiamata — in tal caso il rinnovo non ha alcun effetto.
    fn renew(&self, duration: Duration) -> bool;
}

/// Trait che rappresenta il gestore concorrente di lock con lease e scadenza temporale automatica.
pub trait LeaseLockManager: Clone + Send + Sync {
    /// Acquisisce il lock identificato da `name`, con una lease iniziale della durata data.
    /// Blocca il chiamante, senza consumare cicli di CPU, finché il lock non è libero.
    fn acquire(&self, name: &str, lease_duration: Duration) -> impl Lease;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyLease {
    manager: Arc<(Mutex<ManagerState>, Condvar, Condvar)>,
    generation: u64,
    name: String
}

impl Lease for MyLease {
    fn renew(&self, duration: Duration) -> bool {
        let (mutex, _, cvar_thread) = &*self.manager;
        let mut guard = mutex.lock().unwrap();

        if let Some((timeout, generation)) = guard.map.get_mut(&self.name) {
            if *generation == self.generation {
                *timeout = Instant::now() + duration;
                cvar_thread.notify_one();
                return true;
            }
        }
        
        false
    }
}

impl Drop for MyLease {
    fn drop(&mut self) {
        let (mutex, cvar_lease, cvar_thread) = &*self.manager;
        let mut guard = mutex.lock().unwrap();
        if let Some((_, generation)) = guard.map.get(&self.name) {
            if *generation == self.generation {
                guard.map.remove(&self.name);
                drop(guard);
                cvar_lease.notify_all(); // Sblocca subito chi aspetta acquire()
                cvar_thread.notify_one(); // Sveglia il reaper se deve aggiornare il timeout minimo
            }
        }
    }
}

pub struct ManagerState {
    map: HashMap<String, (Instant, u64)>,
    closed: bool,
    next_generation: u64
}

impl ManagerState {
    pub fn new () -> Self {
        Self {
            map: HashMap::new(),
            next_generation: 0 as u64,
            closed: false
        }
    }
}

pub struct MyLockManager {
    inner: Arc<(Mutex<ManagerState>, Condvar, Condvar)>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>
}

impl MyLockManager {
    pub fn new () -> Self {
        let manager = Arc::new((Mutex::new(ManagerState::new()), Condvar::new(), Condvar::new()));
        let cloned_manager = manager.clone();

        let handle = thread::spawn(move || {
            let (mutex, cvar_lease, cvar_thread) = &*cloned_manager;

            loop {
                let mut guard = mutex.lock().unwrap();

                guard.map.retain(|_, (timeout, _)| {
                    *timeout >= Instant::now() 
                });

                let option_earliest_timeout = guard.map.values().min().cloned();
                cvar_lease.notify_all();

                if let Some((earliest_timeout, _)) = option_earliest_timeout {
                    let dur = earliest_timeout.saturating_duration_since(Instant::now());
                    (guard, _) = cvar_thread.wait_timeout(guard, dur).unwrap();
                }
                else {
                    guard = cvar_thread.wait(guard).unwrap();
                }

                if guard.closed {
                    guard.map.clear();
                    break;
                }
            }
        });

        Self {
            inner: manager,
            handle: Arc::new(Mutex::new(Some(handle)))
        }
    }
}

impl LeaseLockManager for MyLockManager {
    fn acquire(&self, name: &str, lease_duration: Duration) -> impl Lease {
        let (mutex, cvar_lease, cvar_thread) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        let name = name.to_string();
        guard = cvar_lease.wait_while(guard, |c| {
            c.map.contains_key(&name)
        }).unwrap();

        let generation = guard.next_generation;
        guard.next_generation += 1;
        guard.map.insert(name.clone(), (Instant::now() + lease_duration, generation));
        drop(guard);

        cvar_thread.notify_one();
        MyLease {
            manager: self.inner.clone(),
            generation,
            name
        }
    }
}

impl Clone for MyLockManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            handle: self.handle.clone()
        }
    }
}

impl Drop for MyLockManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let (mutex, _, cvar_thread) = &*self.inner;
            let mut guard = mutex.lock().unwrap();
            guard.closed = true;
            drop(guard);
            cvar_thread.notify_one();

            if let Some(handle) = self.handle.lock().unwrap().take() {
                let _ = handle.join();
            }
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `LeaseLockManager`.
pub fn make_lease_lock_manager() -> impl LeaseLockManager {
    MyLockManager::new()
}
