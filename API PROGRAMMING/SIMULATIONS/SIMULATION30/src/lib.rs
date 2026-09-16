//! # Simulazione 030 — LoadBalancer
//!
//! Un bilanciatore che instrada richieste verso più backend deve tenere traccia della salute di ciascuno
//! indipendentemente dagli altri — un backend che fallisce ripetutamente va escluso temporaneamente, con una singola
//! richiesta di prova concessa dopo un periodo di raffreddamento per verificarne il recupero — e, tra i backend
//! attualmente eleggibili, preferire sempre quello con il carico più basso.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `RequestHandle` e `LoadBalancer` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! pub trait RequestHandle {
//!     fn report(self, success: bool);
//! }
//!
//! pub trait LoadBalancer: Clone + Send + Sync {
//!     fn register(&self, id: &str);
//!     fn route(&self) -> impl RequestHandle;
//! }
//!
//! pub fn make_load_balancer(failure_threshold: u32, recovery_after: Duration) -> impl LoadBalancer {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`); lo stato di ciascun backend è indipendente da quello degli altri.
//! - `report(false)` su un backend sano ne incrementa i fallimenti consecutivi; al raggiungimento di `failure_threshold`, il backend passa a non sano, registrando l'istante della transizione. `report(true)` su un backend sano azzera i suoi fallimenti consecutivi.
//! - Un backend non sano diventa eleggibile come "in prova" non appena trascorre `recovery_after` dal momento in cui è diventato tale.
//! - `route()` non deve mai assegnare più di una richiesta di prova alla volta allo stesso backend in prova.
//! - `report(true)` su un backend in prova lo riporta a sano, fallimenti azzerati; `report(false)`, o l'assenza di `report()`, lo riporta a non sano, con il raffreddamento che riparte da questo momento.
//! - Il carico di un backend va incrementato da `route()` e decrementato esattamente una volta quando il `RequestHandle` corrispondente esce dallo scope, indipendentemente dal fatto che `report()` sia stato chiamato.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, Ordering::SeqCst}}, time::{Duration, Instant}};

use crate::BackendState::{Closed, Open, Trial};

/// Trait che rappresenta l'handle per segnalare l'esito di una richiesta instradata.
/// Il carico sul backend viene decrementato automaticamente all'uscita dallo scope ([`Drop`]).
pub trait RequestHandle {
    /// Segnala l'esito della richiesta completata su questo backend.
    /// Invocato al più una volta. Se esce dallo scope senza chiamata esplicita a `report()`,
    /// l'esito è considerato un fallimento (`success = false`).
    fn report(self, success: bool);
}

/// Trait che rappresenta il bilanciatore di carico con health check per-backend e least-loaded routing.
pub trait LoadBalancer: Clone + Send + Sync {
    /// Registra un nuovo backend identificato da `id`, inizialmente sano con carico 0.
    /// Chiamate ripetute con lo stesso `id` sono idempotenti.
    fn register(&self, id: &str);

    /// Seleziona il miglior backend eleggibile (carico minimo tra i sani, oppure backend in prova),
    /// incrementandone il carico e restituendo l'handle per segnalarne l'esito.
    /// Blocca il chiamante senza attesa attiva se nessun backend è attualmente eleggibile.
    fn route(&self) -> impl RequestHandle;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyHandle {
    shared_balancer: Arc<(Mutex<HashMap<String, (BackendState, usize)>>, Condvar)>,
    backend_id: String,
    reported: AtomicBool,
    failure_threshold: u32,
    recovery_after: Duration
}

impl RequestHandle for MyHandle {
    fn report(self, success: bool) {
        if !self.reported.swap(true, SeqCst) {
            let (mutex, cvar) = &*self.shared_balancer;
            let mut guard = mutex.lock().unwrap();

            if let Some((state, _)) = guard.get_mut(&self.backend_id) {
                state.update_state(success, self.failure_threshold, self.recovery_after);
                drop(guard);
                cvar.notify_all();
            }
        }
    }
}

impl Drop for MyHandle {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_balancer;
        let mut guard = mutex.lock().unwrap();

        if let Some((state, active_handles)) = guard.get_mut(&self.backend_id) {
            *active_handles -= 1;

            if !self.reported.load(SeqCst) {
                state.update_state(false, self.failure_threshold, self.recovery_after);
                drop(guard);
                cvar.notify_all();
            }
        }
    }
}

pub enum BackendState {
    Closed(Instant),
    Trial,
    Open(u32) // ( num_failures)
}

impl BackendState {
    pub fn update_state(
        &mut self,
        success: bool,
        failure_threshold: u32,
        recovery_after: Duration,
    ) {
        match *self {
            Open(failures) => {
                if success {
                    // Successo su backend sano: azzera i fallimenti
                    *self = Open(0);
                } else {
                    let new_failures = failures + 1;
                    if new_failures >= failure_threshold {
                        // Soglia superata: transita a non sano con cooldown
                        *self = Closed(Instant::now() + recovery_after);
                    } else {
                        // Incrementa i fallimenti consecutivi
                        *self = Open(new_failures);
                    }
                }
            }
            Trial => {
                if success {
                    // Prova riuscita: backend risanato
                    *self = Open(0);
                } else {
                    // Prova fallita: torna non sano, cooldown riparte da adesso
                    *self = Closed(Instant::now() + recovery_after);
                }
            }
            Closed(_) => {
                // Nessuna azione: le richieste possono essere state instradate solo su Open o Trial
            }
        }
    }
}


pub struct MyBalancer {
    inner: Arc<(Mutex<HashMap<String, (BackendState, usize)>>, Condvar)>, // (state, active_handles)
    failure_threshold: u32,
    recovery_after: Duration
}

impl MyBalancer {
    pub fn new (_failure_threshold: u32, _recovery_after: Duration) -> Self {
        Self {
            inner: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
            failure_threshold: _failure_threshold,
            recovery_after: _recovery_after
        }
    }
}


pub fn find_best_open_backend(map: &HashMap<String, (BackendState, usize)>) -> Option<String> {
    map.iter()
        .filter(|(_, (state, _))| matches!(state, BackendState::Open(_)))
        .min_by_key(|(_, (_, active_handles))| *active_handles)
        .map(|(id, _)| id.clone())
}

pub fn find_expired_closed_backend(map: &HashMap<String, (BackendState, usize)>) -> Option<String> {
    let now = Instant::now();
    map.iter()
        .find(|(_, (state, _))| match state {
            BackendState::Closed(until) => *until <= now,
            _ => false,
        })
        .map(|(id, _)| id.clone())
}

impl LoadBalancer for MyBalancer {
    fn register(&self, id: &str) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.get(&id.to_string()).is_none() {
            guard.insert(id.to_string(), (Open(0), 0));
            drop(guard);
            cvar.notify_all();
        } 
    }

    fn route(&self) -> impl RequestHandle {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            // 1. Cerca il miglior sano, altrimenti cerca il primo non sano con cooldown scaduto
            let candidate = find_best_open_backend(&guard)
                .map(|id| (id, false)) // Backend sano
                .or_else(|| find_expired_closed_backend(&guard).map(|id| (id, true))); // Backend di prova
            
            // 2. Se abbiamo trovato un candidato eleggibile:
            if let Some((backend_id, is_probe)) = candidate {
                let (state, active_handles) = guard.get_mut(&backend_id).unwrap();
                
                if is_probe {
                    *state = BackendState::Trial; // 👈 Promozione atomica a InProva
                }
                *active_handles += 1;
                
                return MyHandle {
                    shared_balancer: self.inner.clone(),
                    backend_id,
                    reported: AtomicBool::new(false),
                    failure_threshold: self.failure_threshold,
                    recovery_after: self.recovery_after,
                };
            }

            // 3. Nessun backend eleggibile al momento: calcola il cooldown più vicino e attende
            let now = Instant::now();
            let earliest_cooldown = guard.values().filter_map(|(state, _)| match state {
                BackendState::Closed(until) if *until > now => Some(*until),
                _ => None,
            }).min();

            if let Some(until) = earliest_cooldown {
                let dur = until.saturating_duration_since(now);
                (guard, _) = cvar.wait_timeout(guard, dur).unwrap();
            }
            else {
                guard = cvar.wait(guard).unwrap();
            }
        }
    }
}

impl Clone for MyBalancer {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            failure_threshold: self.failure_threshold,
            recovery_after: self.recovery_after
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `LoadBalancer`.
pub fn make_load_balancer(
    _failure_threshold: u32,
    _recovery_after: Duration,
) -> impl LoadBalancer {
    MyBalancer::new(_failure_threshold, _recovery_after)
}
