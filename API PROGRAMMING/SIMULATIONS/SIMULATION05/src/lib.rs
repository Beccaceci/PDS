//! # Simulazione 005 — BoundedQueue
//!
//! Nei sistemi produttore/consumatore, un produttore più veloce del consumatore può accumulare dati senza limiti, esaurendo la memoria disponibile. Una coda **limitata** (bounded) risolve il problema imponendo una capacità massima: quando la coda è piena, il produttore stesso viene bloccato finché il consumatore non libera spazio. Questa pressione all'indietro è nota come **backpressure**.
//!
//! Si scriva in Rust una struttura che implementi il tratto generico `BoundedQueue<T: Send>`, un canale produttore/consumatore con capacità massima fissata, in cui sia il produttore che il consumatore possono bloccarsi.
//!
//! ### API richiesta
//!
//! ```rust
//! pub trait BoundedQueue<T: Send>: Clone + Send + Sync {
//!     fn push(&self, value: T) -> bool;
//!     fn pop(&self) -> Option<T>;
//!     fn close(&self);
//! }
//!
//! pub fn make_bounded_queue<T: Send>(capacity: usize) -> impl BoundedQueue<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile tra più produttori e più consumatori (da cui `Clone` sul tratto stesso).
//! - La coda non deve mai contenere più di `capacity` elementi.
//! - Un `push` bloccato per coda piena deve sbloccarsi non appena un `pop` libera spazio; un `pop` bloccato per coda vuota deve sbloccarsi non appena un `push` inserisce un valore.
//! - Dopo `close()`, i `push` successivi falliscono immediatamente (senza bloccare); i `pop` continuano a restituire i valori residui e poi `None`.
//! - Nessuna attesa attiva in nessun punto.

use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}};

/// Trait che rappresenta una coda thread-safe a capacità limitata con supporto al blocco bidirezionale.
pub trait BoundedQueue<T: Send>: Clone + Send + Sync {
    /// Inserisce `value` in coda. Blocca senza attesa attiva se la coda è piena.
    fn push(&self, value: T) -> bool;

    /// Preleva il valore più vecchio in coda (FIFO). Blocca senza attesa attiva se la coda è vuota.
    fn pop(&self) -> Option<T>;

    /// Impedisce ulteriori `push`; i valori già in coda restano disponibili per `pop`.
    fn close(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyQueue<T: Send> {
    state: Arc<(Mutex<(VecDeque<T>, bool)>, Condvar, Condvar)>,
    capacity: usize
}

impl<T: Send> MyQueue<T> {
    pub fn new (_capacity: usize) -> Self {
        Self {
            state: Arc::new((Mutex::new((VecDeque::new(), false)), Condvar::new(), Condvar::new())),
            capacity: _capacity
        }
    }
}

impl<T: Send> Clone for MyQueue<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            capacity: self.capacity
        }
    }
}

impl<T: Send> BoundedQueue<T> for MyQueue<T> {
    fn push(&self, value: T) -> bool {
        let (mutex, cvar_empty, cvar_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard = cvar_full.wait_while(guard, |c| {
            c.0.len() == self.capacity && !c.1
        }).unwrap();

        if guard.1 {
            false
        }
        else {
            guard.0.push_back(value);
            drop(guard);
            cvar_empty.notify_one();
            true
        }
    }

    fn pop(&self) -> Option<T> {
        let (mutex, cvar_empty, cvar_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard = cvar_empty.wait_while(guard, |c| {
            c.0.is_empty() && !c.1
        }).unwrap();

        if let Some(returned_value) = guard.0.pop_front() {
            drop(guard);
            cvar_full.notify_one();
            Some(returned_value)
        }
        else {
            None
        }
    }

    fn close(&self) {
        let (mutex, cvar_empty, cvar_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard.1 = true;
        drop(guard);
        cvar_empty.notify_all();
        cvar_full.notify_all();
    }
}


// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che inizializza una nuova BoundedQueue con la capacità specificata.
pub fn make_bounded_queue<T: Send>(_capacity: usize) -> impl BoundedQueue<T> {
    MyQueue::new(_capacity)
}
