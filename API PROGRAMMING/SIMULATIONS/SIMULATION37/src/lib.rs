//! # Simulazione 037 — PriorityLock (capstone)
//!
//! Se un thread a bassa priorità detiene un lock che un thread a priorità più alta sta aspettando,
//! un sistema che non se ne accorge rischia l'inversione di priorità: il thread a bassa priorità,
//! non essendo esso stesso prioritario, può essere tenuto in attesa da thread a priorità intermedia
//! che non hanno nulla a che fare con quel lock — ritardando indirettamente anche il thread ad alta priorità
//! che aspetta il rilascio. La soluzione classica è elevare temporaneamente la priorità del possessore del lock
//! a quella del richiedente più prioritario in attesa, finché non lo rilascia.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `PriorityGuard<T>` e `PriorityLock<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait PriorityGuard<T: Send> {
//!     fn get(&self) -> &T;
//!     fn get_mut(&mut self) -> &mut T;
//! }
//!
//! pub trait PriorityLock<T: Send>: Clone + Send + Sync {
//!     fn acquire(&self, caller_priority: u32) -> impl PriorityGuard<T>;
//!     fn current_holder_priority(&self) -> Option<u32>;
//! }
//!
//! pub fn make_priority_lock<T: Send + 'static>(value: T) -> impl PriorityLock<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`); al più un `PriorityGuard<T>` attivo alla volta.
//! - `current_holder_priority()` deve riflettere in ogni istante il massimo tra la priorità di acquisizione
//!   del possessore corrente e le priorità di tutti i richiedenti attualmente bloccati in `acquire()` —
//!   aggiornato immediatamente ad ogni nuovo richiedente che inizia ad attendere.
//! - Quando il possessore rilascia il lock (uscita dallo scope del `PriorityGuard<T>`, RAII tramite `Drop`),
//!   se ci sono richiedenti in attesa, il lock deve essere concesso a quello con la priorità di richiesta più alta tra essi.
//! - Dopo il rilascio, `current_holder_priority()` deve riflettere correttamente il nuovo stato.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::BinaryHeap, sync::{Arc, Condvar, Mutex}};

/// Trait che rappresenta il guardiano RAII con accesso esclusivo al dato protetto.
pub trait PriorityGuard<T: Send> {
    /// Restituisce un riferimento immutabile al dato protetto.
    fn get(&self) -> &T;

    /// Restituisce un riferimento mutabile al dato protetto.
    fn get_mut(&mut self) -> &mut T;
}

/// Trait che rappresenta il lock a ereditarietà di priorità (Priority Inheritance Lock).
pub trait PriorityLock<T: Send>: Clone + Send + Sync {
    /// Acquisisce l'accesso esclusivo con priorità `caller_priority`.
    /// Blocca il chiamante se il lock è posseduto, elevando immediatamente la priorità del possessore.
    fn acquire(&self, caller_priority: u32) -> impl PriorityGuard<T>;

    /// Restituisce la priorità effettiva corrente del possessore (massimo tra la sua priorità di acquisizione
    /// e le priorità di tutti i richiedenti in attesa), oppure `None` se il lock è libero.
    fn current_holder_priority(&self) -> Option<u32>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyGuard<T: Send> {
    value: Option<T>,
    shared_locker: MyPriorityLock<T>
}

impl<T: Send> PriorityGuard<T> for MyGuard<T> {
    fn get(&self) -> &T {
        self.value.as_ref().take().unwrap()
    }

    fn get_mut(&mut self) -> &mut T {
        self.value.as_mut().take().unwrap()
    }
}

impl<T: Send> Drop for MyGuard<T> {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_locker.inner;
        let mut guard = mutex.lock().unwrap();
        guard.value = Some(self.value.take().unwrap());
        guard.holder_base_priority = None;
        drop(guard);
        cvar.notify_all();
    }
}

pub struct PriorityLockState<T: Send> {
    value: Option<T>,
    holder_base_priority: Option<u32>,
    waiting_priority_threads: BinaryHeap<u32>
}

impl<T: Send> PriorityLockState<T> {
    pub fn new () -> Self {
        Self {
            value: None,
            holder_base_priority: None,
            waiting_priority_threads: BinaryHeap::new()
        }
    }

    pub fn with_value (_value: T) -> Self {
        Self {
            value: Some(_value),
            holder_base_priority: None,
            waiting_priority_threads: BinaryHeap::new()
        }
    }
}


pub struct MyPriorityLock<T: Send> {
    inner: Arc<(Mutex<PriorityLockState<T>>, Condvar)>
}

impl<T: Send> MyPriorityLock<T> {
    pub fn new (_value: T) -> Self {
        Self {
            inner: Arc::new((Mutex::new(PriorityLockState::with_value(_value)), Condvar::new()))
        }
    }
}

impl<T: Send> PriorityLock<T> for MyPriorityLock<T> {
    fn acquire(&self, caller_priority: u32) -> impl PriorityGuard<T> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        
        guard.waiting_priority_threads.push(caller_priority);

        guard = cvar.wait_while(guard, |c| {
            if c.holder_base_priority.is_none() && c.value.is_some() {
                if let Some(priority) = c.waiting_priority_threads.peek() {
                    if *priority == caller_priority {
                        return false;
                    }
                }
            }
            true
        }).unwrap();

        guard.holder_base_priority = Some(caller_priority);
        guard.waiting_priority_threads.pop();

        MyGuard {
            value: guard.value.take(),
            shared_locker: self.clone()
        }
    }

    fn current_holder_priority(&self) -> Option<u32> {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        let mut max_priority = guard.holder_base_priority;

        if let Some(priority1) = max_priority {   
            if let Some(priority2) = guard.waiting_priority_threads.iter().max().copied() {
                max_priority = if priority1 > priority2 { Some(priority1) } else { Some(priority2) };
            }
            max_priority
        }
        else {
            None
        }
    }
}

impl<T: Send> Clone for MyPriorityLock<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `PriorityLock` che protegge `value`.
pub fn make_priority_lock<T: Send + 'static>(_value: T) -> impl PriorityLock<T> {
    MyPriorityLock::new(_value)
}
