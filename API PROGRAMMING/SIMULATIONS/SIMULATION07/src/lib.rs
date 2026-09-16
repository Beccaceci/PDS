//! # Simulazione 007 — RwCell
//!
//! Molte strutture dati condivise vengono lette molto più spesso di quanto vengano modificate: permettere a più lettori di accedere contemporaneamente, riservando l'accesso esclusivo solo a chi scrive, migliora sensibilmente il throughput rispetto a un semplice mutex. È il classico problema **lettori/scrittori**: più lettori possono procedere insieme, ma uno scrittore richiede accesso esclusivo, e un flusso continuo di lettori non deve poter far attendere indefinitamente uno scrittore in coda (starvation).
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `ReadGuard<T>`, `WriteGuard<T>` e `RwCell<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait ReadGuard<T: Send + Sync> {
//!     fn get(&self) -> &T;
//! }
//!
//! pub trait WriteGuard<T: Send + Sync> {
//!     fn get(&self) -> &T;
//!     fn get_mut(&mut self) -> &mut T;
//! }
//!
//! pub trait RwCell<T: Send + Sync>: Send + Sync {
//!     fn read(&self) -> impl ReadGuard<T> + 'static;
//!     fn write(&self) -> impl WriteGuard<T> + 'static;
//! }
//!
//! pub fn make_rw_cell<T: Send + Sync + 'static>(value: T) -> impl RwCell<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Più `ReadGuard<T>` possono coesistere ed essere attivi contemporaneamente, anche su thread diversi.
//! - Al più un `WriteGuard<T>` può essere attivo alla volta, e la sua presenza esclude qualunque `ReadGuard<T>` o altro `WriteGuard<T>` attivo.
//! - **Preferenza dello scrittore**: se uno scrittore è in attesa, nessun nuovo lettore che arrivi successivamente deve poter ottenere accesso prima di lui, anche se l'accesso in lettura sarebbe altrimenti concedibile — i lettori già attivi al momento dell'arrivo dello scrittore possono invece completare normalmente.
//! - Quando un `ReadGuard<T>` o un `WriteGuard<T>` esce dallo scope, l'accesso corrispondente deve essere rilasciato automaticamente (RAII, tramite il tratto `Drop`), risvegliando eventuali richieste in attesa compatibili con il nuovo stato.
//! - Thread-safe, condivisibile tra più thread contemporaneamente.
//! - Nessuna attesa attiva in nessun punto del sistema.

use std::{cell::UnsafeCell, sync::{Arc, Condvar, Mutex}};

/// Trait per un guard che consente l'accesso condiviso in sola lettura al dato protetto.
pub trait ReadGuard<T: Send + Sync> {
    /// Restituisce un riferimento immutabile al valore protetto.
    fn get(&self) -> &T;
}

/// Trait per un guard che consente l'accesso esclusivo e mutabile al dato protetto.
pub trait WriteGuard<T: Send + Sync> {
    /// Restituisce un riferimento immutabile al valore protetto.
    fn get(&self) -> &T;

    /// Restituisce un riferimento mutabile al valore protetto.
    fn get_mut(&mut self) -> &mut T;
}

/// Trait che rappresenta una cella sincronizzata lettori/scrittori con priorità agli scrittori (writer-preference).
pub trait RwCell<T: Send + Sync>: Send + Sync {
    /// Blocca il chiamante finché non è possibile concedere accesso in lettura:
    /// cioè finché nessuno scrittore ha accesso attivo e nessuno scrittore è in attesa da prima di questa chiamata.
    fn read(&self) -> impl ReadGuard<T> + 'static;

    /// Blocca il chiamante finché non è possibile concedere accesso esclusivo:
    /// cioè finché non ci sono né lettori né scrittori attivi.
    fn write(&self) -> impl WriteGuard<T> + 'static;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

unsafe impl<T: Send + Sync> Sync for SharedCell<T> {}
unsafe impl<T: Send + Sync> Send for SharedCell<T> {}

pub struct MyReadGuard<T: Send + Sync> {
    inner: Arc<SharedCell<T>>
}

impl<T: Send + Sync> ReadGuard<T> for MyReadGuard<T> {
    fn get(&self) -> &T {
        unsafe {
            &*self.inner.value.get()
        }
    }
}

impl<T: Send + Sync> Drop for MyReadGuard<T> {
    fn drop(&mut self) {
        let SharedCell { state, cvar_readers, cvar_writers, value } = &*self.inner;
        let mut guard = state.lock().unwrap();
        guard.active_readers -= 1;
        
        if guard.active_readers == 0 {
            drop(guard);
            cvar_writers.notify_one();
        }
    }
}


pub struct MyWriteGuard<T: Send + Sync> {
    inner: Arc<SharedCell<T>>
}

impl<T: Send + Sync> WriteGuard<T> for MyWriteGuard<T> {
    fn get(&self) -> &T {
        unsafe {
            &*self.inner.value.get()
        }
    }

    fn get_mut(&mut self) -> &mut T {
        unsafe {
            &mut *self.inner.value.get()
        }
    }
}

impl<T: Send + Sync> Drop for MyWriteGuard<T> {
    fn drop(&mut self) {
        let SharedCell { state, cvar_readers, cvar_writers, value } = &*self.inner;
        let mut guard = state.lock().unwrap();
        guard.writer_active = false;

        if guard.waiting_writers > 0 {
            drop(guard);
            cvar_writers.notify_one();
        }
        else {
            drop(guard);
            cvar_readers.notify_all();
        }
    }
}

pub struct State {
    active_readers: usize,
    waiting_writers: usize,
    writer_active: bool
}

impl State {
    pub fn new () -> Self {
        Self {
            active_readers: 0,
            waiting_writers: 0,
            writer_active: false
        }
    }
}

pub struct SharedCell<T: Send + Sync> {
    state: Mutex<State>,
    cvar_readers: Condvar,
    cvar_writers: Condvar,
    value: UnsafeCell<T>
}

impl<T: Send + Sync> SharedCell<T> {
    pub fn new (_value: T) -> Self {
        Self {
            state: Mutex::new(State::new()),
            cvar_readers: Condvar::new(),
            cvar_writers: Condvar::new(),
            value: UnsafeCell::new(_value)
        }
    } 
}

pub struct MyRwCell<T: Send + Sync> {
    inner: Arc<SharedCell<T>>
}

impl<T: Send + Sync> MyRwCell<T> {
    pub fn new (_value: T) -> Self {
        Self {
            inner: Arc::new(SharedCell::new(_value))
        }
    }
}

impl<T: Send + Sync> Clone for MyRwCell<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner)
        }
    }
}

impl<T: Send + Sync + 'static> RwCell<T> for MyRwCell<T> {
    fn read(&self) -> impl ReadGuard<T> + 'static {
        let SharedCell { state, cvar_readers, cvar_writers, value } = &*self.inner;
        let mut guard = state.lock().unwrap();

        guard = cvar_readers.wait_while(guard, |c| {
            c.waiting_writers > 0 || c.writer_active
        }).unwrap();

        guard.active_readers += 1;
        MyReadGuard {
            inner: self.inner.clone()
        }
    }

    fn write(&self) -> impl WriteGuard<T> + 'static {
        let SharedCell { state, cvar_readers, cvar_writers, value } = &*self.inner;
        let mut guard = state.lock().unwrap();

        guard.waiting_writers += 1;
        guard = cvar_writers.wait_while(guard, |c| {
            c.active_readers > 0 || c.writer_active
        }).unwrap();

        guard.waiting_writers -= 1;
        guard.writer_active = true;
        MyWriteGuard {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che inizializza una nuova `RwCell` contenente il valore iniziale specificato.
pub fn make_rw_cell<T: Send + Sync + 'static>(_value: T) -> impl RwCell<T> {
    MyRwCell::new(_value)
}