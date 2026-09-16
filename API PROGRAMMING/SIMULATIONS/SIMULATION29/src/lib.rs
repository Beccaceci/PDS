//! # Simulazione 029 — AppendLog
//!
//! Un log a cui più thread accodano scritture in continuazione deve poter essere periodicamente "chiuso a chiave"
//! (checkpoint) — per essere scritto su disco, ad esempio — in un momento preciso: le scritture già in corso in quel momento
//! vanno considerate parte del checkpoint e attese prima di procedere, ma le scritture che iniziano *dopo* l'avvio del checkpoint
//! devono poter procedere immediatamente, senza attendere che il checkpoint finisca, e senza essere conteggiate in esso.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `AppendGuard` e `AppendLog` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait AppendGuard {}
//!
//! pub trait AppendLog: Clone + Send + Sync {
//!     fn begin_append(&self) -> impl AppendGuard;
//!     fn checkpoint(&self) -> usize;
//! }
//!
//! pub fn make_append_log() -> impl AppendLog {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - `begin_append()` non deve mai bloccare, in nessuna circostanza — nemmeno mentre un `checkpoint()` è in corso.
//! - Un `checkpoint()` deve attendere esclusivamente le scritture iniziate *prima* della propria chiamata; scritture iniziate dopo (anche durante la sua attesa) appartengono alla nuova epoca e non influenzano né sono influenzate da quel `checkpoint()`.
//! - Checkpoint successivi sono indipendenti: ciascuno attende solo le scritture della propria epoca, mai quelle di un'epoca già chiusa da un checkpoint precedente.
//! - Il valore restituito da `checkpoint()` è il numero totale di scritture che sono appartenute a quell'epoca, non il numero di quelle ancora attive al momento della chiamata (che potrebbe essere già inferiore, se alcune erano già terminate).
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}};

/// Trait marcatore che rappresenta una scrittura in corso nel log.
/// Quando esce dallo scope ([`Drop`]), la scrittura è considerata completata per l'epoca di appartenenza.
pub trait AppendGuard: Send {}

/// Trait che rappresenta il log concorrente basato su epoche con checkpoint non bloccanti per gli scrittori.
pub trait AppendLog: Clone + Send + Sync {
    /// Inizia una nuova scrittura registrandola nell'epoca corrente. Non blocca mai il chiamante.
    fn begin_append(&self) -> impl AppendGuard;

    /// Chiude l'epoca corrente e ne apre una nuova.
    /// Blocca il chiamante finché tutte le scritture dell'epoca chiusa non sono terminate,
    /// quindi restituisce il numero totale di scritture che vi sono appartenute.
    fn checkpoint(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyGuard {
    shared_log: Arc<(Mutex<LogState>, Condvar)>,
    generation_id: usize
}

impl AppendGuard for MyGuard { }

impl Drop for MyGuard {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_log;
        let mut guard = mutex.lock().unwrap();
        let (_, active_writings) = guard.map.get_mut(&self.generation_id).unwrap();
        *active_writings -= 1;

        if *active_writings == 0 {
            drop(guard);
            cvar.notify_all();
        }
    }
}

pub struct LogState {
    map: HashMap<usize, (usize, usize)>,
    next_generation_id: usize
}

pub struct MyLog {
    inner: Arc<(Mutex<LogState>, Condvar)>
}

impl MyLog {
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(LogState {
                map: HashMap::new(),
                next_generation_id: 0
            }), Condvar::new()))
        }
    }
}

impl AppendLog for MyLog {
    fn begin_append(&self) -> impl AppendGuard {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        let generation_id = guard.next_generation_id;

        if let Some((num_writings, active_writings)) = guard.map.get_mut(&generation_id) {
            *num_writings += 1;
            *active_writings += 1;
        }
        else {
            guard.map.insert(generation_id, (1, 1));
        }

        MyGuard {
            shared_log: self.inner.clone(),
            generation_id
        }
    }

    fn checkpoint(&self) -> usize {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        let generation_id = guard.next_generation_id;
        guard.next_generation_id += 1;

        guard = cvar.wait_while(guard, |c| {
            if let Some((_, active_writings)) = c.map.get(&generation_id) {
                if *active_writings > 0 {
                    return true;
                }
            }
            false
        }).unwrap();

        if let Some((num_writings, _)) = guard.map.remove(&generation_id) {
            num_writings
        }
        else {
            0
        }
    }
}

impl Clone for MyLog {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `AppendLog`.
pub fn make_append_log() -> impl AppendLog {
    MyLog::new()
}
