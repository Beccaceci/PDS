//! # Simulazione 017 — TurnManager
//!
//! In una simulazione multi-agente, o in un gioco a turni, un insieme fisso di partecipanti
//! identificati da un indice deve agire uno alla volta, rispettando rigidamente un ordine di rotazione
//! prestabilito: dopo il partecipante 0 tocca sempre al partecipante 1, poi al 2, e così via ciclicamente,
//! indipendentemente da quale partecipante arrivi per primo a richiedere il proprio turno — è lo stesso
//! principio di un token che circola in una rete ad anello, dove solo chi possiede il token può agire.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait Turn: Send {
//!     // Indice del partecipante a cui è stato concesso questo turno.
//!     fn participant(&self) -> usize;
//! }
//!
//! pub trait TurnManager: Send + Sync {
//!     // Numero di partecipanti nella rotazione, identificati dagli indici
//!     // 0..participants().
//!     fn participants(&self) -> usize;
//!
//!     // Blocca il chiamante finché non è esattamente il turno del
//!     // partecipante `id`, quindi restituisce un oggetto che rappresenta il
//!     // turno ottenuto.
//!     fn wait_for_turn(&self, id: usize) -> impl Turn + 'static;
//! }
//!
//! pub fn make_turn_manager(participants: usize) -> impl TurnManager {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread (`Send + Sync`).
//! - L'avanzamento dei turni segue rigorosamente l'ordine della rotazione ciclica (`0 -> 1 -> 2 -> ... -> N-1 -> 0`), mai l'ordine temporale di arrivo delle chiamate a `wait_for_turn`.
//! - Al più un partecipante alla volta può essere in possesso del turno.
//! - Il rilascio del turno avviene tramite il distruttore RAII (`Drop`) del tratto `Turn`.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::sync::{Arc, Condvar, Mutex};

/// Trait che rappresenta il turno esclusivo concesso a uno specifico partecipante.
pub trait Turn: Send {
    /// Restituisce l'indice identificativo del partecipante a cui è associato questo turno.
    fn participant(&self) -> usize;
}

/// Trait che gestisce la sincronizzazione a turni ciclici rigidi tra partecipanti.
pub trait TurnManager: Send + Sync {
    /// Restituisce il numero totale di partecipanti nella rotazione (indici `0..participants()`).
    fn participants(&self) -> usize;

    /// Blocca il chiamante finché non è esattamente il turno del partecipante `id`,
    /// restituendo una guardia RAII `Turn`.
    fn wait_for_turn(&self, id: usize) -> impl Turn + 'static;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyTurn {
    shared_state: Arc<(Mutex<usize>, Condvar)>,
    num_partecipants: usize
}

impl Turn for MyTurn {
    fn participant(&self) -> usize {
        let (mutex, _) = &*self.shared_state;
        let guard = mutex.lock().unwrap();
        *guard
    }
}

impl Drop for MyTurn {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_state;
        let mut guard = mutex.lock().unwrap();
        *guard = if *guard == self.num_partecipants - 1 { 0 } else { *guard + 1 };
        drop(guard);
        cvar.notify_all();
    }
}

pub struct MyTurnManager {
    inner: Arc<(Mutex<usize>, Condvar)>,
    num_partecipants: usize
}

impl MyTurnManager {
    pub fn new (_participants: usize) -> Self {
        Self {
            inner: Arc::new((Mutex::new(0), Condvar::new())),
            num_partecipants: _participants
        }
    }
}

impl TurnManager for MyTurnManager {
    fn participants(&self) -> usize {
        self.num_partecipants
    }

    fn wait_for_turn(&self, id: usize) -> impl Turn + 'static {
        if id >= self.num_partecipants {
            panic!()
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        guard = cvar.wait_while(guard, |c| {
            *c != id
        }).unwrap();

        MyTurn {
            shared_state: self.inner.clone(),
            num_partecipants: self.num_partecipants
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `TurnManager` per il numero specificato di partecipanti.
pub fn make_turn_manager(_participants: usize) -> impl TurnManager {
    MyTurnManager::new(_participants)
}
