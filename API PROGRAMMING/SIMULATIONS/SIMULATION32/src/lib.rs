//! # Simulazione 032 — Phaser
//!
//! Un'elaborazione a fasi sincronizzate, in cui un numero di partecipanti non fisso nel tempo
//! deve completare ciascuno il proprio lavoro prima che chiunque possa procedere alla fase successiva,
//! non può usare un contatore fissato a priori come in un barrier classico: i partecipanti possono unirsi
//! o ritirarsi durante l'esecuzione, e l'ultimo evento che soddisfa i requisiti della fase corrente — sia esso
//! un arrivo o un ritiro — deve far avanzare la fase.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `PhaserParty` e `Phaser` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait PhaserParty: Send {
//!     fn arrive_and_wait(&self);
//!     fn deregister(self);
//! }
//!
//! pub trait Phaser: Clone + Send + Sync {
//!     fn register(&self) -> impl PhaserParty + 'static;
//!     fn phase(&self) -> u64;
//! }
//!
//! pub fn make_phaser() -> impl Phaser {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Una fase si completa quando il numero di `arrive_and_wait` ricevuti per essa eguaglia il numero di partecipanti attualmente registrati.
//! - Registrare un nuovo partecipante mentre altri sono già in attesa della fase corrente obbliga anche loro ad attendere l'arrivo del nuovo partecipante.
//! - `deregister()` può, da solo, causare il completamento della fase corrente (se era l'ultimo mancante), risvegliando tutti gli attendenti.
//! - Un partecipante bloccato in `arrive_and_wait()` deve risvegliarsi solo quando la fase che stava effettivamente attendendo avanza.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::sync::{Arc, Condvar, Mutex};

/// Trait che rappresenta un partecipante registrato a un Phaser multi-fase dinamico.
pub trait PhaserParty: Send {
    /// Segnala l'arrivo alla fase corrente e blocca il chiamante finché tutti i partecipanti registrati non sono arrivati.
    fn arrive_and_wait(&self);

    /// Deregistra definitivamente questo partecipante.
    /// Se era l'ultimo partecipante mancante per completare la fase, la fa avanzare immediatamente.
    fn deregister(self);
}

/// Trait che rappresenta il sincronizzatore a fasi dinamiche (Phaser).
pub trait Phaser: Clone + Send + Sync {
    /// Registra un nuovo partecipante, incrementando il numero di arrivi richiesti per la fase corrente.
    fn register(&self) -> impl PhaserParty + 'static;

    /// Restituisce il numero della fase corrente, a partire da 0.
    fn phase(&self) -> u64;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================


pub struct MyParty {
    shared_phaser: Arc<(Mutex<PhaserState>, Condvar)>
}

impl PhaserParty for MyParty {
    fn arrive_and_wait(&self) {
        let (mutex, cvar) = &*self.shared_phaser;
        let mut guard = mutex.lock().unwrap();
        let actual_phase = guard.phase;
        guard.num_arrived += 1;

        if guard.num_arrived == guard.num_participants {
            guard.phase += 1;
            guard.num_arrived = 0;
            drop(guard);
            cvar.notify_all();
        }
        else {
            guard = cvar.wait_while(guard, |c| {
                c.phase == actual_phase
            }).unwrap();
        }
    }

    fn deregister(self) {
        let (mutex, cvar) = &*self.shared_phaser;
        let mut guard = mutex.lock().unwrap();
        guard.num_participants -= 1;

        if guard.num_participants > 0 && guard.num_arrived == guard.num_participants {
            guard.phase += 1;
            guard.num_arrived = 0;
            drop(guard);
            cvar.notify_all();
        }
        else if guard.num_participants == 0 {
            guard.num_arrived = 0;
        }
    }
}

pub struct PhaserState {
    phase: u64,
    num_arrived: usize,
    num_participants: usize
}

pub struct MyPhaser {
    inner: Arc<(Mutex<PhaserState>, Condvar)>
}

impl MyPhaser {
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(PhaserState {
                phase: 0,
                num_arrived: 0,
                num_participants: 0
            }), Condvar::new()))
        }
    }
}

impl Phaser for MyPhaser {
    fn phase(&self) -> u64 {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.phase
    }
    
    fn register(&self) -> impl PhaserParty + 'static {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.num_participants += 1;
        MyParty {
            shared_phaser: self.inner.clone()
        }
    }
}

impl Clone for MyPhaser {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `Phaser`.
pub fn make_phaser() -> impl Phaser {
    MyPhaser::new()
}
