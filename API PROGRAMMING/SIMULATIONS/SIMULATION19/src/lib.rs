//! # Simulazione 019 — Broadcast
//!
//! Un flusso di eventi che più iscritti vogliono consumare uno alla volta, in ordine, bloccandosi
//! quando non c'è nulla di nuovo, è esattamente la semantica di un iteratore Rust — con la differenza
//! che qui il "prossimo elemento" può non essere ancora disponibile e richiedere un'attesa.
//!
//! Si scriva in Rust una struttura che implementi il tratto generico `Broadcast<T: Send + Clone>` definito di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait Broadcast<T: Send + Clone>: Clone + Send + Sync {
//!     fn publish(&self, value: T);
//!     fn subscribe(&self) -> impl Iterator<Item = T> + Send + 'static;
//!     fn close(&self);
//! }
//!
//! pub fn make_broadcast<T: Send + Clone + 'static>() -> impl Broadcast<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - L'oggetto restituito da `subscribe()` deve implementare `Iterator<Item = T> + Send + 'static` e poter essere usato in un ciclo `for`.
//! - Ogni iscritto riceve tutti e soli i valori pubblicati dopo la propria sottoscrizione, nell'ordine di pubblicazione.
//! - `close()` chiude il flusso: nessun iscritto (esistente o futuro) riceverà ulteriori valori.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

pub mod mpsc_broadcast;
pub mod shared_log_broadcast;
pub mod two_level_mutex_broadcast;

pub use mpsc_broadcast::{make_mpsc_broadcast, MpscBroadcast, MpscSubscription};
pub use shared_log_broadcast::{make_shared_log_broadcast, SharedLogBroadcast, SharedLogSubscription};
pub use two_level_mutex_broadcast::{
    make_two_level_mutex_broadcast, TwoLevelMutexBroadcast, TwoLevelSubscription,
};

/// Trait che rappresenta un canale di broadcast 1-a-N in cui ogni iscritto è un `Iterator`.
pub trait Broadcast<T: Send + Clone>: Clone + Send + Sync {
    /// Pubblica un nuovo valore, visibile a tutti gli iscritti creati prima di questa chiamata.
    fn publish(&self, value: T);

    /// Crea un nuovo iscritto. Il tipo restituito implementa `Iterator<Item = T>`:
    /// `next()` blocca finché non è disponibile un nuovo elemento e ritorna `None` dopo la chiusura del canale.
    fn subscribe(&self) -> impl Iterator<Item = T> + Send + 'static;

    /// Chiude il flusso: nessun iscritto riceverà ulteriori valori oltre a quelli già pubblicati.
    fn close(&self);
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `Broadcast` (default: implementazione MPSC).
pub fn make_broadcast<T: Send + Clone + 'static>() -> impl Broadcast<T> {
    make_mpsc_broadcast()
}
