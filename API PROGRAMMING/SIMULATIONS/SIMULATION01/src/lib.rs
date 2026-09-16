//! # Simulazione 001 — EventBus (Crate Root)
//!
//! Questo modulo radice definisce i tratti pubblici condivisi `Subscription<E>` ed `EventBus<E>`,
//! ed esporta le due implementazioni architetturali concorrenti:
//!
//! 1. **[`condvar_event_bus`]**: Implementazione classica basata su `Mutex`, `Condvar` e `HashMap` di code `VecDeque`.
//! 2. **[`mpsc_event_bus`]**: Implementazione idiomatica basata su canali `std::sync::mpsc`.

pub mod condvar_event_bus;
pub mod mpsc_event_bus;

/// Trait che rappresenta un'iscrizione attiva a un [`EventBus`].
pub trait Subscription<E: Send + Clone>: Send {
    /// Attende e restituisce il prossimo evento pubblicato sul bus dopo la creazione di questa iscrizione.
    fn next_event(&self) -> Option<E>;
}

/// Trait che rappresenta un Event Bus concorrente basato sul pattern Publish/Subscribe.
pub trait EventBus<E: Send + Clone>: Clone + Send + Sync {
    /// Pubblica un evento `event` sul bus, consegnandolo a tutti gli iscritti correntemente attivi.
    fn publish(&self, event: E);

    /// Crea e restituisce una nuova iscrizione al bus (`Subscription<E>`).
    fn subscribe(&self) -> impl Subscription<E> + 'static;

    /// Restituisce il numero di iscrizioni correntemente attive registrate sul bus.
    fn subscriber_count(&self) -> usize;

    /// Chiude il bus di eventi.
    fn close(&self);
}

// Re-export dei costruttori con nomi distinti e logici
pub use condvar_event_bus::make_condvar_event_bus;
pub use mpsc_event_bus::make_mpsc_event_bus;