//! # Simulazione 003 — Exchanger (Crate Root)
//!
//! Questo modulo radice definisce il tratto pubblico `Exchanger<T: Send>` ed esporta
//! due implementazioni architetturali concorrenti:
//!
//! 1. **[`condvar_exchanger`]**: Implementazione classica basata su primitive a basso livello (`Mutex` + `Condvar`).
//! 2. **[`mpsc_exchanger`]**: Implementazione idiomatica basata su canali di rendez-vous (`std::sync::mpsc::channel`).

pub mod condvar_exchanger;
pub mod mpsc_exchanger;

use std::time::Duration;

/// Trait che rappresenta un punto di incontro (Rendezvous Exchanger) per lo scambio simmetrico di valori tra thread.
pub trait Exchanger<T: Send>: Send + Sync {
    /// Blocca il thread chiamante finché un altro thread non chiama a sua volta `exchange`, restituendo il valore dell'altro thread.
    fn exchange(&self, value: T) -> T;

    /// Variante con timeout per l'attesa di uno scambio.
    fn exchange_timeout(&self, value: T, timeout: Duration) -> Result<T, T>;
}

// Re-export dei costruttori pubblici con nomi logici e distinti
pub use condvar_exchanger::make_condvar_exchanger;
pub use mpsc_exchanger::make_mpsc_exchanger;

/// Costruttore predefinito (punto di ingresso standard).
pub fn make_exchanger<T: Send + 'static>() -> impl Exchanger<T> {
    make_condvar_exchanger()
}
