//! # Simulazione 012 — MetricsBoard (Crate Root)
//!
//! Questo modulo radice definisce i tratti pubblici `MetricHandle` e `MetricsBoard`,
//! il tipo di errore `NameAlreadyUsed`, ed esporta due implementazioni concorrenti:
//!
//! 1. **[`condvar_board`]**: Implementazione classica basata su `Mutex<f64>` + `Condvar` a granularità mista.
//! 2. **[`atomic_board`]**: Implementazione ultra-efficiente basata su bit-casting atomico `AtomicU64` e notifica selettiva.

pub mod atomic_board;
pub mod condvar_board;

use std::error::Error;
use std::fmt;

/// Tipo di errore restituito quando si tenta di registrare una metrica con un nome già in uso.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct NameAlreadyUsed;

impl fmt::Display for NameAlreadyUsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metric name is already registered and active")
    }
}

impl Error for NameAlreadyUsed {}

/// Handle associato a una specifica metrica registrata nel board.
/// Consente aggiornamenti ad alta frequenza del valore numerico.
/// Quando l'handle viene distrutto (drop), il nome della metrica torna disponibile per nuove registrazioni.
pub trait MetricHandle: Send + Sync + 'static {
    /// Aggiorna il valore corrente della metrica associata a questo handle.
    /// Pensato per essere invocato con altissima frequenza da più thread,
    /// anche su metriche diverse contemporaneamente.
    fn record(&self, value: f64);
}

/// Pannello di controllo concorrente per la gestione, monitoraggio e sincronizzazione di metriche numeriche.
pub trait MetricsBoard: Send + Sync {
    /// Registra una nuova metrica identificata da `name`. Se il nome è già
    /// in uso da una registrazione ancora attiva, l'operazione fallisce e
    /// restituisce `NameAlreadyUsed`, senza alterare lo stato del board. Il
    /// nome torna disponibile per una nuova registrazione quando l'handle
    /// restituito da questa chiamata viene distrutto.
    fn register(&self, name: &str) -> Result<impl MetricHandle + 'static, NameAlreadyUsed>;

    /// Restituisce una fotografia interamente coerente di tutte le metriche
    /// correntemente registrate, come coppie nome-valore — non un insieme
    /// di letture prese in istanti diversi le une dalle altre.
    fn snapshot(&self) -> Vec<(String, f64)>;

    /// Resta sospeso finché il valore della metrica `name` non è maggiore o
    /// uguale a `threshold`. Si assuma che venga invocato solo su metriche
    /// già registrate.
    fn wait_for_threshold(&self, name: &str, threshold: f64);
}

// Re-export dei costruttori pubblici
pub use atomic_board::make_atomic_metrics_board;
pub use condvar_board::make_condvar_metrics_board;

/// Costruttore predefinito (punto di ingresso standard a massime prestazioni).
pub fn make_metrics_board() -> impl MetricsBoard {
    make_atomic_metrics_board()
}
