//! # Simulazione 004 — TaskExecutor (Crate Root)
//!
//! Questo modulo radice definisce il tratto pubblico `TaskExecutor` ed esporta
//! due implementazioni architetturali concorrenti:
//!
//! 1. **[`condvar_executor`]**: Implementazione classica basata su primitive a basso livello (`Mutex` + `Condvar`).
//! 2. **[`mpsc_executor`]**: Implementazione idiomatica basata su canali `std::sync::mpsc` e `thread::JoinHandle`.

pub mod condvar_executor;
pub mod mpsc_executor;

/// Trait che rappresenta un executor a singolo thread dedicato per task FIFO.
pub trait TaskExecutor: Send + Sync {
    /// Accoda `task` per l'esecuzione sul thread di lavoro interno.
    fn submit<F: FnOnce() + Send + 'static>(&self, task: F) -> bool;

    /// Impedisce l'accodamento di nuovi task.
    fn close(&self);

    /// Blocca il chiamante finché il thread di lavoro non ha eseguito tutti i task rimanenti e si è fermato.
    fn join(&self);
}

// Re-export dei costruttori pubblici con nomi logici e distinti
pub use condvar_executor::make_condvar_task_executor;
pub use mpsc_executor::make_mpsc_task_executor;

/// Costruttore predefinito (punto di ingresso standard).
pub fn make_task_executor() -> impl TaskExecutor {
    make_condvar_task_executor()
}
