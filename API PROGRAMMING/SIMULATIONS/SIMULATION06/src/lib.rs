//! # Simulazione 006 — TransactionalQueue (Crate Root)
//!
//! Questo modulo radice definisce i tratti pubblici `Reservation<T: Send>` e `TransactionalQueue<T: Send>`,
//! ed esporta due implementazioni architetturali concorrenti:
//!
//! 1. **[`condvar_queue`]**: Implementazione classica basata su primitive a basso livello (`Mutex` + `Condvar`).
//! 2. **[`sync_channel_queue`]**: Implementazione idiomatica basata su token pool sincrono (`std::sync::mpsc::sync_channel`).

pub mod condvar_queue;
pub mod sync_channel_queue;

use std::time::Duration;

/// Trait che rappresenta una prenotazione aperta di una posizione all'interno di una [`TransactionalQueue`].
pub trait Reservation<T: Send> {
    /// Consuma la prenotazione, inserendo definitivamente `value` in coda al posto riservato.
    /// Se questo metodo non viene mai chiamato e l'oggetto esce dallo scope, lo spazio riservato
    /// deve tornare disponibile automaticamente senza che nulla venga inserito in coda (RAII tramite `Drop`).
    fn commit(self, value: T);
}

/// Trait che rappresenta una coda a capacità limitata con supporto a prenotazioni transazionali a due fasi.
pub trait TransactionalQueue<T: Send>: Clone + Send + Sync {
    /// Riserva una posizione in coda. Se la coda ha già raggiunto la capacità massima
    /// (contando sia gli elementi committati sia le prenotazioni ancora aperte), blocca il chiamante
    /// senza consumare cicli di CPU finché una posizione non si libera. Restituisce `None` se la coda
    /// è chiusa o viene chiusa durante l'attesa.
    fn reserve(&self) -> Option<impl Reservation<T> + 'static>;

    /// Variante con attesa limitata di `reserve`: se non ottiene una posizione entro `timeout`,
    /// rinuncia e restituisce `None`.
    fn reserve_timeout(&self, timeout: Duration) -> Option<impl Reservation<T> + 'static>;

    /// Preleva l'elemento meno recente tra quelli effettivamente committati in coda (FIFO).
    /// Blocca senza consumo di CPU se non ci sono elementi disponibili. Restituisce `None` solo
    /// quando la coda è stata chiusa e non contiene più elementi committati né prenotazioni pendenti.
    fn pop(&self) -> Option<T>;

    /// Chiude la coda impedendo nuove chiamate a `reserve`/`reserve_timeout`.
    /// Le prenotazioni già aperte e gli elementi già committati restano validi.
    fn close(&self);

    /// Restituisce la capacità massima totale della coda.
    fn capacity(&self) -> usize;
}

// Re-export dei costruttori pubblici con nomi logici e distinti
pub use condvar_queue::make_condvar_transactional_queue;
pub use sync_channel_queue::make_sync_channel_transactional_queue;

/// Costruttore predefinito (punto di ingresso standard).
pub fn make_transactional_queue<T: Send + 'static>(capacity: usize) -> impl TransactionalQueue<T> {
    make_condvar_transactional_queue(capacity)
}
