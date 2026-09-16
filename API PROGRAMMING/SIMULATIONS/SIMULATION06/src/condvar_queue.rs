//! # Implementazione 1: TransactionalQueue con Condvar & Mutex (`condvar_queue.rs`)
//!
//! Questa implementazione utilizza le primitive a basso livello (`Mutex` + `Condvar`):
//! - **`SharedState<T>`**: Mantiene la coda FIFO degli elementi committati (`VecDeque<T>`),
//!   il contatore dei permessi disponibili (`open_reservations`), e il flag `closed`.
//! - **Gestione Permessi**:
//!   - `reserve()`: consuma 1 permesso (`open_reservations -= 1`).
//!   - `commit()`: inserisce il dato in coda e imposta `committed = true` (senza restituire il permesso).
//!   - `Drop` su `ReservationHandle`: se `!committed`, restituisce 1 permesso (`open_reservations += 1`).
//!   - `pop()`: estrae l'elemento e restituisce 1 permesso (`open_reservations += 1`).

use super::{Reservation, TransactionalQueue};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Handle RAII per una prenotazione aperta.
pub struct CondvarReservationHandle<T: Send> {
    shared_state: Arc<(Mutex<SharedState<T>>, Condvar, Condvar)>,
    committed: AtomicBool,
}

impl<T: Send> Reservation<T> for CondvarReservationHandle<T> {
    fn commit(self, value: T) {
        if !self.committed.load(SeqCst) {
            self.committed.store(true, SeqCst);
            let (mutex, cvar_not_empty, _) = &*self.shared_state;
            let mut guard = mutex.lock().unwrap();

            guard.queue.push_back(value);
            drop(guard);
            cvar_not_empty.notify_one();
        }
    }
}

impl<T: Send> Drop for CondvarReservationHandle<T> {
    fn drop(&mut self) {
        // Se la prenotazione viene distrutta senza commit, rilascia il permesso allo slot
        if !self.committed.load(SeqCst) {
            let (mutex, _, cvar_not_full) = &*self.shared_state;
            let mut guard = mutex.lock().unwrap();

            guard.open_reservations += 1;
            drop(guard);
            cvar_not_full.notify_one();
        }
    }
}

/// Stato condiviso interno.
pub struct SharedState<T: Send> {
    queue: VecDeque<T>,
    open_reservations: usize,
    closed: bool,
}

impl<T: Send> SharedState<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            open_reservations: capacity,
            closed: false,
        }
    }
}

/// Implementazione di TransactionalQueue basata su Mutex e Condvar.
pub struct CondvarTransactionalQueue<T: Send> {
    state: Arc<(Mutex<SharedState<T>>, Condvar, Condvar)>,
    capacity: usize,
}

impl<T: Send> CondvarTransactionalQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new((
                Mutex::new(SharedState::new(capacity)),
                Condvar::new(), // cvar_not_empty
                Condvar::new(), // cvar_not_full
            )),
            capacity,
        }
    }
}

impl<T: Send> Clone for CondvarTransactionalQueue<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            capacity: self.capacity,
        }
    }
}

impl<T: Send + 'static> TransactionalQueue<T> for CondvarTransactionalQueue<T> {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn reserve(&self) -> Option<impl Reservation<T> + 'static> {
        let (mutex, _, cvar_not_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard = cvar_not_full
            .wait_while(guard, |c| c.open_reservations == 0 && !c.closed)
            .unwrap();

        if guard.closed {
            None
        } else {
            let new_handle = CondvarReservationHandle {
                shared_state: self.state.clone(),
                committed: AtomicBool::new(false),
            };

            guard.open_reservations -= 1;
            Some(new_handle)
        }
    }

    fn reserve_timeout(&self, timeout: Duration) -> Option<impl Reservation<T> + 'static> {
        let (mutex, _, cvar_not_full) = &*self.state;
        let guard = mutex.lock().unwrap();

        let (guard, timeout_result) = cvar_not_full
            .wait_timeout_while(guard, timeout, |c| {
                c.open_reservations == 0 && !c.closed
            })
            .unwrap();

        if guard.closed || timeout_result.timed_out() {
            None
        } else {
            let mut guard = guard;
            let new_handle = CondvarReservationHandle {
                shared_state: self.state.clone(),
                committed: AtomicBool::new(false),
            };

            guard.open_reservations -= 1;
            Some(new_handle)
        }
    }

    fn pop(&self) -> Option<T> {
        let (mutex, cvar_not_empty, cvar_not_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard = cvar_not_empty
            .wait_while(guard, |c| {
                // Attende finché non ci sono elementi committati AND
                // (la coda è aperta OPPURE ci sono ancora prenotazioni aperte che potrebbero committare)
                c.queue.is_empty() && (!c.closed || (c.open_reservations < self.capacity && c.open_reservations > 0))
            })
            .unwrap();

        if let Some(element) = guard.queue.pop_front() {
            guard.open_reservations += 1;
            drop(guard);
            cvar_not_full.notify_one();
            Some(element)
        } else {
            None
        }
    }

    fn close(&self) {
        let (mutex, cvar_not_empty, cvar_not_full) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        guard.closed = true;
        drop(guard);
        cvar_not_empty.notify_all();
        cvar_not_full.notify_all();
    }
}

/// Costruttore per la versione con Condvar e Mutex.
pub fn make_condvar_transactional_queue<T: Send + 'static>(capacity: usize) -> impl TransactionalQueue<T> {
    CondvarTransactionalQueue::new(capacity)
}
