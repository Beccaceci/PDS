//! # Implementazione 2: TransactionalQueue basata su sync_channel & mpsc (`sync_channel_queue.rs`)
//!
//! Come studiato a lezione, la semantica a permessi e backpressure di una coda transazionale
//! si sposa naturalmente con i canali sincronizzati (`std::sync::mpsc::sync_channel`):
//!
//! ## Architettura dell'Implementazione:
//! 1. **Permit Pool Bounded (`sync_channel(capacity)`)**:
//!    - Un canale sincrono `permits: (SyncSender<()>, Receiver<()>)` di capacità `capacity`
//!      rappresenta esattamente i permessi/slot disponibili.
//!    - All'avvio, il canale viene inizializzato con `capacity` token `()`.
//! 2. **Rendezvous e Prenotazione (`reserve`)**:
//!    - `reserve()` tenta di prelevare un token da `permit_rx.recv()`. Se tutti i permessi sono occupati,
//!      il thread viene automaticamente sospeso a costo zero di CPU.
//!    - All'acquisizione del token, restituisce un guard RAII `SyncReservationHandle` che possiede
//!      il proprio clone di `Sender<T>`.
//! 3. **Due vie di uscita (`commit` vs `Drop`)**:
//!    - Se viene chiamato `commit(self, value)`: invia il valore sul canale dati `data_tx.send(value)`
//!      e imposta `committed = true`. Il permesso NON viene restituito ora, perché lo slot è occupato dal dato in coda!
//!    - Se l'handle esce dallo scope senza commit (`Drop`): restituisce immediatamente il token con `permit_tx.send(())`.
//! 4. **Estrazione e Rilascio Slot (`pop`)**:
//!    - `pop()` estrae l'elemento dal canale dati `data_rx.recv()`.
//!    - Quando l'elemento viene consumato, restituisce il token con `permit_tx.send(())`, liberando lo slot per i produttori.
//!    - Quando la coda viene chiusa e tutte le prenotazioni pendenti terminano, tutti i trasmettitori vengono distrutti
//!      e `pop()` restituisce `None` al termine del drenaggio.

use super::{Reservation, TransactionalQueue};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

/// Handle RAII per la versione basata su canali.
pub struct SyncReservationHandle<T: Send> {
    permit_tx: mpsc::SyncSender<()>,
    data_tx: mpsc::Sender<T>,
    committed: bool,
}

impl<T: Send> Reservation<T> for SyncReservationHandle<T> {
    fn commit(mut self, value: T) {
        self.committed = true;
        let _ = self.data_tx.send(value);
    }
}

impl<T: Send> Drop for SyncReservationHandle<T> {
    fn drop(&mut self) {
        // Se la prenotazione viene abbandonata senza commit, restituisce il token al canale dei permessi
        if !self.committed {
            let _ = self.permit_tx.try_send(());
        }
    }
}

/// Stato condiviso dell'implementazione con canali sincroni.
struct SharedState<T: Send> {
    permit_tx: mpsc::SyncSender<()>,
    permit_rx: Mutex<mpsc::Receiver<()>>,
    data_tx: Mutex<Option<mpsc::Sender<T>>>,
    data_rx: Mutex<mpsc::Receiver<T>>,
    closed: AtomicBool,
    capacity: usize,
}

/// Coda transazionale basata su canali standard `sync_channel`.
pub struct SyncChannelTransactionalQueue<T: Send> {
    state: Arc<SharedState<T>>,
}

impl<T: Send> SyncChannelTransactionalQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let (permit_tx, permit_rx) = mpsc::sync_channel::<()>(capacity);

        // Pre-popoliamo il canale con `capacity` token
        for _ in 0..capacity {
            let _ = permit_tx.send(());
        }

        let (data_tx, data_rx) = mpsc::channel::<T>();

        Self {
            state: Arc::new(SharedState {
                permit_tx,
                permit_rx: Mutex::new(permit_rx),
                data_tx: Mutex::new(Some(data_tx)),
                data_rx: Mutex::new(data_rx),
                closed: AtomicBool::new(false),
                capacity,
            }),
        }
    }
}

impl<T: Send> Clone for SyncChannelTransactionalQueue<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T: Send + 'static> TransactionalQueue<T> for SyncChannelTransactionalQueue<T> {
    fn capacity(&self) -> usize {
        self.state.capacity
    }

    fn reserve(&self) -> Option<impl Reservation<T> + 'static> {
        if self.state.closed.load(SeqCst) {
            return None;
        }

        // Blocca finché un permesso non è disponibile
        let permit_rx = self.state.permit_rx.lock().unwrap();
        match permit_rx.recv() {
            Ok(()) => {
                if self.state.closed.load(SeqCst) {
                    // Coda chiusa durante l'attesa: restituisce il token
                    let _ = self.state.permit_tx.try_send(());
                    None
                } else {
                    let data_tx_guard = self.state.data_tx.lock().unwrap();
                    if let Some(ref tx) = *data_tx_guard {
                        Some(SyncReservationHandle {
                            permit_tx: self.state.permit_tx.clone(),
                            data_tx: tx.clone(),
                            committed: false,
                        })
                    } else {
                        let _ = self.state.permit_tx.try_send(());
                        None
                    }
                }
            }
            Err(_) => None,
        }
    }

    fn reserve_timeout(&self, timeout: Duration) -> Option<impl Reservation<T> + 'static> {
        if self.state.closed.load(SeqCst) {
            return None;
        }

        let permit_rx = self.state.permit_rx.lock().unwrap();
        match permit_rx.recv_timeout(timeout) {
            Ok(()) => {
                if self.state.closed.load(SeqCst) {
                    let _ = self.state.permit_tx.try_send(());
                    None
                } else {
                    let data_tx_guard = self.state.data_tx.lock().unwrap();
                    if let Some(ref tx) = *data_tx_guard {
                        Some(SyncReservationHandle {
                            permit_tx: self.state.permit_tx.clone(),
                            data_tx: tx.clone(),
                            committed: false,
                        })
                    } else {
                        let _ = self.state.permit_tx.try_send(());
                        None
                    }
                }
            }
            Err(_) => None,
        }
    }

    fn pop(&self) -> Option<T> {
        let rx = self.state.data_rx.lock().unwrap();
        match rx.recv() {
            Ok(value) => {
                // Elemento consumato: restituisce il token al permit pool
                let _ = self.state.permit_tx.try_send(());
                Some(value)
            }
            Err(_) => None,
        }
    }

    fn close(&self) {
        self.state.closed.store(true, SeqCst);

        // Distruggere il data_tx del repository principale chiude il canale per nuove prenotazioni,
        // ma le prenotazioni già aperte mantengono il loro Sender clonato finché non committano o droppano!
        let mut guard = self.state.data_tx.lock().unwrap();
        let _ = guard.take();

        // Rilascia token per svegliare eventuali thread bloccati in reserve()
        for _ in 0..self.state.capacity {
            let _ = self.state.permit_tx.try_send(());
        }
    }
}

/// Costruttore per la versione basata su `sync_channel`.
pub fn make_sync_channel_transactional_queue<T: Send + 'static>(capacity: usize) -> impl TransactionalQueue<T> {
    SyncChannelTransactionalQueue::new(capacity)
}
