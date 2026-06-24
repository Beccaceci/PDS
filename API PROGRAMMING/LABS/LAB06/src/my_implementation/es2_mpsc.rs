#![allow(dead_code)]

//! Exercise 2 - Manually Implemented MPSC Channel
//!
//! A manual implementation of an asynchronous, unbounded Multi-Producer Single-Consumer (MPSC)
//! communication channel, similar to `std::sync::mpsc`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Error returned by `Sender::send` when the `Receiver` has been destroyed.
/// It wraps the item that could not be delivered so the caller can reclaim it.
#[derive(Debug)]
pub struct SendError<T>(pub T);

/// Error returned by `Receiver::recv` when the channel is empty and all `Sender`s are dropped.
#[derive(Debug, PartialEq, Eq)]
pub struct RecvError;

/// Error returned by the non-blocking `Receiver::try_recv`.
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is currently empty, but at least one `Sender` still exists.
    Empty,
    /// All `Sender`s have been destroyed and the internal queue is empty.
    Disconnected,
}

/// Internal state shared between Senders and the Receiver.
pub struct ChannelState<T> {
    /// Buffer for messages sent but not yet received.
    messages: VecDeque<T>,
    /// Flag to track if the Receiver still exists.
    receiver_alive: bool,
    /// Count of active Senders.
    n_senders: usize,
}

impl<T> ChannelState<T> {
    /// Initializes the state with an empty queue and assumes at least one sender exists initially.
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            receiver_alive: true,
            n_senders: 1, // Created with the first Sender
        }
    }
}

/// The core synchronization structure held by an Arc.
pub struct Inner<T> {
    /// Protects the shared ChannelState.
    queue: Mutex<ChannelState<T>>,
    /// Used to wake up the Receiver when new data or disconnection occurs.
    cv: Condvar,
}

impl<T> Inner<T> {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(ChannelState::new()),
            cv: Condvar::new(),
        }
    }
}

// =============================================================================
// SENDER IMPLEMENTATION
// =============================================================================

/// The sending end of the channel can be cloned to support multiple producers.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    /// Internal constructor for a Sender handle.
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Sends an item into the channel. Never blocks for capacity
    /// Returns `Err(SendError(item))` if the `Receiver` is dropped.
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let mut state = self.inner.queue.lock().unwrap();

        if state.receiver_alive {
            state.messages.push_back(item);
            // Notify the receiver that new data is available.
            self.inner.cv.notify_one();
            Ok(())
        } else {
            // Receiver is gone; return the item back to the caller.
            Err(SendError(item))
        }
    }
}

impl<T> Clone for Sender<T> {
    /// Creates a new Sender handle pointing to the same channel.
    fn clone(&self) -> Self {
        let mut state = self.inner.queue.lock().unwrap();
        state.n_senders += 1;
        Sender::new(Arc::clone(&self.inner))
    }
}

impl<T> Drop for Sender<T> {
    /// Decrements the sender count when a handle is destroyed.
    fn drop(&mut self) {
        let mut state = self.inner.queue.lock().unwrap();
        state.n_senders -= 1;

        // If this was the last sender, notify the receiver so it can stop waiting.
        if state.n_senders == 0 {
            self.inner.cv.notify_all();
        }
    }
}

// =============================================================================
// RECEIVER IMPLEMENTATION
// =============================================================================

/// The receiving end of the channel. Only one Receiver can exist per channel.
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    /// Internal constructor for the Receiver handle.
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Extracts the next element. Blocks passively if the queue is empty.
    /// Returns `Err(RecvError)` only if the queue is empty AND no senders remain.
    pub fn recv(&self) -> Result<T, RecvError> {
        let mut state = self.inner.queue.lock().unwrap();

        // Wait while the channel is alive but empty
        state = self.inner.cv.wait_while(state, |s| {
            s.n_senders > 0 && s.messages.is_empty()
        }).unwrap();

        // Check if we have data to return, prioritising data over disconnection status.
        if let Some(item) = state.messages.pop_front() {
            Ok(item)
        } else {
            // Queue is empty and no more senders exist.
            Err(RecvError)
        }
    }

    /// Non-blocking variant of recv. Returns immediately.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = self.inner.queue.lock().unwrap();

        if let Some(item) = state.messages.pop_front() {
            Ok(item)
        } else if state.n_senders == 0 {
            // Queue is empty and no one is left to send.[span_42](end_span)
            Err(TryRecvError::Disconnected)
        } else {
            // Queue is empty but senders are still active.
            Err(TryRecvError::Empty)
        }
    }
}

impl<T> Drop for Receiver<T> {
    /// Marks the channel as closed so Senders can detect it.
    fn drop(&mut self) {
        let mut state = self.inner.queue.lock().unwrap();
        state.receiver_alive = false;
        // No need to notify here as Senders don't block in this implementation.
    }
}

/// Creates a new (Sender, Receiver) pair
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner::new());
    (Sender::new(Arc::clone(&inner)), Receiver::new(inner))
}


/// === TESTS ===

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn send_e_recv_singolo_thread() {
        let (tx, rx) = channel::<i32>();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
    }

    #[test]
    fn try_recv_su_canale_vuoto() {
        let (tx, rx) = channel::<i32>();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.send(42).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn recv_si_blocca_finche_arriva_un_messaggio() {
        let (tx, rx) = channel::<i32>();
        let ricevuti = Arc::new(AtomicUsize::new(0));

        let r2 = Arc::clone(&ricevuti);
        let consumer = thread::spawn(move || {
            let v = rx.recv().unwrap();
            assert_eq!(v, 99);
            r2.fetch_add(1, Ordering::SeqCst);
        });

        // Diamo tempo al consumatore di mettersi in attesa.
        thread::sleep(Duration::from_millis(100));
        assert_eq!(ricevuti.load(Ordering::SeqCst), 0);

        tx.send(99).unwrap();
        consumer.join().unwrap();
        assert_eq!(ricevuti.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn recv_restituisce_errore_quando_tutti_i_sender_droppati() {
        let (tx, rx) = channel::<i32>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn recv_si_sblocca_alla_chiusura_dei_sender() {
        // Il Receiver è bloccato su recv quando l'ultimo Sender viene droppato:
        // recv deve sbloccarsi e restituire RecvError.
        let (tx, rx) = channel::<i32>();
        let sbloccato = Arc::new(AtomicUsize::new(0));

        let s2 = Arc::clone(&sbloccato);
        let consumer = thread::spawn(move || {
            let r = rx.recv();
            assert_eq!(r, Err(RecvError));
            s2.fetch_add(1, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(100));
        assert_eq!(sbloccato.load(Ordering::SeqCst), 0);

        drop(tx);
        consumer.join().unwrap();
        assert_eq!(sbloccato.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn messaggi_residui_letti_anche_dopo_chiusura() {
        // Se i Sender vengono droppati ma ci sono ancora messaggi in coda,
        // il Receiver deve poterli leggere; solo dopo riceverà RecvError.
        let (tx, rx) = channel::<i32>();
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        drop(tx);

        assert_eq!(rx.recv(), Ok(10));
        assert_eq!(rx.recv(), Ok(20));
        assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn send_fallisce_se_receiver_droppato() {
        let (tx, rx) = channel::<i32>();
        drop(rx);
        match tx.send(1) {
            Err(SendError(v)) => assert_eq!(v, 1),
            Ok(_) => panic!("send avrebbe dovuto fallire dopo il drop del Receiver"),
        }
    }

    #[test]
    fn sender_clonabile_multi_producer() {
        let (tx, rx) = channel::<i32>();
        let n_prod = 5;
        let per_prod = 100;

        let mut handles = vec![];
        for _ in 0..n_prod {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_prod {
                    tx.send(i).unwrap();
                }
            }));
        }
        // Rilasciamo l'originale: ne restano `n_prod` cloni.
        drop(tx);

        let mut ricevuti = 0usize;
        while let Ok(_) = rx.recv() {
            ricevuti += 1;
        }
        assert_eq!(ricevuti, n_prod * per_prod as usize);
    }

    #[test]
    fn ordine_fifo_da_singolo_sender() {
        // Da un singolo Sender, i messaggi devono arrivare nello stesso ordine
        // in cui sono stati inviati.
        let (tx, rx) = channel::<i32>();
        let producer = thread::spawn(move || {
            for i in 0..1000 {
                tx.send(i).unwrap();
            }
        });

        let mut atteso = 0;
        while let Ok(v) = rx.recv() {
            assert_eq!(v, atteso);
            atteso += 1;
        }
        assert_eq!(atteso, 1000);
        producer.join().unwrap();
    }

    #[test]
    fn drop_canale_senza_ricezioni_non_blocca() {
        // Costruire ed eliminare canali senza usarli non deve causare
        // attese o blocchi.
        for _ in 0..100 {
            let (_tx, _rx) = channel::<i32>();
        }
    }
}