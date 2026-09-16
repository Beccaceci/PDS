//! # Implementazione 1: Broadcast con Canali Standard MPSC (`mpsc_broadcast.rs`)
//!
//! Questa implementazione rappresenta l'approccio più compatto ed idiomatico in Rust:
//! - Utilizza `std::sync::mpsc::{channel, Sender, Receiver}`.
//! - La `Subscription` possiede semplicemente il `Receiver<T>`, delegando a `recv().ok()` la sincronizzazione e l'estrazione.
//! - Non necessita di alcuna `Condvar` esplicita né di code duplicate.

use super::Broadcast;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

/// Sottoscrizione individuale basata su `mpsc::Receiver`.
pub struct MpscSubscription<T: Send + Clone> {
    receiver: Receiver<T>,
}

impl<T: Send + Clone> Iterator for MpscSubscription<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

/// Stato interno per la versione MPSC.
struct MpscState<T> {
    senders: Vec<Sender<T>>,
    closed: bool,
}

/// Canale di broadcast basato su `mpsc`.
pub struct MpscBroadcast<T: Send + Clone> {
    inner: Arc<Mutex<MpscState<T>>>,
}

impl<T: Send + Clone> MpscBroadcast<T> {
    /// Inizializza un nuovo canale di broadcast vuoto.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MpscState {
                senders: Vec::new(),
                closed: false,
            })),
        }
    }
}

impl<T: Send + Clone + 'static> Broadcast<T> for MpscBroadcast<T> {
    fn subscribe(&self) -> impl Iterator<Item = T> + Send + 'static {
        let (tx, rx) = channel::<T>();
        let mut guard = self.inner.lock().unwrap();

        if !guard.closed {
            guard.senders.push(tx);
        }

        MpscSubscription { receiver: rx }
    }

    fn publish(&self, value: T) {
        let mut guard = self.inner.lock().unwrap();
        if !guard.closed {
            guard.senders.retain(|tx| tx.send(value.clone()).is_ok());
        }
    }

    fn close(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.closed = true;
        guard.senders.clear();
    }
}

impl<T: Send + Clone> Clone for MpscBroadcast<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Costruttore per la versione basata su MPSC.
pub fn make_mpsc_broadcast<T: Send + Clone + 'static>() -> impl Broadcast<T> {
    MpscBroadcast::new()
}
