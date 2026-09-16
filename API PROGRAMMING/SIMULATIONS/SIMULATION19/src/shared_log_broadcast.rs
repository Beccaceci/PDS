//! # Implementazione 2: Broadcast a Log Condiviso con Cursore (`shared_log_broadcast.rs`)
//!
//! Questa implementazione evita la moltiplicazione delle code mantenendo un **unico log globale** di messaggi:
//! - Il canale mantiene un singolo `Arc<(Mutex<LogState<T>>, Condvar)>` contenente `messages: Vec<T>`.
//! - Ciascuna `Subscription` possiede soltanto un indice scalare locale (`cursor: usize`), senza allocare code individuali.
//! - Non ci sono lock annidati: esiste un solo livello di `Mutex` + `Condvar` condiviso.

use super::Broadcast;
use std::sync::{Arc, Condvar, Mutex};

/// Stato condiviso per l'implementazione a log centrale.
struct LogState<T> {
    /// Vettore sequenziale di tutti i messaggi pubblicati.
    messages: Vec<T>,
    /// Flag che indica se il broadcast è stato chiuso.
    closed: bool,
}

/// Sottoscrizione con cursore sequenziale sul log condiviso.
pub struct SharedLogSubscription<T: Send + Clone> {
    shared: Arc<(Mutex<LogState<T>>, Condvar)>,
    /// Posizione di lettura del prossimo messaggio per questo subscriber.
    cursor: usize,
}

impl<T: Send + Clone> Iterator for SharedLogSubscription<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let (mutex, cvar) = &*self.shared;
        let mut guard = mutex.lock().unwrap();

        loop {
            // Se ci sono nuovi messaggi disponibili oltre la nostra posizione attuale
            if self.cursor < guard.messages.len() {
                let msg = guard.messages[self.cursor].clone();
                self.cursor += 1;
                return Some(msg);
            }

            // Se il canale è chiuso e non ci sono messaggi residui da leggere
            if guard.closed {
                return None;
            }

            // Attesa passiva finché non viene pubblicato un nuovo messaggio o chiuso il canale
            guard = cvar
                .wait_while(guard, |s| s.messages.len() <= self.cursor && !s.closed)
                .unwrap();
        }
    }
}

/// Canale di broadcast a log centralizzato.
pub struct SharedLogBroadcast<T: Send + Clone> {
    inner: Arc<(Mutex<LogState<T>>, Condvar)>,
}

impl<T: Send + Clone> SharedLogBroadcast<T> {
    /// Inizializza un nuovo canale a log condiviso.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((
                Mutex::new(LogState {
                    messages: Vec::new(),
                    closed: false,
                }),
                Condvar::new(),
            )),
        }
    }
}

impl<T: Send + Clone + 'static> Broadcast<T> for SharedLogBroadcast<T> {
    fn subscribe(&self) -> impl Iterator<Item = T> + Send + 'static {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        // L'iscritto inizia a leggere a partire dai messaggi pubblicati da questo istante in poi
        let initial_cursor = guard.messages.len();
        drop(guard);

        SharedLogSubscription {
            shared: Arc::clone(&self.inner),
            cursor: initial_cursor,
        }
    }

    fn publish(&self, value: T) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        if !guard.closed {
            guard.messages.push(value);
            drop(guard);
            cvar.notify_all();
        }
    }

    fn close(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        cvar.notify_all();
    }
}

impl<T: Send + Clone> Clone for SharedLogBroadcast<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Costruttore per la versione a log condiviso.
pub fn make_shared_log_broadcast<T: Send + Clone + 'static>() -> impl Broadcast<T> {
    SharedLogBroadcast::new()
}
