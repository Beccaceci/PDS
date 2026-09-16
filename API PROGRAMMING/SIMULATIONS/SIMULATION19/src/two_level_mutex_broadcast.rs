//! # Implementazione 3: Broadcast a Due Livelli di Mutex (`two_level_mutex_broadcast.rs`)
//!
//! Questa implementazione modella esplicitamente code indipendenti per ciascun sottoscrittore:
//! - **Livello Esterno (Outer Mutex)**: Un `Mutex` sul vettore di tutti i canali dei sottoscrittori (`Vec<Arc<(Mutex<SubQueue<T>>, Condvar)>>`).
//! - **Livello Interno (Inner Mutex)**: Un `Mutex` + `Condvar` dedicato per ciascuna coda individuale `VecDeque<T>`.
//! - **Vantaggio**: Ogni sottoscrittore attende solo sulla propria `Condvar` personale, evitando il risveglio a sciame (*thundering herd*) tra sottoscrittori diversi.

use super::Broadcast;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Stato interno della coda privata di un singolo sottoscrittore.
struct SubState<T> {
    queue: VecDeque<T>,
    closed: bool,
}

/// Handle condiviso alla coda del singolo sottoscrittore.
type SubHandle<T> = Arc<(Mutex<SubState<T>>, Condvar)>;

/// Sottoscrizione con coda e lock privati.
pub struct TwoLevelSubscription<T: Send + Clone> {
    inner: SubHandle<T>,
}

impl<T: Send + Clone> Iterator for TwoLevelSubscription<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            if let Some(item) = guard.queue.pop_front() {
                return Some(item);
            }

            if guard.closed {
                return None;
            }

            guard = cvar
                .wait_while(guard, |s| s.queue.is_empty() && !s.closed)
                .unwrap();
        }
    }
}

/// Stato globale del canale di broadcast (Livello Esterno).
struct OuterState<T> {
    subscribers: Vec<SubHandle<T>>,
    closed: bool,
}

/// Canale di broadcast con due livelli di sincronizzazione.
pub struct TwoLevelMutexBroadcast<T: Send + Clone> {
    outer: Arc<Mutex<OuterState<T>>>,
}

impl<T: Send + Clone> TwoLevelMutexBroadcast<T> {
    /// Inizializza un nuovo canale a due livelli di lock.
    pub fn new() -> Self {
        Self {
            outer: Arc::new(Mutex::new(OuterState {
                subscribers: Vec::new(),
                closed: false,
            })),
        }
    }
}

impl<T: Send + Clone + 'static> Broadcast<T> for TwoLevelMutexBroadcast<T> {
    fn subscribe(&self) -> impl Iterator<Item = T> + Send + 'static {
        let mut outer_guard = self.outer.lock().unwrap();

        let sub_handle = Arc::new((
            Mutex::new(SubState {
                queue: VecDeque::new(),
                closed: outer_guard.closed,
            }),
            Condvar::new(),
        ));

        if !outer_guard.closed {
            outer_guard.subscribers.push(Arc::clone(&sub_handle));
        }

        TwoLevelSubscription { inner: sub_handle }
    }

    fn publish(&self, value: T) {
        let mut outer_guard = self.outer.lock().unwrap();
        if outer_guard.closed {
            return;
        }

        // Rimuove automaticamente i subscriber che sono stati droppati (strong_count == 1 indica che solo il vettore li possiede)
        outer_guard
            .subscribers
            .retain(|sub| Arc::strong_count(sub) > 1);

        for sub in &outer_guard.subscribers {
            let (sub_mutex, sub_cvar) = &**sub;
            let mut sub_guard = sub_mutex.lock().unwrap();
            sub_guard.queue.push_back(value.clone());
            drop(sub_guard);
            sub_cvar.notify_one();
        }
    }

    fn close(&self) {
        let mut outer_guard = self.outer.lock().unwrap();
        outer_guard.closed = true;

        for sub in &outer_guard.subscribers {
            let (sub_mutex, sub_cvar) = &**sub;
            let mut sub_guard = sub_mutex.lock().unwrap();
            sub_guard.closed = true;
            drop(sub_guard);
            sub_cvar.notify_all();
        }

        outer_guard.subscribers.clear();
    }
}

impl<T: Send + Clone> Clone for TwoLevelMutexBroadcast<T> {
    fn clone(&self) -> Self {
        Self {
            outer: Arc::clone(&self.outer),
        }
    }
}

/// Costruttore per la versione a due livelli di mutex.
pub fn make_two_level_mutex_broadcast<T: Send + Clone + 'static>() -> impl Broadcast<T> {
    TwoLevelMutexBroadcast::new()
}
