//! # Implementazione 1: Condvar & Mutex Event Bus (`condvar_event_bus.rs`)
//!
//! Questa implementazione utilizza le primitive classiche di sincronizzazione a basso livello (`Mutex` + `Condvar`):
//! - **`SharedBusState<E>`**: Registro centrale che associa a ciascuna iscrizione un ID univoco (`usize`)
//!   e una coda thread-safe indipendente `Arc<(Mutex<SubscriberQueue<E>>, Condvar)>`.
//! - **`SubscriberQueue<E>`**: Buffer FIFO individuale per ciascun iscritto contenente `VecDeque<E>` e un flag `closed`.
//! - **`Item<E>`**: Handle dell'iscrizione con semantica RAII. Alla sua distruzione (`Drop`), rimuove automaticamente l'ID
//!   dal registro del bus in tempo $O(1)$ e risveglia eventuali thread pendenti.

use super::{EventBus, Subscription};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

/// Struttura dati interna che rappresenta la coda FIFO di un singolo iscritto.
struct SubscriberQueue<E> {
    queue: VecDeque<E>,
    closed: bool,
}

/// Stato condiviso globale dell'Event Bus protetto da Mutex.
struct SharedBusState<E> {
    subscribers: HashMap<usize, Arc<(Mutex<SubscriberQueue<E>>, Condvar)>>,
    next_id: usize,
    closed: bool,
}

/// Handle concreto dell'iscrizione basato su `Mutex` + `Condvar` (`impl Subscription<E>`).
pub struct Item<E: Send + Clone + 'static> {
    id: usize,
    sub_state: Arc<(Mutex<SubscriberQueue<E>>, Condvar)>,
    bus_state: Arc<Mutex<SharedBusState<E>>>,
}

impl<E: Send + Clone + 'static> Subscription<E> for Item<E> {
    fn next_event(&self) -> Option<E> {
        let (lock, cvar) = &*self.sub_state;
        let mut guard = lock.lock().unwrap();

        // Attesa bloccante che gestisce i risvegli spuri (spurious wakeups)
        guard = cvar.wait_while(guard, |q| 
            q.queue.is_empty() && !q.closed
        ).unwrap();

        if let Some(event) = guard.queue.pop_front() {
            Some(event)
        } else {
            None
        }
    }
}

impl<E: Send + Clone + 'static> Drop for Item<E> {
    fn drop(&mut self) {
        // Rimuove l'iscritto dal bus centrale in O(1)
        let mut bus_guard = self.bus_state.lock().unwrap();
        bus_guard.subscribers.remove(&self.id);
        drop(bus_guard);

        // Segnala la chiusura della coda locale e sblocca thread in attesa su next_event()
        let (lock, cvar) = &*self.sub_state;
        let mut sub_guard = lock.lock().unwrap();
        sub_guard.closed = true;
        drop(sub_guard);
        cvar.notify_all();
    }
}

/// Struttura principale dell'Event Bus basata su `Mutex` + `Condvar`.
pub struct System<E: Send + Clone + 'static> {
    state: Arc<Mutex<SharedBusState<E>>>,
}

impl<E: Send + Clone + 'static> System<E> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SharedBusState {
                subscribers: HashMap::new(),
                next_id: 0,
                closed: false,
            })),
        }
    }
}

impl<E: Send + Clone + 'static> EventBus<E> for System<E> {
    fn publish(&self, event: E) {
        let bus_guard = self.state.lock().unwrap();
        if bus_guard.closed {
            return;
        }

        // PREVENZIONE DEADLOCK: estrae le code e rilascia il lock del bus prima delle notifiche
        let subscribers: Vec<Arc<(Mutex<SubscriberQueue<E>>, Condvar)>> =
            bus_guard.subscribers.values().cloned().collect();
        drop(bus_guard);

        for sub_state in subscribers {
            let (lock, cvar) = &*sub_state;
            let mut guard = lock.lock().unwrap();
            if !guard.closed {
                guard.queue.push_back(event.clone());
                drop(guard);
                cvar.notify_all();
            }
        }
    }

    fn subscribe(&self) -> impl Subscription<E> + 'static {
        let mut bus_guard = self.state.lock().unwrap();
        let id = bus_guard.next_id;
        bus_guard.next_id += 1;

        let closed = bus_guard.closed;
        let sub_state = Arc::new((
            Mutex::new(SubscriberQueue {
                queue: VecDeque::new(),
                closed
            }),
            Condvar::new(),
        ));

        if !closed {
            bus_guard.subscribers.insert(id, Arc::clone(&sub_state));
        }

        Item {
            id,
            sub_state,
            bus_state: Arc::clone(&self.state),
        }
    }

    fn subscriber_count(&self) -> usize {
        let bus_guard = self.state.lock().unwrap();
        bus_guard.subscribers.len()
    }

    fn close(&self) {
        let mut bus_guard = self.state.lock().unwrap();
        bus_guard.closed = true;

        let subscribers: Vec<Arc<(Mutex<SubscriberQueue<E>>, Condvar)>> =
            bus_guard.subscribers.values().cloned().collect();
        bus_guard.subscribers.clear();
        drop(bus_guard);

        for sub_state in subscribers {
            let (lock, cvar) = &*sub_state;
            let mut guard = lock.lock().unwrap();
            guard.closed = true;
            drop(guard);
            cvar.notify_all();
        }
    }
}

impl<E: Send + Clone + 'static> Clone for System<E> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

/// Costruttore pubblico per l'implementazione basata su Condvar e Mutex.
pub fn make_condvar_event_bus<E: Send + Clone + 'static>() -> impl EventBus<E> {
    System::new()
}
