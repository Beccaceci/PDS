//! # Implementazione 2: Idiomatica basata su Canali MPSC (`mpsc_event_bus.rs`)
//!
//! Questa implementazione utilizza i canali della libreria standard (`std::sync::mpsc`):
//! - **Sospensione efficiente dei thread**: `recv()` sospende il thread chiamante su primitive del SO senza attesa attiva.
//! - **Disconnessione automatica**: Quando i trasmettitori `Sender` vengono distrutti o la mappa viene svuotata in `close()`,
//!   `recv()` restituisce un errore che `.ok()` converte elegantemente in `None`.

use super::{EventBus, Subscription};
use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

/// Registro centrale condiviso dell'Event Bus basato su `mpsc::Sender`.
struct SharedBusState<E> {
    subscribers: HashMap<usize, mpsc::Sender<E>>,
    next_id: usize,
    closed: bool,
}

/// Handle di iscrizione basato su `mpsc::Receiver<E>`.
pub struct ChannelSubscription<E: Send + Clone + 'static> {
    id: usize,
    rx: Mutex<mpsc::Receiver<E>>,
    bus_state: Arc<Mutex<SharedBusState<E>>>,
}

impl<E: Send + Clone + 'static> Subscription<E> for ChannelSubscription<E> {
    fn next_event(&self) -> Option<E> {
        let guard = self.rx.lock().unwrap();
        // `recv()` sospende il thread ed estrae il messaggio. `.ok()` converte RecvError in None alla disconnessione!
        guard.recv().ok()
    }
}

impl<E: Send + Clone + 'static> Drop for ChannelSubscription<E> {
    fn drop(&mut self) {
        let mut bus_guard = self.bus_state.lock().unwrap();
        bus_guard.subscribers.remove(&self.id);
    }
}

/// Struttura principale dell'Event Bus basata su canali `mpsc`.
#[derive(Clone)]
pub struct ChannelEventBus<E: Send + Clone + 'static> {
    state: Arc<Mutex<SharedBusState<E>>>,
}

impl<E: Send + Clone + 'static> ChannelEventBus<E> {
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

impl<E: Send + Clone + 'static> EventBus<E> for ChannelEventBus<E> {
    fn publish(&self, event: E) {
        let bus_guard = self.state.lock().unwrap();
        if bus_guard.closed {
            return;
        }

        // Invia una copia dell'evento a ciascun trasmettitore registrato
        for tx in bus_guard.subscribers.values() {
            let _ = tx.send(event.clone());
        }
    }

    fn subscribe(&self) -> impl Subscription<E> + 'static {
        let mut bus_guard = self.state.lock().unwrap();
        let (tx, rx) = mpsc::channel();
        let id = bus_guard.next_id;
        bus_guard.next_id += 1;

        if !bus_guard.closed {
            bus_guard.subscribers.insert(id, tx);
        }

        ChannelSubscription {
            id,
            rx: Mutex::new(rx),
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
        // Distruggere i Sender fa disconnettere i canali. `recv().ok()` restituisce automaticamente None!
        bus_guard.subscribers.clear();
    }
}

/// Costruttore pubblico per l'implementazione basata su canali MPSC.
pub fn make_mpsc_event_bus<E: Send + Clone + 'static>() -> impl EventBus<E> {
    ChannelEventBus::new()
}
