//! # Simulazione 002 — Forgettable Channel
//!
//! Nei sistemi di elaborazione concorrente di task prioritari,
//! i produttori accodano compiti di lavoro associando loro un livello di priorità.
//! Ciascuna sottomissione restituisce un handle di annullamento dinamico che consente
//! al produttore di cancellare il compito prima che esso venga estratto dal consumatore.
//! Se il compito viene annullato prima dell'estrazione, il consumatore deve scartarlo
//! silenziosamente ed elaborare il successivo compito disponibile a priorità più alta,
//! senza mai generare attesa attiva (busy-waiting).
//!
//! Si scriva in Rust una struttura che implementi le interfacce trasmesse dai tratti
//! `WorkProducer<T: Send>` e `WorkConsumer<T: Send>`.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait CancelableHandle: Send + Sync {
//!     fn cancel(&self) -> bool;
//!     fn is_done(&self) -> bool;
//! }
//!
//! pub trait WorkProducer<T: Send>: Clone + Send + Sync {
//!     fn submit(&self, task: T, priority: u8) -> Option<Arc<dyn CancelableHandle>>;
//! }
//!
//! pub trait WorkConsumer<T: Send>: Clone + Send + Sync {
//!     fn pop_next(&self) -> Option<T>;
//! }
//!
//! pub fn create_channel<T: Send>() -> (impl WorkProducer<T>, impl WorkConsumer<T>) {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! 1. Topologia Multi-Producer / Single-Consumer o Multi-Consumer: Supportare più produttori tramite il tratto Clone su `WorkProducer<T>`.
//! 2. Ordinamento per Priorità: I task devono essere serviti in ordine di priorità decrescente.
//! 3. Annullamento e Handle Dinamici: `submit()` deve restituire un `Option<Arc<dyn CancelableHandle>>`.
//! 4. Scarto Silenzioso: `pop_next()` deve scartare i task annullati in modo trasparente senza restituirli.
//! 5. Nessuna Attesa Attiva: L'attesa del consumatore su una coda vuota non deve consumare cicli di CPU.
//! 6. Spegnimento Pulito: Quando tutti i produttori vengono distrutti, `pop_next()` deve restituire `None`.

use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering::*}}};

/// Trait che rappresenta l'handle dinamico di annullamento restituito al momento dell'invio.
pub trait CancelableHandle: Send + Sync {
    /// Tenta di annullare il task accodato prima che venga elaborato dal consumatore.
    fn cancel(&self) -> bool;

    /// Verifica se il task è già stato estratto ed elaborato dal consumatore.
    fn is_done(&self) -> bool;
}

/// Trait che rappresenta il lato produttore del canale prioritario.
pub trait WorkProducer<T: Send>: Clone + Send + Sync {
    /// Sottomette un nuovo task nel canale specificandone il livello di priorità.
    fn submit(&self, task: T, priority: u8) -> Option<Arc<dyn CancelableHandle>>;
}

/// Trait che rappresenta il lato consumatore del canale prioritario.
pub trait WorkConsumer<T: Send>: Clone + Send + Sync {
    /// Estrae il prossimo task a priorità più alta non ancora annullato.
    fn pop_next(&self) -> Option<T>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct Element<T: Send> {
    task: Mutex<Option<T>>,
    priority: u8,
    canceled: AtomicBool,
    received: AtomicBool
}

impl<T: Send> Element<T> {
    pub fn new (_task: T, _priority: u8) -> Self {
        Self {
            task: Mutex::new(Some(_task)),
            priority: _priority,
            canceled: AtomicBool::new(false),
            received: AtomicBool::new(false)
        }
    }
}

impl<T: Send + Sync> CancelableHandle for Element<T> {
    fn cancel(&self) -> bool {
        if !self.received.load(SeqCst) && !self.canceled.swap(true, SeqCst) {
            true
        }
        else {
            false
        }
    }

    fn is_done(&self) -> bool {
        self.received.load(SeqCst)
    }
}

pub struct SharedState<T: Send> {
    queue: VecDeque<Arc<Element<T>>>,
    closed: bool
}
impl<T: Send> SharedState<T> {
    pub fn new () -> Self {
        Self {
            queue: VecDeque::new(),
            closed: false
        }
    }
}


pub struct Consumer<T: Send> {
    receiver: Arc<(Mutex<SharedState<T>>, Condvar)>
}

impl<T: Send> Consumer<T> {
    pub fn new () -> Self {
        Self {
            receiver: Arc::new((Mutex::new(SharedState::new()), Condvar::new()))
        }
    }
}

impl<T: Send> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: Arc::clone(&self.receiver)
        }
    }
}

impl<T: Send + Sync> WorkConsumer<T> for Consumer<T> {
    fn pop_next(&self) -> Option<T> {
        let (mutex, cvar) = &*self.receiver;
        let mut guard = mutex.lock().unwrap();

        loop {
            if let Some(elem) = guard.queue.pop_front() {
                if !elem.canceled.swap(true, SeqCst) {
                    elem.received.store(true, SeqCst);
                    let mut elem_guard = elem.task.lock().unwrap();
                    return elem_guard.take();
                }

                continue;
            }

            

            guard = cvar.wait_while(guard, |c| {
                c.queue.is_empty() && !c.closed
            }).unwrap();

            if guard.queue.is_empty() && guard.closed {
                return None;
            }
        }
    }
}

impl<T: Send> Drop for Consumer<T> {
    fn drop(&mut self) {
        let (mutex, _) = &*self.receiver;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
    }
}



pub struct Producer<T: Send> {
    senders: Arc<(Mutex<SharedState<T>>, Condvar)>,
    num_senders: Arc<AtomicUsize>
}

impl<T: Send> Clone for Producer<T> {
    fn clone(&self) -> Self {
        let new_num_senders = self.num_senders.load(SeqCst) + 1;
        self.num_senders.store(new_num_senders, SeqCst);

        Self {
            senders: Arc::clone(&self.senders),
            num_senders: Arc::clone(&self.num_senders)
        }
    }
}

impl<T: Send + Sync + 'static> WorkProducer<T> for Producer<T> {
    fn submit(&self, task: T, priority: u8) -> Option<Arc<dyn CancelableHandle>> {
        let elem = Arc::new(Element::new(task, priority));

        let (mutex, cvar) = &*self.senders;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            None
        }
        else {
            let mut index: usize = guard.queue.len();
            for (i, e) in guard.queue.clone().iter().enumerate() {
                if e.priority < elem.priority {
                    index = i;
                    break;
                }
            }

            guard.queue.insert(index, elem.clone());
            drop(guard);
            cvar.notify_one();
            Some(elem)
        }
    }
}

impl<T: Send> Drop for Producer<T> {
    fn drop(&mut self) {
        let new_num_senders = self.num_senders.load(SeqCst) - 1;
        self.num_senders.store(new_num_senders, SeqCst);

        if new_num_senders == 0 {
            let (mutex, cvar) = &*self.senders;
            let mut guard = mutex.lock().unwrap();
            guard.closed = true;
            drop(guard);
            cvar.notify_all();
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che crea il canale prioritario condiviso.
pub fn create_channel<T: Send + Sync + 'static>() -> (impl WorkProducer<T>, impl WorkConsumer<T>) {
    let consumer = Consumer::new();
    let producer = Producer {
        senders: Arc::clone(&consumer.receiver),
        num_senders: Arc::new(AtomicUsize::new(1usize))
    };
    (producer, consumer)
}
