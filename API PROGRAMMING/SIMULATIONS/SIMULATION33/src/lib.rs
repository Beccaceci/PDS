//! # Simulazione 033 — WorkStealingPool
//!
//! In uno scheduler a più worker, far condividere a tutti un'unica coda centrale crea un collo di bottiglia:
//! ogni worker deve contendersi lo stesso lock anche per operazioni che non hanno nulla a che fare con gli altri.
//! Il work-stealing risolve il problema dando a ciascun worker una coda propria, su cui lavora liberamente senza contesa;
//! solo quando la propria coda è vuota, un worker tenta di "rubare" un task dalla coda di un altro — dall'estremità
//! opposta rispetto a quella che il proprietario usa, per motivi di cache locality e per ridurre la frequenza dei furti.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `WorkQueue<T>` e `WorkStealingPool<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait WorkQueue<T: Send> {
//!     fn push(&self, task: T);
//!     fn pop(&self) -> Option<T>;
//!     fn steal(&self) -> Option<T>;
//! }
//!
//! pub trait WorkStealingPool<T: Send>: Clone + Send + Sync {
//!     fn worker_count(&self) -> usize;
//!     fn queue(&self, worker_id: usize) -> impl WorkQueue<T> + 'static;
//!     fn steal_any(&self, own_id: usize) -> Option<(usize, T)>;
//!     fn wait_for_work(&self);
//! }
//!
//! pub fn make_work_stealing_pool<T: Send + 'static>(worker_count: usize) -> impl WorkStealingPool<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Ogni coda è indipendente dalle altre: operare su una non deve mai bloccare l'accesso a una diversa.
//! - Se una coda contiene esattamente un task e il proprietario chiama `pop()` mentre un altro worker chiama `steal()` su di essa concorrentemente, esattamente una delle due chiamate deve ottenere il task, l'altra deve ricevere `None` — mai entrambe lo stesso task, mai il task perso. Lo stesso vale per due `steal()` concorrenti da worker diversi sulla stessa coda con un solo task disponibile.
//! - `steal_any` deve variare l'ordine di tentativo tra chiamate successive.
//! - `wait_for_work` deve sbloccarsi per un `push()` su una qualunque coda, non solo quella del chiamante.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, Ordering::SeqCst}}};

/// Trait che rappresenta una coda a due estremità per un singolo worker:
/// - Il proprietario opera in modalità LIFO (estremità "bottom": `push` e `pop`).
/// - I ladri (altri worker) operano in modalità FIFO (estremità "top": `steal`).
pub trait WorkQueue<T: Send>: Send + Sync {
    /// Il proprietario accoda un task alla propria estremità locale.
    fn push(&self, task: T);

    /// Il proprietario preleva il task più recente dalla propria estremità locale (LIFO).
    /// Restituisce `None` se la coda è vuota.
    fn pop(&self) -> Option<T>;

    /// Un altro worker tenta di rubare il task meno recente da questa coda (FIFO).
    /// Restituisce `None` se la coda è vuota.
    fn steal(&self) -> Option<T>;
}

/// Trait che rappresenta il pool di code con work-stealing distribuito.
pub trait WorkStealingPool<T: Send>: Clone + Send + Sync {
    /// Restituisce il numero totale di worker nel pool.
    fn worker_count(&self) -> usize;

    /// Restituisce l'interfaccia alla coda locale del worker `worker_id` (0..worker_count()).
    fn queue(&self, worker_id: usize) -> impl WorkQueue<T> + 'static;

    /// Tenta di rubare un task da una qualunque coda diversa da `own_id` che ne abbia disponibile.
    /// Restituisce `Some((victim_id, task))` oppure `None`.
    /// L'ordine dei tentativi varia tra chiamate successive.
    fn steal_any(&self, own_id: usize) -> Option<(usize, T)>;

    /// Blocca il chiamante finché non viene eseguito un `push()` su una qualunque coda del pool.
    fn wait_for_work(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyQueue<T: Send> {
    tasks: Arc<Mutex<VecDeque<T>>>,
    shared_signal: Arc<(Mutex<u64>, Condvar)>
}

impl<T: Send> WorkQueue<T> for MyQueue<T> {
    fn push(&self, task: T) {
        let mut guard = self.tasks.lock().unwrap();
        guard.push_back(task);
        drop(guard);

        let (mutex, cvar) = &*self.shared_signal;
        let mut guard_signal = mutex.lock().unwrap();
        *guard_signal += 1;
        drop(guard_signal);
        cvar.notify_one();
    }

    fn pop(&self) -> Option<T> {
        let mut guard = self.tasks.lock().unwrap();
        guard.pop_back()
    }

    fn steal(&self) -> Option<T> {
        let mut guard = self.tasks.lock().unwrap();
        guard.pop_front()
    }
}

pub struct MyWorkPool<T: Send> {
    queues: Arc<Vec<MyQueue<T>>>,
    signal: Arc<(Mutex<u64>, Condvar)>,
    worker_count: usize,
    forward: AtomicBool
}

impl<T: Send> MyWorkPool<T> {
    pub fn with_worker_count (_worker_count: usize) -> Self {
        let signal = Arc::new((Mutex::new(0), Condvar::new()));
        let mut vec = Vec::new();
        for _ in 0.._worker_count {
            vec.push(MyQueue {
                tasks: Arc::new(Mutex::new(VecDeque::new())),
                shared_signal: signal.clone()
            });
        }
        let pool = Arc::new(vec);

        Self {
            queues: pool,
            signal,
            worker_count: _worker_count,
            forward: AtomicBool::new(true)
        }
    } 
}

impl<T: Send + 'static> WorkStealingPool<T> for MyWorkPool<T> {
    fn queue(&self, worker_id: usize) -> impl WorkQueue<T> + 'static {
        MyQueue {
            tasks: self.queues[worker_id].tasks.clone(),
            shared_signal: self.signal.clone()
        }
    }

    fn steal_any(&self, own_id: usize) -> Option<(usize, T)> {
        let forward = self.forward.fetch_not(SeqCst);
        let mut queue_id = own_id;

        loop {
            queue_id = if forward {
                if queue_id == self.worker_count - 1 { 0 } else { queue_id + 1 }   
            }
            else {
                if queue_id == 0 { self.worker_count - 1 } else { queue_id - 1 }
            };

            if queue_id == own_id {
                return None;
            }

            if let Some(task) = self.queues[queue_id].steal() {
                return Some((queue_id, task));
            }
        }
    }

    fn wait_for_work(&self) {
        let (mutex, cvar) = &*self.signal;
        let mut guard = mutex.lock().unwrap();

        let initial_count = *guard;
        guard = cvar.wait_while(guard, |c| {
            *c == initial_count
        }).unwrap();
    }

    fn worker_count(&self) -> usize {
        self.worker_count
    }
}

impl<T: Send> Clone for MyWorkPool<T> {
    fn clone(&self) -> Self {
        Self {
            queues: self.queues.clone(),
            signal: self.signal.clone(),
            worker_count: self.worker_count,
            forward: AtomicBool::new(self.forward.load(SeqCst))
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `WorkStealingPool`.
pub fn make_work_stealing_pool<T: Send + 'static>(
    _worker_count: usize,
) -> impl WorkStealingPool<T> {
    MyWorkPool::with_worker_count(_worker_count)
}
