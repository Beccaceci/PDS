//! # Implementazione 2: TaskExecutor basato su Canali MPSC & JoinHandle (`mpsc_executor.rs`)
//!
//! Come studiato a lezione, i canali della libreria standard (`std::sync::mpsc`)
//! semplificano notevolmente l'architettura di un single-thread worker:
//!
//! ## Vantaggi Architetturali:
//! 1. **Accodamento FIFO Nativo**: `mpsc::channel::<Task>()` fornisce nativamente una coda unbounded FIFO thread-safe.
//! 2. **Iterazione Naturale sul Worker Thread**: Il worker esegue semplicemente `for task in rx { task(); }`.
//!    Quando il trasmettitore `tx` viene distrutto (tramite `close()`), il ciclo `for` termina automaticamente
//!    dopo aver drenato ed eseguito tutti i task in coda.
//! 3. **Gestione di `join()` Concorrente**:
//!    I chiamanti di `join()` sincronizzano la terminazione su una variabile di condizione `done`,
//!    assicurando che `join()` possa essere chiamato da più thread contemporaneamente in modo sicuro.

use super::TaskExecutor;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;

type Task = Box<dyn FnOnce() + Send + 'static>;

/// Implementazione concreta basata su `std::sync::mpsc::channel` e `thread::JoinHandle`.
pub struct MpscTaskExecutor {
    /// Trasmettitore del canale protetto da Mutex (rimosso con `.take()` su `close()`).
    sender: Mutex<Option<mpsc::Sender<Task>>>,
    /// Stato di completamento condiviso per consentire a più chiamanti di invocare `join()` concorrentemente.
    done: Arc<(Mutex<bool>, Condvar)>,
    /// Handle del worker thread.
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl MpscTaskExecutor {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<Task>();
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let done_clone = Arc::clone(&done);

        let handle = thread::spawn(move || {
            // Estrae ed esegue i task in ordine FIFO.
            // Il ciclo termina automaticamente quando tx viene distrutto e tutti i task sono stati drenati!
            for task in rx {
                task();
            }

            // Segnala a tutti i chiamanti di join() che il thread ha terminato
            let (lock, cvar) = &*done_clone;
            let mut guard = lock.lock().unwrap();
            *guard = true;
            drop(guard);
            cvar.notify_all();
        });

        Self {
            sender: Mutex::new(Some(tx)),
            done,
            handle: Mutex::new(Some(handle)),
        }
    }
}

impl TaskExecutor for MpscTaskExecutor {
    fn submit<F: FnOnce() + Send + 'static>(&self, task: F) -> bool {
        let guard = self.sender.lock().unwrap();
        if let Some(ref tx) = *guard {
            // Invia il task sul canale. Restituisce true se accodato con successo.
            tx.send(Box::new(task)).is_ok()
        } else {
            // Executor già chiuso: submit rifiutata
            false
        }
    }

    fn close(&self) {
        let mut guard = self.sender.lock().unwrap();
        // Distruggere il trasmettitore Sender chiude il canale e notifica EOF al ciclo `for task in rx`
        let _ = guard.take();
    }

    fn join(&self) {
        // 1. Attende che il flag di completamento diventi true
        let (lock, cvar) = &*self.done;
        let guard = lock.lock().unwrap();
        drop(cvar.wait_while(guard, |terminated| !*terminated).unwrap());

        // 2. Se l'handle è ancora presente, effettua il join finale del thread
        let mut handle_guard = self.handle.lock().unwrap();
        if let Some(h) = handle_guard.take() {
            let _ = h.join();
        }
    }
}

/// Costruttore per il TaskExecutor basato su canali MPSC.
pub fn make_mpsc_task_executor() -> impl TaskExecutor {
    MpscTaskExecutor::new()
}
