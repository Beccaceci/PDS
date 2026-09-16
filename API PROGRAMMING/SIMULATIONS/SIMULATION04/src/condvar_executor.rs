//! # Implementazione 1: TaskExecutor con Condvar & Mutex (`condvar_executor.rs`)
//!
//! Questa implementazione utilizza le primitive di sincronizzazione a basso livello (`Mutex` + `Condvar`):
//! - **`TaskManagerState`**: Contiene la coda dei task `VecDeque<Task>`, il flag `closed` e il flag `terminated`.
//! - **Worker Thread Dedicato**:
//!   Estrae i task in ordine FIFO ed esegue `task()`. Quando la coda è vuota e `closed == true`,
//!   imposta `terminated = true` e risveglia tutti i thread in attesa su `join()`.
//! - **Concorrenza Multi-Thread**:
//!   `submit()`, `close()` e `join()` sono liberamente invocabili da più thread contemporaneamente.

use super::TaskExecutor;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

type Task = Box<dyn FnOnce() + Send + 'static>;

/// Stato condiviso dell'Executor protetto da Mutex.
struct TaskManagerState {
    queue: VecDeque<Task>,
    closed: bool,
    terminated: bool,
}

/// Implementazione concreta basata su Mutex e due Condvar (`cvar_work` e `cvar_done`).
pub struct CondvarTaskExecutor {
    state: Arc<(Mutex<TaskManagerState>, Condvar, Condvar)>,
}

impl CondvarTaskExecutor {
    pub fn new() -> Self {
        let state = Arc::new((
            Mutex::new(TaskManagerState {
                queue: VecDeque::new(),
                closed: false,
                terminated: false,
            }),
            Condvar::new(), // Notifica al worker che ci sono nuovi task o close()
            Condvar::new(), // Notifica ai chiamanti di join() che il worker è terminato
        ));

        let state_clone = Arc::clone(&state);

        thread::spawn(move || {
            let (mutex, cvar_work, cvar_done) = &*state_clone;

            loop {
                let mut guard = mutex.lock().unwrap();

                if let Some(task) = guard.queue.pop_front() {
                    // Rilasciamo il mutex durante l'esecuzione del task per consentire submit concorrenti
                    drop(guard);
                    task();
                } else {
                    if guard.closed {
                        // Coda vuota e close() chiamato: il worker thread termina
                        guard.terminated = true;
                        drop(guard);
                        cvar_done.notify_all();
                        break;
                    } else {
                        // Coda vuota ma executor ancora aperto: sospensione efficiente senza consumo CPU
                        guard = cvar_work
                            .wait_while(guard, |s| s.queue.is_empty() && !s.closed)
                            .unwrap();
                    }
                }
            }
        });

        Self { state }
    }
}

impl TaskExecutor for CondvarTaskExecutor {
    fn submit<F: FnOnce() + Send + 'static>(&self, task: F) -> bool {
        let (mutex, cvar_work, _) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            false
        } else {
            guard.queue.push_back(Box::new(task));
            drop(guard);
            cvar_work.notify_one();
            true
        }
    }

    fn close(&self) {
        let (mutex, cvar_work, _) = &*self.state;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        cvar_work.notify_all();
    }

    fn join(&self) {
        let (mutex, _, cvar_done) = &*self.state;
        let guard = mutex.lock().unwrap();

        // Attesa bloccante finché il worker thread non imposta terminated = true
        drop(
            cvar_done
                .wait_while(guard, |s| !s.terminated)
                .unwrap(),
        );
    }
}

impl Clone for CondvarTaskExecutor {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

/// Costruttore per il TaskExecutor basato su Condvar e Mutex.
pub fn make_condvar_task_executor() -> impl TaskExecutor {
    CondvarTaskExecutor::new()
}
