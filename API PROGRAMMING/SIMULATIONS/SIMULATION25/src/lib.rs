//! # Simulazione 025 — TransferPipeline (Capstone)
//!
//! Un servizio che copia file in background su richiesta deve gestire fallimenti transitori
//! ritentando automaticamente, verificare l'integrità del risultato prima di dichiararlo completo,
//! e permettere l'annullamento di un trasferimento — ma solo finché non è già entrato nella fase di verifica.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `TransferHandle` e `TransferPipeline` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust
//! #[derive(Debug, PartialEq, Eq, Clone, Copy)]
//! pub enum TransferStatus {
//!     Queued,
//!     Transferring,
//!     Verifying,
//!     Complete,
//!     Failed,
//!     Cancelled,
//! }
//!
//! pub trait TransferHandle: Send + Sync {
//!     fn status(&self) -> TransferStatus;
//!     fn cancel(&self) -> bool;
//!     fn join(&self) -> TransferStatus;
//! }
//!
//! pub trait TransferPipeline: Clone + Send + Sync {
//!     fn submit(
//!         &self,
//!         max_attempts: u32,
//!         do_transfer: impl Fn() -> bool + Send + Sync + 'static,
//!         do_verify: impl Fn() -> bool + Send + Sync + 'static,
//!     ) -> impl TransferHandle + 'static;
//! }
//!
//! pub fn make_transfer_pipeline(worker_count: usize) -> impl TransferPipeline {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Alla creazione, `worker_count` thread di lavoro devono essere avviati.
//! - Transizioni valide: `Queued` → `Transferring` → o `Verifying` (se `do_transfer` ha successo) o `Queued` (se fallisce e restano tentativi) o `Failed` (se tentativi esauriti); `Verifying` → `Complete` (se `do_verify` ha successo) o `Failed` (se fallisce).
//! - Se `cancel()` ha successo mentre lo stato è `Transferring`, il worker deve fermarsi a `Cancelled` non appena `do_transfer` in corso termina.
//! - Se `cancel()` arriva mentre lo stato è `Verifying`, `Complete`, `Failed`, o `Cancelled`, restituisce `false` e non ha effetto.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{sync::{Arc, Condvar, Mutex}, thread};

use crate::TransferStatus::{Cancelled, Complete, Failed, Queued, Transferring, Verifying};

/// Stato osservabile corrente del trasferimento.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TransferStatus {
    Queued,
    Transferring,
    Verifying,
    Complete,
    Failed,
    Cancelled,
}

/// Trait che rappresenta l'handle per il controllo e l'attesa di un singolo trasferimento.
pub trait TransferHandle: Send + Sync {
    /// Restituisce lo stato corrente osservabile del trasferimento.
    fn status(&self) -> TransferStatus;

    /// Tenta di annullare il trasferimento.
    /// Restituisce `true` solo se lo stato al momento della chiamata è `Queued` o `Transferring`.
    /// Se lo stato è già `Verifying` o terminale (`Complete`, `Failed`, `Cancelled`), restituisce `false`.
    fn cancel(&self) -> bool;

    /// Blocca il chiamante senza consumo di cicli di CPU finché il trasferimento non raggiunge
    /// uno stato terminale (`Complete`, `Failed`, `Cancelled`), e lo restituisce.
    fn join(&self) -> TransferStatus;
}

/// Trait che rappresenta la pipeline concorrente di trasferimento con retry e verifica.
pub trait TransferPipeline: Clone + Send + Sync {
    /// Sottomette un nuovo trasferimento gestito dal pool interno di worker thread.
    fn submit(
        &self,
        max_attempts: u32,
        do_transfer: impl Fn() -> bool + Send + Sync + 'static,
        do_verify: impl Fn() -> bool + Send + Sync + 'static,
    ) -> impl TransferHandle + 'static;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================



pub struct HandleState {
    status: Mutex<TransferStatus>,
    cvar: Condvar,
    max_attempts: u32,
    do_transfer: Arc<dyn Fn() -> bool + Send + Sync + 'static>,
    do_verify: Arc<dyn Fn() -> bool + Send + Sync + 'static>
}

pub struct MyHandle {
    inner: Arc<HandleState>,
    shared_pipeline: Arc<(Mutex<PipelineState>, Condvar)>
}

impl MyHandle {
    pub fn do_i_am_queued (&self) -> bool {
        let handle_state = &*self.inner;
        let state_guard = handle_state.status.lock().unwrap();
        matches!(*state_guard, Queued)
    }
}

impl Clone for MyHandle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            shared_pipeline: self.shared_pipeline.clone()
        }
    }
}

impl TransferHandle for MyHandle {
    fn status(&self) -> TransferStatus {
        let handle_state = &*self.inner;
        let state_guard = handle_state.status.lock().unwrap();
        *state_guard
    }

    fn cancel(&self) -> bool {
        let handle_state = &*self.inner;
        let mut state_guard = handle_state.status.lock().unwrap();
        
        if matches!(*state_guard, Queued) || matches!(*state_guard, Transferring) {
            *state_guard = Cancelled;
            drop(state_guard);
            handle_state.cvar.notify_all();
            self.shared_pipeline.1.notify_all();
            true
        }
        else {
            false
        }
    }

    fn join(&self) -> TransferStatus {
        let handle_state = &*self.inner;
        let mut state_guard = handle_state.status.lock().unwrap();

        state_guard = handle_state.cvar.wait_while(state_guard, |c| {
            matches!(*c, Queued) || matches!(*c, Transferring) || matches!(*c, Verifying)
        }).unwrap();
        *state_guard
    }
}

pub struct PipelineState {
    handles: Vec<MyHandle>,
    closed: bool
}

impl PipelineState {
    pub fn new () -> Self {
        Self {
            handles: Vec::new(),
            closed: false
        }
    }
}

pub struct MyTransferPipeline {
    pipeline: Arc<(Mutex<PipelineState>, Condvar)>
}

impl MyTransferPipeline {
    pub fn new (_worker_count: usize) -> Self {
        let pipeline = MyTransferPipeline {
            pipeline: Arc::new((Mutex::new(PipelineState::new()), Condvar::new()))
        };
        
        for _ in 0.._worker_count {
            let cloned_pipeline = pipeline.clone();

            let _ = thread::spawn(move || {
                loop {
                    let (thread_mutex, thread_cvar) = &*cloned_pipeline.pipeline;
                    let mut thread_guard = thread_mutex.lock().unwrap();

                    thread_guard = thread_cvar.wait_while(thread_guard, |c| {
                        !(c.closed || c.handles.iter().any(|handle| {
                            handle.do_i_am_queued()
                        }))
                    }).unwrap();

                    if thread_guard.handles.is_empty() && thread_guard.closed {
                        thread_guard.handles.clear();
                        break;
                    }

                    if let Some(handle) = thread_guard.handles.iter_mut().find(|handle| {
                        handle.do_i_am_queued()
                    }) {
                        let handle_state = &*handle.inner;

                        let mut state_guard = handle_state.status.lock().unwrap();
                        if !matches!(*state_guard, Cancelled) {
                            *state_guard = Transferring;
                            drop(state_guard);

                            let max_attempts = handle_state.max_attempts;
                            let do_transfer = handle_state.do_transfer.clone();
                            let do_verify = handle_state.do_verify.clone();

                            let mut transfererd = false;
                            let mut verified = false;
                            let mut cancelled = false;
                            for _ in 0..max_attempts {
                                transfererd = do_transfer();

                                let mut state_guard = handle_state.status.lock().unwrap();
                                if matches!(*state_guard, Cancelled) {
                                    cancelled = true;
                                    break;
                                }

                                if transfererd {
                                    *state_guard = Verifying;
                                    drop(state_guard);
                                    verified = do_verify();
                                    break; 
                                }
                            }

                            if !cancelled {
                                let mut state_guard = handle_state.status.lock().unwrap();
                                *state_guard = if verified && transfererd { Complete } else { Failed };
                                drop(state_guard);
                                handle_state.cvar.notify_all();
                            }
                        }
                    }


                    thread_guard.handles.retain(|handle| {
                        let status_guard = handle.inner.status.lock().unwrap();
                        !(matches!(*status_guard, Complete) || matches!(*status_guard, Failed) || matches!(*status_guard, Cancelled))
                    });
                }
            });
        }

        Self {
            pipeline: pipeline.pipeline.clone()
        }
    }
}

impl TransferPipeline for MyTransferPipeline {
    fn submit(
        &self,
        max_attempts: u32,
        do_transfer: impl Fn() -> bool + Send + Sync + 'static,
        do_verify: impl Fn() -> bool + Send + Sync + 'static,
    ) -> impl TransferHandle + 'static
    {
        let new_handle_state = HandleState {
            status: Mutex::new(Queued),
            cvar: Condvar::new(),
            max_attempts,
            do_transfer: Arc::new(do_transfer),
            do_verify: Arc::new(do_verify)
        };

        let new_handle = MyHandle {
            inner:  Arc::new(new_handle_state),
            shared_pipeline: self.pipeline.clone()
        };

        let (mutex, thread_cvar) = &*self.pipeline;
        let mut guard = mutex.lock().unwrap();
        guard.handles.push(new_handle.clone());
        drop(guard);
        thread_cvar.notify_one();
        new_handle
    }
}

impl Clone for MyTransferPipeline {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone()
        }
    }
}

impl Drop for MyTransferPipeline {
    fn drop(&mut self) {
        if Arc::strong_count(&self.pipeline) == 1 {
            let (mutex, thread_cvar) = &*self.pipeline;
            let mut guard = mutex.lock().unwrap();
            guard.closed = true;
            drop(guard);
            thread_cvar.notify_all();
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `TransferPipeline` avviando `worker_count` thread di lavoro.
pub fn make_transfer_pipeline(_worker_count: usize) -> impl TransferPipeline {
    MyTransferPipeline::new(_worker_count)
}   
