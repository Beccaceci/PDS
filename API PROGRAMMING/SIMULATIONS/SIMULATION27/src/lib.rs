//! # Simulazione 027 — SagaOrchestrator (Capstone)
//!
//! Una sequenza di operazioni su sistemi diversi (es. riservare inventario, addebitare un pagamento,
//! pianificare una spedizione) non può essere resa atomica con un lock distribuito se i sistemi non lo supportano —
//! ma se un passo a metà sequenza fallisce o va in timeout, gli effetti dei passi già riusciti vanno annullati
//! esplicitamente (`compensate()`), in ordine **STRETTAMENTE INVERSO** rispetto a quello con cui sono stati applicati.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Step` e `SagaOrchestrator` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust
//! use std::time::Duration;
//!
//! pub trait Step: Send {
//!     fn execute(&self) -> bool;
//!     fn compensate(&self);
//! }
//!
//! pub trait SagaOrchestrator: Clone + Send + Sync {
//!     fn run_saga(&self, steps: Vec<Box<dyn Step>>, step_timeout: Duration) -> bool;
//! }
//!
//! pub fn make_saga_orchestrator() -> impl SagaOrchestrator {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - I passi vanno eseguiti in ordine sequenziale, uno alla volta.
//! - Ciascun passo ha al più `step_timeout` per completare `execute()`.
//! - Se un passo fallisce (restituisce `false` o scade il timeout), l'esecuzione si interrompe e viene invocato `compensate()` su tutti i passi precedenti riusciti in ordine STRETTAMENTE INVERSO.
//! - `compensate()` va invocato esclusivamente sui passi il cui `execute()` ha già avuto successo prima del fallimento.
//! - Un passo che supera `step_timeout` continua sullo sfondo senza bloccare l'avanzamento della compensazione.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`), isolamento completo tra saghe concorrenti.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{sync::{Arc, Condvar, Mutex}, thread, time::Duration};

/// Trait che rappresenta un singolo passo di una Saga con azione diretta e compensazione.
pub trait Step: Send {
    /// Esegue il passo. Restituisce `true` in caso di successo, `false` in caso di fallimento.
    fn execute(&self) -> bool;

    /// Annulla gli effetti di questo passo. Invocato al più una volta, e solo se `execute()` aveva già avuto successo.
    fn compensate(&self);
}

/// Trait che rappresenta l'orchestratore di saghe distribuite con compensazione all'indietro.
pub trait SagaOrchestrator: Clone + Send + Sync {
    /// Esegue sequenzialmente i passi forniti applicando un timeout per ciascun passo.
    fn run_saga(&self, steps: Vec<Box<dyn Step>>, step_timeout: Duration) -> bool;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyWrapperState {
    steps_state: Vec<Arc<Mutex<Box<dyn Step>>>>
}

impl MyWrapperState {
    pub fn with_steps (_steps: Vec<Box<dyn Step>>) -> Self {
        Self {
            steps_state: _steps
                        .into_iter()
                        .map(|s| Arc::new(Mutex::new(s)))
                        .collect()
        }
    }
}

#[derive(Clone)]
pub struct TxState {
    inner: Arc<(Mutex<Vec<Option<bool>>>, Condvar)>
}

impl TxState {
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(Vec::new()), Condvar::new()))
        }
    }

    pub fn with_num_steps (_num_steps: usize) -> Self {
        Self {
            inner: Arc::new((Mutex::new(vec![None; _num_steps]), Condvar::new()))
        }
    }
}

#[derive(Clone)]
pub struct MyOrchestrator;

impl SagaOrchestrator for MyOrchestrator {
    fn run_saga(&self, steps: Vec<Box<dyn Step>>, step_timeout: Duration) -> bool {
        let num_steps = steps.len();
        let tx_state = TxState::with_num_steps(num_steps);
        let wrapper = MyWrapperState::with_steps(steps);

        let (mutex, cvar) = &*tx_state.inner;
        
        let mut option_index = None;
        for i in 0..num_steps {
            let cloned_wrapper = Arc::clone(&wrapper.steps_state[i]);
            let cloned_state = Arc::clone(&tx_state.inner);

            thread::spawn(move || {
                let result = cloned_wrapper.lock().unwrap().execute();
                let (mutex, cvar) = &*cloned_state;
                let mut guard = mutex.lock().unwrap();
                guard[i] = Some(result);
                drop(guard);
                cvar.notify_one();
            });

            let mut guard = mutex.lock().unwrap();
            let timeout_result;
            (guard, timeout_result) = cvar.wait_timeout_while(guard, step_timeout, |c| {
                c[i].is_none()
            }).unwrap();

            if !timeout_result.timed_out() {
                if Some(true) == guard[i] {
                    continue;
                }
            }

            option_index = Some(i);
            break;
        }

        if let Some(index) = option_index {
            for i in (0..index).rev() {
                wrapper.steps_state[i].lock().unwrap().compensate();
            }
            false
        }
        else {
            true
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `SagaOrchestrator`.
pub fn make_saga_orchestrator() -> impl SagaOrchestrator {
    MyOrchestrator
}
