//! # Simulazione 049 — CancelScope (capstone)
//!
//! Albero gerarchico di ambiti di cancellazione (stile `context.Context` / `CancellationToken`)[cite: 16].
//! La cancellazione di un ambito propaga il segnale a tutti i discendenti vivi lungo l'albero[cite: 16].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile (`Clone` condivide lo stesso ambito)[cite: 16].
//! - `is_cancelled()` restituisce `true` se questo ambito o un suo antenato è stato cancellato[cite: 16].
//! - `cancel()` cancella l'ambito e tutti i discendenti vivi[cite: 16].
//! - I figli futuri generati da un ambito già cancellato nascono già cancellati[cite: 16].
//! - I riferimenti verso i figli sono deboli (`Weak`): la distruzione di un figlio non è impedita
//!   dalla permanenza in vita del genitore[cite: 16].
//! - `cancel()` non mantiene mai più di un lock contemporaneamente durante la discesa dell'albero[cite: 16].
//! - `is_cancelled()` non blocca e legge solo lo stato locale dell'ambito[cite: 16].

use std::sync::{Arc, Mutex, Weak };

/// Tratto per un ambito di cancellazione gerarchico[cite: 16].
pub trait CancelScope: Clone + Send + Sync {
    /// Restituisce `true` se questo ambito, o un qualunque suo antenato, è stato cancellato[cite: 16].
    fn is_cancelled(&self) -> bool;

    /// Cancella questo ambito e, ricorsivamente, tutti i suoi discendenti vivi[cite: 16].
    fn cancel(&self);

    /// Crea un nuovo ambito figlio[cite: 16].
    /// Se questo ambito è già cancellato, il figlio nasce già cancellato[cite: 16].
    fn child(&self) -> impl CancelScope;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct ScopeState {
    cancelled: bool,
    sons: Vec<Weak<Mutex<ScopeState>>>
}

pub struct MyCancelScope {
    inner: Arc<Mutex<ScopeState>>
}

impl MyCancelScope {
    pub fn new () -> Self {
        Self {
            inner: Arc::new(Mutex::new(ScopeState {
                cancelled: false,
                sons: Vec::new()
            }))
        }
    }
}

impl CancelScope for MyCancelScope {
    fn is_cancelled(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.cancelled
    }

    fn cancel(&self) {
        let living_children: Vec<Arc<Mutex<ScopeState>>> = {
            let mut guard = self.inner.lock().unwrap();
            if guard.cancelled == true {
                return;
            }
            else {
                guard.cancelled = true;
            }

            let mut living = Vec::new();
            guard.sons.retain(|weak_child| {
                if let Some(strong_child) = weak_child.upgrade() {
                    living.push(strong_child);
                    true
                }
                else {
                    false
                }
            });

            living
        };
        
        
        for child in living_children {
            let child_scope = MyCancelScope { inner: child };
            child_scope.cancel();
        }
    }

    fn child(&self) -> impl CancelScope {
        let mut guard = self.inner.lock().unwrap();

        let new_cancel_scope = Arc::new(Mutex::new(ScopeState {
                cancelled: guard.cancelled,
                sons: Vec::new()
            }));

        guard.sons.push(Arc::downgrade(&new_cancel_scope));

        MyCancelScope {
            inner: new_cancel_scope.clone()
        }
    }
}

impl Clone for MyCancelScope {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce la radice di un nuovo albero di `CancelScope`[cite: 16].
pub fn make_root_scope() -> impl CancelScope {
    MyCancelScope::new()
}