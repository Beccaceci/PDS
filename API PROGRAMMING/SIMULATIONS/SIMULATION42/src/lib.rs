//! # Simulazione 048 — TransactionalPair (capstone)
//!
//! Mini-memoria transazionale software (STM) applicata a una coppia di variabili condivise[cite: 15].
//! Fornisce lettura atomica non bloccante (`read_both`) e applicazione di transazioni
//! ottimistiche concorrenti con ritentativo non attivo (`atomically`)[cite: 15].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile tra thread (`Clone + Send + Sync`)[cite: 15].
//! - `read_both` restituisce una fotografia coerente e atomica dei due valori senza mai bloccare[cite: 15].
//! - `atomically` esegue la chiusura `transaction` fuori da ogni lock[cite: 15].
//! - In caso di conflitto (valori modificati da una scrittura concorrente nel frattempo), il tentativo
//!   viene scartato e il thread si blocca senza consumo di CPU (`Condvar`) finché le versioni
//!   non cambiano effettivamente rispetto a quelle osservate prima di ritentare[cite: 15].
//! - Nessuna attesa attiva / busy-looping[cite: 15].

use std::sync::{Arc, Condvar, Mutex};

/// Tratto per una coppia di valori protetta da semantica transazionale ottimistica[cite: 15].
pub trait TransactionalPair<V: Clone + Send>: Clone + Send + Sync {
    /// Legge i valori correnti della coppia atomicamente insieme senza mai bloccare[cite: 15].
    fn read_both(&self) -> (V, V);

    /// Applica ottimisticamente una transazione alla coppia di valori[cite: 15].
    /// In caso di conflitti concorrenti, ritenta automaticamente bloccando il chiamante
    /// finché i valori non vengono modificati da un altro commit[cite: 15].
    fn atomically(&self, transaction: impl Fn(V, V) -> (V, V)) -> (V, V);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct PairState<V: Clone + Send> {
    value1: V,
    value2: V,
    generation: usize
}

pub struct MyTransactionalPair<V: Clone + Send> {
    inner: Arc<(Mutex<PairState<V>>, Condvar)>
}

impl<V: Clone + Send> TransactionalPair<V> for MyTransactionalPair<V> {
    fn read_both(&self) -> (V, V) {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        (guard.value1.clone(), guard.value2.clone())
    }

    fn atomically(&self, transaction: impl Fn(V, V) -> (V, V)) -> (V, V) {
        loop {
            let (input_value1, input_value2) = self.read_both();

            let generation = self.get_generation();
            let (computed_value1, computed_value2) = transaction(input_value1, input_value2);

            let (mutex, _) = &*self.inner;
            let mut guard = mutex.lock().unwrap();
            if generation == guard.generation {
                guard.value1 = computed_value1.clone();
                guard.value2 = computed_value2.clone();
                guard.generation += 1;
                return (computed_value1, computed_value2)
            }
        }
    }
}

impl<V: Clone + Send> MyTransactionalPair<V> {
    pub fn get_generation (&self) -> usize {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.generation
    }
}

impl<V: Clone + Send> Clone for MyTransactionalPair<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `TransactionalPair`[cite: 15].
pub fn make_transactional_pair<V: Clone + Send + 'static>(
    _first: V,
    _second: V,
) -> impl TransactionalPair<V> {
    MyTransactionalPair {
        inner: Arc::new((Mutex::new(PairState {
            value1: _first,
            value2: _second,
            generation: 0
        }), Condvar::new()))
    }
}