//! # Simulazione 039 — PoisonableBarrier
//!
//! Estende `Rendezvous` nella direzione della propagazione di un fallimento attraverso una barriera.
//! Se un partecipante non può completare il proprio contributo per un errore irreversibile, l'intero
//! round viene avvelenato, risvegliando immediatamente chiunque fosse già in attesa con un esito
//! di fallimento (`Poisoned`) invece che con i valori del round.
//!
//! Si scrivano in Rust le strutture che implementano il tratto `PoisonableBarrier<T>` e i relativi tipi.
//!
//! ### Requisiti
//! - Thread-safe, condivisibile tra `participants` thread (`Send + Sync`).
//! - In assenza di `poison()`, il comportamento è quello di un barrier ciclico: un round si completa
//!   quando esattamente `participants` valori sono stati consegnati, e tutti i chiamanti bloccati
//!   ricevono `Complete(valori)` preservando l'ordine di arrivo.
//! - `poison()` risveglia immediatamente tutti i partecipanti già in attesa con `Poisoned`.
//! - Dopo un completamento o un avvelenamento, il round successivo riparte pulito.
//! - Nessuna attesa attiva (`Mutex` + `Condvar`).

use std::sync::{Arc, Condvar, Mutex};

use crate::ArriveResult::{Complete, Poisoned};

/// Esito della conclusione di un round della barriera.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArriveResult<T: Clone> {
    /// Tutti i partecipanti sono arrivati con successo nel round corrente;
    /// contiene l'insieme completo dei valori nell'ordine di arrivo.
    Complete(Vec<T>),
    /// Il round è stato avvelenato prima del proprio completamento.
    Poisoned,
}

/// Tratto per una barriera ciclica avvelenabile.
pub trait PoisonableBarrier<T: Clone + Send>: Send + Sync {
    /// Consegna `value` per il round corrente e blocca il chiamante, senza consumare cicli di CPU,
    /// finché il round non si conclude (per completamento normale o per avvelenamento).
    fn join_with(&self, value: T) -> ArriveResult<T>;

    /// Avvelena immediatamente il round corrente: tutti i partecipanti già in attesa ricevono
    /// `Poisoned` senza ulteriore attesa, e il round successivo inizia pulito.
    fn poison(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci qui le tue strutture private e le relative implementazioni dei tratti.
// =================================================================================

pub struct BarrierState<T: Clone + Send> {
    values: Vec<T>,
    last_outcome: Option<ArriveResult<T>>,
    generation: usize
}

impl<T: Clone + Send> BarrierState<T> {
    pub fn new () -> Self {
        Self {
            values: Vec::new(),
            last_outcome: None,
            generation: 0
        }
    }
}

pub struct MyPoisonableBarrier<T: Clone + Send> {
    inner: Arc<(Mutex<BarrierState<T>>, Condvar)>,
    participants: usize
}

impl<T: Clone + Send> MyPoisonableBarrier<T> {
    pub fn with_num_participants (participants: usize) -> Self {
        Self {
            inner: Arc::new((Mutex::new(BarrierState::new()), Condvar::new())),
            participants
        }
    }
}

impl<T: Send + Clone> PoisonableBarrier<T> for MyPoisonableBarrier<T> {
    fn join_with(&self, value: T) -> ArriveResult<T> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.values.push(value);
        let actual_generation = guard.generation;

        if guard.values.len() == self.participants {
            guard.generation += 1;
            let outcome = guard.values.clone();
            guard.last_outcome = Some(Complete(outcome.clone()));
            guard.values.clear();

            drop(guard);
            cvar.notify_all();
            return Complete(outcome);
        }

        guard = cvar.wait_while(guard, |c| {
            c.generation == actual_generation
        }).unwrap();

        guard.last_outcome.as_ref().unwrap().clone()
    }

    fn poison(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        guard.generation += 1;
        guard.last_outcome = Some(Poisoned);
        guard.values.clear();
        drop(guard);
        cvar.notify_all();
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `PoisonableBarrier` per `participants` concorrenti.
pub fn make_poisonable_barrier<T: Clone + Send + 'static>(
    participants: usize,
) -> impl PoisonableBarrier<T> {
    MyPoisonableBarrier::with_num_participants(participants)
}