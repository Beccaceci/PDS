//! # Simulazione 035 — Elezione del leader (capstone)
//!
//! In un cluster di nodi paritari, esattamente un nodo alla volta deve potersi considerare "leader" per un dato termine —
//! un contatore logico condiviso, incrementato ogni volta che un nodo tenta una nuova elezione.
//! Un nodo diventa leader solo se ottiene il voto di una maggioranza stretta degli altri nodi per quel termine;
//! ogni nodo concede al più un voto per termine, indipendentemente da chi lo richiede, e riconosce automaticamente
//! un termine più recente del proprio osservato in qualunque messaggio ricevuto, rinunciando a qualunque pretesa di leadership per termini ormai superati.
//!
//! Si scriva in Rust una struttura che implementi il tratto `Node` definito di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum VoteResult {
//!     Granted,
//!     Denied,
//! }
//!
//! pub trait Node: Send + Sync {
//!     fn current_term(&self) -> u64;
//!     fn is_leader(&self) -> bool;
//!     fn request_vote(&self, term: u64) -> VoteResult;
//!     fn receive_heartbeat(&self, term: u64);
//!     fn start_election(&self, others: &[&dyn Node]) -> bool;
//! }
//!
//! pub fn make_node() -> impl Node {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Lo stato di ciascun nodo (termine corrente, se ha già votato in questo termine, se si considera leader) è indipendente da quello di ogni altro nodo.
//! - `request_vote` e `receive_heartbeat` devono poter essere chiamati concorrentemente sullo stesso nodo da più chiamanti, con un esito coerente con un qualche ordine di serializzazione effettivo.
//! - `start_election` deve contattare tutti gli `others` in parallelo (un thread per nodo), non in sequenza.
//! - Il ritorno anticipato — sia per maggioranza raggiunta sia per maggioranza ormai impossibile — è obbligatorio quando applicabile.
//! - Risposte che arrivano dopo che `start_election` ha già deciso e restituito il controllo non devono alterare l'esito già determinato, né causare panico.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{sync::{Arc, Condvar, Mutex}, thread};

use crate::{ElectionResult::{Lose, Win}, VoteResult::{Denied, Granted}};

/// Risultato di una richiesta di voto da parte di un candidato.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    /// Voto concesso per il termine richiesto.
    Granted,
    /// Voto negato (termine inferiore o voto già espresso per questo termine).
    Denied,
}

/// Trait che rappresenta un nodo distribuito conforme al protocollo di elezione leader in stile Raft.
pub trait Node: Send + Sync {
    /// Restituisce il termine corrente conosciuto da questo nodo.
    fn current_term(&self) -> u64;

    /// Restituisce `true` se questo nodo si considera attualmente leader per il proprio termine corrente.
    fn is_leader(&self) -> bool;

    /// Un candidato richiede il voto per `term` a questo nodo (il votante).
    ///
    /// - Se `term > current_term`: aggiorna il proprio termine a `term`, dimentica voti precedenti, concede il voto e restituisce `Granted`.
    /// - Se `term == current_term` e il votante non ha ancora votato in questo termine: concede e restituisce `Granted`.
    /// - Altrimenti (`term < current_term` o già votato per questo termine): restituisce `Denied`.
    fn request_vote(&self, term: u64) -> VoteResult;

    /// Un leader per `term` segnala il proprio battito cardiaco (*heartbeat*) a questo nodo.
    ///
    /// - Se `term >= current_term`: aggiorna il proprio termine (se maggiore) e rinuncia a qualunque pretesa di leadership.
    /// - Se `term < current_term`: nessun effetto.
    fn receive_heartbeat(&self, term: u64);

    /// Questo nodo tenta di diventare leader per un nuovo termine (`current_term() + 1`),
    /// richiedendo il voto a ciascuno dei nodi in `others` in parallelo (contando anche il proprio voto implicito a se stesso).
    ///
    /// Termina non appena raggiunta una maggioranza stretta dei voti totali (`others.len() + 1`) diventando leader e restituendo `true`,
    /// oppure non appena una maggioranza diventa matematicamente impossibile restituendo `false`,
    /// senza necessariamente attendere tutte le risposte in entrambi i casi.
    fn start_election(&self, others: &[&dyn Node]) -> bool;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub enum ElectionResult {
    Win,
    Lose
}

pub struct ElectionState {
    num_nodes: usize,
    num_granted: usize,
    num_denied: usize
}

impl ElectionState {
    pub fn election_result (&self) -> Option<ElectionResult> {
        let quota = (self.num_nodes/2) + 1;

        if self.num_granted >= quota {
            return Some(Win);
        }
        else if self.num_denied > self.num_nodes - quota {
            return Some(Lose);
        }
        else {
            None
        }
    }
}

pub struct NodeState {
    term: u64,
    leader: bool,
    voted: bool
}

pub struct MyNode {
    inner: Arc<Mutex<NodeState>>
}

impl MyNode {
    pub fn new () -> Self {
        Self {
            inner: Arc::new(Mutex::new(NodeState {
                term: 0,
                leader: false,
                voted: false
            }))
        }
    }
}

impl Node for MyNode {
    fn current_term(&self) -> u64 {
        let mutex = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.term
    }

    fn is_leader(&self) -> bool {
        let mutex = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.leader
    }

    fn request_vote(&self, term: u64) -> VoteResult {
        let mutex = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if term > guard.term  {
            guard.term = term;
            guard.voted = true;
            guard.leader = false;
            Granted
        }
        else if term == guard.term && !guard.voted {
            guard.voted = true;
            guard.leader = false;
            Granted
        }
        else {
            Denied
        }
    }

    fn receive_heartbeat(&self, term: u64) {
        let mutex = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if term >= guard.term  {
            if term > guard.term {
                guard.term = term;
                guard.voted = false;
            }
            guard.leader = false;
        }
    }

    fn start_election(&self, others: &[&dyn Node]) -> bool {
        let mutex = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        let election_state = Arc::new((Mutex::new(ElectionState {
            num_nodes: others.len() + 1,
            num_granted: 1, // itself
            num_denied: 0
        }), Condvar::new()));

        let actual_term = guard.term + 1;
        guard.term = actual_term;
        guard.voted = true;
        drop(guard);

        for &node in others {
            let cloned_state = Arc::clone(&election_state);
            let raw_node: &'static dyn Node = unsafe { std::mem::transmute(node) };

            thread::spawn(move || {
                let outcome = raw_node.request_vote(actual_term);
                let (state_mutex, state_cvar) = &*cloned_state;
                let mut cloned_guard = state_mutex.lock().unwrap();

                match outcome {
                    Denied => {
                        cloned_guard.num_denied += 1;
                    }
                    Granted => {
                        cloned_guard.num_granted += 1;
                    }
                }
                drop(cloned_guard);
                state_cvar.notify_one();
            });
        }

        let (state_mutex, state_cvar) = &*election_state;
        let mut state_guard = state_mutex.lock().unwrap();
        state_guard = state_cvar.wait_while(state_guard, |c| {
            c.election_result().is_none()
        }).unwrap();

        let election_verdict = state_guard.election_result().unwrap();
        let final_result = if matches!(election_verdict, Win) { true } else { false };
        drop(state_guard);

        let mutex = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.leader = final_result;
        final_result
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo nodo distribuito (`Node`).
pub fn make_node() -> impl Node {
    MyNode::new()
}
