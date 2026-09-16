//! # Simulazione 026 — TwoPhaseCommitCoordinator (Capstone)
//!
//! Una transazione distribuita su più partecipanti indipendenti non può limitarsi a chiedere
//! a ciascuno di confermare direttamente: se anche uno solo fallisse dopo che gli altri hanno già confermato,
//! il sistema resterebbe in uno stato incoerente. Il protocollo a due fasi (2PC) risolve il problema
//! chiedendo prima a tutti se *sarebbero* in grado di confermare (fase di preparazione), e solo se tutti
//! rispondono positivamente entro un tempo limite procede con la conferma definitiva (`commit`);
//! altrimenti annulla (`abort`) presso tutti quelli che si erano dichiarati pronti.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Participant` e `Coordinator` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! pub trait Participant: Send {
//!     fn prepare(&self) -> bool;
//!     fn commit(&self);
//!     fn abort(&self);
//! }
//!
//! pub trait Coordinator: Clone + Send + Sync {
//!     fn run_transaction(&self, participants: Vec<Box<dyn Participant>>, prepare_timeout: Duration) -> bool;
//! }
//!
//! pub fn make_coordinator() -> impl Coordinator {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - `prepare()` deve essere invocato su ciascun partecipante in parallelo (un thread per partecipante).
//! - Il timeout `prepare_timeout` si applica alla fase di preparazione nel suo complesso.
//! - Se tutti rispondono `true` entro il timeout, invoca `commit()` su tutti e restituisce `true`.
//! - Se anche uno solo risponde `false`, o se il timeout scade, invoca `abort()` su ciascun partecipante che aveva risposto `true` e restituisce `false`.
//! - Non invoca `abort()` sui partecipanti che hanno risposto `false` o la cui risposta non è ancora arrivata al momento della decisione.
//! - `run_transaction` ritorna solo dopo che tutte le chiamate a `commit()` o `abort()` rilevanti sono terminate.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`), isolamento completo tra transazioni concorrenti.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

/// Trait che rappresenta un partecipante eterogeneo a una transazione distribuita a due fasi (2PC).
///
/// **Vincolo di Concorrenza**: `Participant: Send` (ma NON `Sync`).
/// I partecipanti possono essere trasferiti per valore a un thread dedicato, ma non possono essere
/// condivisi tramite riferimenti immutabili `&` a meno di non essere protetti da una primitiva
/// di mutua esclusione come `Mutex<T>` (che è `Sync` se `T: Send`).
pub trait Participant: Send {
    /// Chiede al partecipante di prepararsi (Fase 1).
    /// Restituisce `true` se è pronto a confermare definitivamente, `false` se deve annullare.
    fn prepare(&self) -> bool;

    /// Conferma definitivamente la transazione (Fase 2 - Successo Globale).
    /// Viene invocato solo se TUTTI i partecipanti hanno risposto `true` entro il timeout.
    fn commit(&self);

    /// Annulla e ripristina lo stato precedente (Fase 2 - Rollback / Fallimento).
    /// Viene invocato SOLO sui partecipanti che avevano risposto `true` a `prepare()`.
    fn abort(&self);
}

/// Trait che rappresenta il coordinatore delle transazioni distribuite a due fasi.
pub trait Coordinator: Clone + Send + Sync {
    /// Esegue una transazione atomica a due fasi sull'insieme eterogeneo di partecipanti forniti.
    fn run_transaction(
        &self,
        participants: Vec<Box<dyn Participant>>,
        prepare_timeout: Duration,
    ) -> bool;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// =================================================================================
// 🏛️ ARCHITETTURA DEL SISTEMA & MACCHINA A STATI FINITI (FSM)
// =================================================================================
//
// 1. STATO TRANSIENTE SCOPED (PER-TRANSACTION ISOLATION):
//    `MyCoordinator` è una Unit Struct (0 byte, stateless). Non conserva alcuno stato globale,
//    garantendo che chiamate concorrenti a `run_transaction` su cloni diversi operino su insiemi
//    di risposte totalmente isolati, evitando corruzioni o interferenze tra transazioni parallele.
//
// 2. OWNERSHIP & TYPE-SYSTEM BRIDGING (`dyn Participant` Send -> Sync):
//    Poiché `Participant` implementa `Send` ma NON `Sync`, non è possibile passare `&dyn Participant`
//    tra thread. Incapsulando ogni partecipante in `Arc<Mutex<Box<dyn Participant>>>`, sfruttiamo la
//    proprietà fondamentale di Rust: `Mutex<T>` è `Sync` se `T: Send`.
//    Questo permette al thread di `prepare()` e al coordinatore di condividere in sicurezza lo stesso oggetto.
//
// 3. DIAGRAMMA DELLA MACCHINA A STATI FINITI (2PC COORDINATOR FSM):
//
//               [ Inizio run_transaction(participants, timeout) ]
//                                      │
//                                      ▼
//                    ┌───────────────────────────────────┐
//                    │ FASE 1: PREPARATION PARALLELA     │
//                    │ (N thread spawnati in parallelo)  │
//                    └─────────────────┬─────────────────┘
//                                      │
//                    wait_timeout_while(responses.any(None))
//                                      │
//                                      ▼
//                    ┌───────────────────────────────────┐
//                    │ VALUTAZIONE DELLA DECISIONE:      │
//                    │ Tutti i partecipanti = Some(true)?│
//                    └─────────┬───────────────┬─────────┘
//                              │               │
//                     SI (100% Successo)       NO (Timeout o Almeno 1 False)
//                              │               │
//                              ▼               ▼
//         ┌─────────────────────────┐     ┌──────────────────────────────────┐
//         │ FASE 2A: COMMIT TOTALE  │     │ FASE 2B: ABORT SELETTIVO         │
//         │ - Invoca commit()       │     │ - Invoca abort() SOLO su chi     │
//         │   su TUTTI i N          │     │   aveva risposto Some(true)      │
//         │   partecipanti          │     │ - Ignora i None e i Some(false)  │
//         └────────────┬────────────┘     └────────────────┬─────────────────┘
//                      │                                   │
//                      ▼                                   ▼
//               [ Ritorna true ]                    [ Ritorna false ]
//
// =================================================================================

/// Struttura transiente locale che raccoglie le risposte asincrone della fase di preparazione.
struct TxState {
    /// Vettore indicizzato [0..N]:
    /// - `None`: risposta non ancora pervenuta (thread ancora in esecuzione).
    /// - `Some(true)`: partecipante pronto a confermare.
    /// - `Some(false)`: partecipante impossibilitato a confermare.
    responses: Vec<Option<bool>>,
}

/// Coordinatore stateless (Zero-Sized Type).
/// Non mantiene stato persistente per garantire l'isolamento thread-safe tra transazioni concorrenti.
#[derive(Clone)]
pub struct MyCoordinator;

impl Coordinator for MyCoordinator {
    fn run_transaction(
        &self,
        participants: Vec<Box<dyn Participant>>,
        prepare_timeout: Duration,
    ) -> bool {
        let num_participants = participants.len();

        // Se non ci sono partecipanti, la transazione è vacuamente valida ed eseguita con successo
        if num_participants == 0 {
            return true;
        }

        // =========================================================================
        // PASSO 1: INCAPSULAMENTO DEI PARTECIPANTI (Send -> Sync via Mutex)
        // =========================================================================
        // Ogni `Box<dyn Participant>` viene inserito in un proprio `Arc<Mutex<...>>`.
        // In questo modo il puntatore può essere clonato e passato al thread di `prepare()`,
        // mentre il coordinatore conserva un clone per invocare successivamente `commit()` o `abort()`.
        let wrapped: Vec<Arc<Mutex<Box<dyn Participant>>>> = participants
            .into_iter()
            .map(|p| Arc::new(Mutex::new(p)))
            .collect();

        // =========================================================================
        // PASSO 2: ALLOCAZIONE DELLO STATO DELLA TRANSAZIONE (Locale & Scoped)
        // =========================================================================
        let tx_state = Arc::new((
            Mutex::new(TxState {
                responses: vec![None; num_participants],
            }),
            Condvar::new(),
        ));

        // =========================================================================
        // PASSO 3: LANCIO DEI THREAD DI PREPARAZIONE IN PARALLELO
        // =========================================================================
        // Ciascun partecipante esegue `prepare()` su un thread dedicato, in modo che
        // il timeout complessivo si applichi alla fase globale e non alla somma sequenziale.
        for i in 0..num_participants {
            let p_clone = Arc::clone(&wrapped[i]);
            let tx_clone = Arc::clone(&tx_state);

            thread::spawn(move || {
                // Esecuzione di prepare() protetta dal lock del partecipante
                let result = p_clone.lock().unwrap().prepare();

                // Scrittura del risultato nella cella corrispondente e notifica al coordinatore
                let (mutex, cvar) = &*tx_clone;
                let mut guard = mutex.lock().unwrap();
                guard.responses[i] = Some(result);
                cvar.notify_all();
            });
        }

        // =========================================================================
        // PASSO 4: ATTESA CON TIMEOUT DEL COORDINATORE
        // =========================================================================
        // Il coordinatore si mette in attesa bloccante su `Condvar` finché:
        // - Tutti i partecipanti hanno risposto (nessuna cella è più `None`), OPPURE
        // - Il timeout globale `prepare_timeout` è scaduto.
        let (mutex, cvar) = &*tx_state;
        let mut guard = mutex.lock().unwrap();
        (guard, _) = cvar
            .wait_timeout_while(guard, prepare_timeout, |state| {
                state.responses.iter().any(|r| r.is_none())
            })
            .unwrap();

        // =========================================================================
        // PASSO 5: DECISIONE GLOBALE ATOMICA
        // =========================================================================
        // La transazione può essere confermata (`commit = true`) SE E SOLO SE:
        // Tutti gli N partecipanti hanno risposto `Some(true)`.
        // Se anche uno solo ha risposto `Some(false)` o è ancora `None` (timeout), `commit` sarà `false`.
        let commit = guard.responses.iter().all(|option_result| {
            if let Some(result) = option_result {
                *result
            } else {
                false
            }
        });

        // =========================================================================
        // PASSO 6: ESECUZIONE FASE 2 (COMMIT GLOBALE O ABORT SELETTIVO)
        // =========================================================================
        // Requisito vincolante: Il metodo ritorna SOLO dopo che tutte le chiamate
        // a commit() o abort() rilevanti sono state completate al 100%.
        for (index, p) in wrapped.into_iter().enumerate() {
            if commit {
                // Caso di Successo: Invoca commit() su ciascun partecipante
                p.lock().unwrap().commit();
            } else if guard.responses[index] == Some(true) {
                // Caso di Fallimento: Invoca abort() SOLO su chi aveva risposto positivamente.
                // Chi ha risposto false o chi è ancora in esecuzione (None) NON viene abortito!
                p.lock().unwrap().abort();
            }
        }

        commit
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `Coordinator`.
pub fn make_coordinator() -> impl Coordinator {
    MyCoordinator
}