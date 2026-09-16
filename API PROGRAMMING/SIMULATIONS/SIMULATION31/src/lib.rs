//! # Simulazione 031 — CreditedChannel
//!
//! Un mittente che invia dati più velocemente di quanto il ricevente possa elaborarli va rallentato —
//! ma non necessariamente in base a quanti elementi sono già in coda: talvolta chi riceve preferisce concedere
//! una quota di invii permessi in anticipo (un "credito"), indipendentemente da quanti ne siano già arrivati,
//! per controllare esplicitamente il ritmo del mittente invece di limitarsi a reagire a una coda piena.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `CreditedSender<T>` e `CreditedReceiver<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait CreditedSender<T: Send>: Clone + Send + Sync {
//!     fn send(&self, value: T);
//! }
//!
//! pub trait CreditedReceiver<T: Send>: Send {
//!     fn recv(&self) -> Option<T>;
//!     fn grant_credit(&self, amount: usize);
//!     fn close(&self);
//! }
//!
//! pub fn make_credited_channel<T: Send + 'static>(
//!     initial_credit: usize,
//! ) -> (impl CreditedSender<T>, impl CreditedReceiver<T>) {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - `CreditedSender<T>` deve essere condivisibile tra più mittenti (`Clone`); `CreditedReceiver<T>` rappresenta un solo ricevente, non condivisibile.
//! - Il credito disponibile parte da `initial_credit` e non è mai legato automaticamente al numero di elementi in coda: cresce solo per effetto di `grant_credit()`, diminuisce solo per effetto di un `send()` riuscito.
//! - Se il credito disponibile è insufficiente per più mittenti in attesa contemporaneamente, un `grant_credit(amount)` deve permettere di procedere ad **al più** `amount` di essi (uno per unità di credito concessa), lasciando gli altri in attesa.
//! - `recv()` non consuma né richiede credito: è `send()`, non `recv()`, l'unica operazione soggetta al vincolo di credito.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

/// Trait che rappresenta l'estremità di invio soggetta a controllo di flusso a crediti.
pub trait CreditedSender<T: Send>: Clone + Send + Sync {
    /// Invia `value`, consumando un'unità di credito disponibile.
    /// Blocca il chiamante se il credito disponibile è 0, finché il ricevente non ne concede altro.
    fn send(&self, value: T);
}

/// Trait che rappresenta l'estremità di ricezione e concessione dei crediti.
pub trait CreditedReceiver<T: Send>: Send {
    /// Riceve il valore meno recente ancora in coda, bloccando se vuota.
    /// Restituisce `None` solo quando il canale è stato chiuso e non ci sono più valori residui.
    fn recv(&self) -> Option<T>;

    /// Concede `amount` unità di credito aggiuntive ai mittenti.
    fn grant_credit(&self, amount: usize);

    /// Chiude il canale: i mittenti non possono più inviare; `recv()` drena i valori residui poi restituisce `None`.
    fn close(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// =================================================================================
// 🏛️ ARCHITETTURA DEL SISTEMA & DISACCOPPIAMENTO DEL CONTROLLO DI FLUSSO
// =================================================================================
//
// 1. DIFFERENZA CHIAVE DA BOUNDED QUEUE:
//    In una BoundedQueue classica, la contropressione è automatica e legata alla capienza del buffer:
//    `send()` si blocca quando `len == capacity` e `recv()` libera spazio (`remaining_capacity += 1`).
//    In un CreditedChannel (stile HTTP/2 o finestre TCP), la contropressione è DISACCOPPIATA dalla coda:
//    - Il credito cresce ESCLUSIVAMENTE tramite `grant_credit(amount)`.
//    - `recv()` preleva gli elementi ma NON ripristina crediti!
//    - La coda non ha una capacità fissa: può crescere liberamente finché c'è credito disponibile.
//
// 2. COORDINAMENTO TRAMITE DUE CONDVAR DISTINTE:
//    - `cvar_has_data`: svegliata quando un mittente inserisce un valore nella coda (`values_queue.push_back`),
//      usata da `recv()` per attendere finché la coda è vuota.
//    - `cvar_has_credits`: svegliata quando il ricevente concede crediti (`grant_credit`),
//      usata da `send()` per attendere finché `remaining_credits == 0`.
//
// =================================================================================

/// Stato interno condiviso del canale a crediti.
pub struct ChannelState<T: Send> {
    /// Buffer FIFO degli elementi in attesa di essere consumati.
    values_queue: VecDeque<T>,
    /// Crediti disponibili correnti per gli invii.
    remaining_credits: usize,
    /// Flag di chiusura del canale.
    closed: bool,
}

impl<T: Send> ChannelState<T> {
    pub fn new(initial_credits: usize) -> Self {
        Self {
            values_queue: VecDeque::new(),
            remaining_credits: initial_credits,
            closed: false,
        }
    }
}

/// Struttura mittente (`CreditedSender`), condivisibile tramite clonazione (`Clone`).
pub struct MySender<T: Send> {
    /// Canale condiviso protetto da Mutex e due Condvar (has_data, has_credits).
    inner: Arc<(Mutex<ChannelState<T>>, Condvar, Condvar)>,
}

impl<T: Send> CreditedSender<T> for MySender<T> {
    fn send(&self, value: T) {
        let (mutex, cvar_has_data, cvar_has_credits) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa bloccante finché non ci sono crediti disponibili (e il canale non è chiuso)
        guard = cvar_has_credits.wait_while(guard, |state| 
            state.remaining_credits == 0 && !state.closed
        ).unwrap();

        // Se il canale non è chiuso, consuma 1 credito e accoda il valore
        if !guard.closed {
            guard.remaining_credits -= 1;
            guard.values_queue.push_back(value);

            // Rilascia il lock prima di notificare il ricevente
            drop(guard);
            cvar_has_data.notify_one();
        }
    }
}

impl<T: Send> Clone for MySender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Struttura ricevente (`CreditedReceiver`), a possesso esclusivo (non `Clone`).
pub struct MyReceiver<T: Send> {
    inner: Arc<(Mutex<ChannelState<T>>, Condvar, Condvar)>,
}

impl<T: Send> CreditedReceiver<T> for MyReceiver<T> {
    fn recv(&self) -> Option<T> {
        let (mutex, cvar_has_data, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa bloccante finché la coda è vuota (e il canale non è chiuso)
        guard = cvar_has_data.wait_while(guard, |state|
            state.values_queue.is_empty() && !state.closed
        ).unwrap();

        // Estrazione FIFO del valore (se presente).
        // NOTA BENE: recv() NON incrementa i crediti! I crediti aumentano SOLO con grant_credit().
        guard.values_queue.pop_front()
    }

    fn grant_credit(&self, amount: usize) {
        let (mutex, _, cvar_has_credits) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Incremento esplicito delle quote di credito
        guard.remaining_credits += amount;

        // Rilascia il lock e risveglia tutti i mittenti in attesa.
        // Ciascun mittente risvegliato ricontrollerà la condizione `remaining_credits > 0`,
        // garantendo che al più `amount` mittenti procedano.
        drop(guard);
        cvar_has_credits.notify_all();
    }

    fn close(&self) {
        let (mutex, cvar_has_data, cvar_has_credits) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        guard.closed = true;

        // Rilascia il lock e sveglia tutti i mittenti e il ricevente per sbloccare eventuali attese
        drop(guard);
        cvar_has_data.notify_all();
        cvar_has_credits.notify_all();
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una coppia `(Sender, Receiver)` per il canale a crediti.
pub fn make_credited_channel<T: Send + 'static>(
    initial_credit: usize,
) -> (impl CreditedSender<T>, impl CreditedReceiver<T>) {
    let channel = Arc::new((
        Mutex::new(ChannelState::new(initial_credit)),
        Condvar::new(), // cvar_has_data (per il receiver)
        Condvar::new(), // cvar_has_credits (per i senders)
    ));

    (
        MySender {
            inner: channel.clone(),
        },
        MyReceiver { inner: channel },
    )
}
