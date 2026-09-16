//! # Simulazione 008 — DelayQueue
//!
//! Molti sistemi devono rendere disponibile un dato solo dopo un certo intervallo di tempo dalla sua produzione — un tentativo di retry da ritardare dopo un fallimento, una voce di cache con scadenza, un timer di sistema. A differenza di una coda ordinaria, qui la disponibilità di un elemento non dipende solo dall'ordine di arrivo, ma da un'informazione temporale associata a ciascun elemento, che può anche essere annullata prima che scada.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `ScheduledItem<T: Send>` e `DelayQueue<T: Send>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! use std::time::Duration;
//!
//! pub trait ScheduledItem<T: Send> {
//!     fn cancel(&self) -> bool;
//! }
//!
//! pub trait DelayQueue<T: Send>: Clone + Send + Sync {
//!     fn push_after(&self, value: T, delay: Duration) -> impl ScheduledItem<T> + 'static;
//!     fn pop(&self) -> T;
//! }
//!
//! pub fn make_delay_queue<T: Send + 'static>() -> impl DelayQueue<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più produttori e più consumatori (da cui `Clone` sul tratto `DelayQueue` stesso).
//! - `pop()` deve restituire sempre l'elemento non annullato con la scadenza più vicina, non necessariamente il primo inserito.
//! - Se, mentre un thread è bloccato in `pop()` in attesa della scadenza dell'elemento più vicino conosciuto, un altro thread inserisce un elemento con scadenza ancora più vicina, il thread in attesa deve accorgersene e ridurre di conseguenza il proprio tempo di attesa residuo — non deve attendere fino alla scadenza dell'elemento che conosceva all'inizio.
//! - Un `cancel()` riuscito rimuove l'elemento dalla coda in modo che non venga mai restituito da `pop()`, senza che il chiamante di `pop()` debba occuparsene esplicitamente.
//! - Nessuna attesa attiva in nessun punto del sistema.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

/// Trait che rappresenta un handle per un elemento pianificato nel tempo.
pub trait ScheduledItem<T: Send> {
    /// Annulla la consegna futura dell'elemento associato, se non è ancora stato reso disponibile a `pop`.
    /// Restituisce `true` se l'annullamento ha avuto effetto, `false` se l'elemento era già stato
    /// consegnato (o il suo termine era già scaduto al momento della chiamata).
    fn cancel(&self) -> bool;
}

/// Trait che rappresenta una coda temporizzata concorrente con consegna ordinata per scadenza.
pub trait DelayQueue<T: Send>: Clone + Send + Sync {
    /// Inserisce `value`, che diventerà disponibile per `pop` solo dopo che sarà trascorso `delay`
    /// dal momento di questa chiamata. Restituisce un handle che permette di annullarne la consegna futura.
    fn push_after(&self, value: T, delay: Duration) -> impl ScheduledItem<T> + 'static;

    /// Preleva l'elemento non ancora annullato con la scadenza più vicina.
    /// Se il suo termine non è ancora trascorso, blocca il chiamante fino alla scadenza
    /// (senza consumare cicli di CPU). Se non ci sono elementi in coda, blocca indefinitamente.
    fn pop(&self) -> T;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================

/// Handle restituito al chiamante di `push_after`, che consente di cancellare atomicamente
/// la consegna dell'elemento pianificato senza dover acquisire il lock dell'intera coda.
pub struct ScheduledItemHandle {
    /// Flag atomico condiviso tra l'handle esterno e la voce memorizzata internamente nella coda.
    canceled: Arc<AtomicBool>,
}

impl<T: Send> ScheduledItem<T> for ScheduledItemHandle {
    /// Annulla la consegna dell'elemento.
    /// Utilizza `swap(true, SeqCst)`: se il valore precedente era `false`, l'annullamento ha avuto successo
    /// e restituisce `true`. Se era già `true` (perché già annullato o già consegnato da `pop()`), restituisce `false`.
    fn cancel(&self) -> bool {
        !self.canceled.swap(true, SeqCst)
    }
}

/// Struttura interna che rappresenta un singolo elemento presente nella coda temporizzata.
pub struct MyHandle<T: Send> {
    /// Il valore generico effettivo da consegnare al consumatore.
    value: T,
    /// L'istante temporale assoluto (`Instant`) a partire dal quale l'elemento può essere consumato.
    deadline: Instant,
    /// Puntatore al flag atomico di cancellazione condiviso con l'handle esterno.
    canceled: Arc<AtomicBool>,
}

/// Implementazione thread-safe della coda temporizzata `DelayQueue`.
pub struct MyQueue<T: Send> {
    /// Stato interno protetto da `Mutex` contenente il vettore ordinato degli elementi,
    /// associato a una `Condvar` per coordinare le attese temporizzate dei consumatori.
    inner: Arc<(Mutex<Vec<MyHandle<T>>>, Condvar)>,
}

impl<T: Send> MyQueue<T> {
    /// Crea una nuova istanza di `MyQueue` con vettore vuoto e variabile di condizione.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
        }
    }
}

impl<T: Send> Clone for MyQueue<T> {
    /// Clona l'`Arc` interno, consentendo la condivisione sicura della coda tra thread multipli.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send + Sync + 'static> DelayQueue<T> for MyQueue<T> {
    /// Inserisce un nuovo elemento programmato per la consegna tra `delay` intervallo di tempo.
    /// Mantiene il vettore ordinato in **ordine decrescente di deadline**, in modo che l'elemento
    /// con la scadenza più vicina (imminente) si trovi sempre in fondo al vettore (`last()`),
    /// consentendo estrazioni $O(1)$ con `pop()`.
    fn push_after(&self, value: T, delay: Duration) -> impl ScheduledItem<T> + 'static {
        let deadline = Instant::now() + delay;
        let canceled_flag = Arc::new(AtomicBool::new(false));

        let handle = MyHandle {
            value,
            deadline,
            canceled: Arc::clone(&canceled_flag),
        };

        let returned_item = ScheduledItemHandle {
            canceled: canceled_flag,
        };

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Ricerca binaria del punto di inserimento per mantenere l'ordine decrescente di scadenza:
        // [scadenza_lontana, ..., scadenza_intermedia, scadenza_vicina]
        let pos = guard.partition_point(|h| h.deadline >= deadline);
        guard.insert(pos, handle);
        drop(guard);

        // Notifica tutti i thread in attesa in pop(): se è arrivata una scadenza più vicina
        // rispetto a quella attualmente attesa, i consumatori ricalcoleranno il timeout residuo.
        cvar.notify_all();

        returned_item
    }

    /// Estrae l'elemento con la scadenza più imminente non ancora cancellato.
    /// Se la scadenza non è ancora trascorsa, sospende il thread senza consumo di CPU
    /// fino alla deadline calcolata (o finché un nuovo elemento più urgente non viene inserito).
    fn pop(&self) -> T {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            // Ispezione dell'elemento in fondo al vettore (scadenza più vicina)
            if let Some(handle) = guard.last() {
                let now = Instant::now();

                if handle.deadline <= now {
                    // La scadenza è già trascorsa: rimuoviamo l'elemento dal vettore in O(1)
                    let handle = guard.pop().unwrap();

                    // Se l'elemento non è stato cancellato, marchiamo il flag a true
                    // (impedendo che chiamate successive a cancel() restituiscano true) e restituiamo il dato.
                    if !handle.canceled.swap(true, SeqCst) {
                        return handle.value;
                    }
                    // Se l'elemento era stato cancellato, lo scartiamo silenziosamente e riesaminiamo la coda.
                    continue;
                } else {
                    // La scadenza è nel futuro: calcoliamo il tempo residuo esatto di attesa
                    let remaining = handle.deadline.saturating_duration_since(now);

                    // Sospensione temporizzata sulla Condvar: rilascia il lock e attende fino a `remaining`
                    // oppure finché una push_after() non risveglia il thread con notify_all().
                    let (new_guard, _) = cvar.wait_timeout(guard, remaining).unwrap();
                    guard = new_guard;
                }
            } else {
                // Coda completamente vuota: attesa indefinita finché non viene inserito un elemento
                guard = cvar.wait_while(guard, |c| c.is_empty()).unwrap();
            }
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che inizializza una nuova `DelayQueue`.
pub fn make_delay_queue<T: Send + Sync + 'static>() -> impl DelayQueue<T> {
    MyQueue::new()
}
