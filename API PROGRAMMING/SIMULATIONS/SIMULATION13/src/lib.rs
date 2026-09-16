//! # Simulazione 013 — AgingScheduler
//!
//! Una coda a priorità concorrente in cui la priorità degli elementi cresce col passare del tempo
//! (invecchiamento / *aging*), combinata con la possibilità di annullare (`cancel()`) o promuovere (`boost()`)
//! elementi già accodati tramite un ticket dedicato, e con supporto per la chiusura controllata (`close()`).
//!
//! ### API richiesta
//!
//! ```text
//! pub trait Ticket: Send + Sync {
//!     fn cancel(&self) -> bool;
//!     fn boost(&self, extra: u32);
//! }
//!
//! pub trait AgingScheduler<T: Send>: Clone + Send + Sync {
//!     fn push(&self, value: T, priority: u32) -> Option<impl Ticket + 'static>;
//!     fn pop(&self) -> Option<T>;
//!     fn close(&self);
//! }
//!
//! pub fn make_aging_scheduler<T: Send + Sync + 'static>(aging_rate: f64) -> impl AgingScheduler<T> {
//!     ...
//! }
//! ```

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// Trait che rappresenta un ticket per gestire e manipolare un elemento accodato nello scheduler.
pub trait Ticket: Send + Sync {
    /// Annulla l'elemento associato a questo ticket, se non è ancora stato
    /// prelevato da `pop()`. Restituisce `true` se l'annullamento ha avuto
    /// effetto, `false` se l'elemento era già stato prelevato o annullato.
    fn cancel(&self) -> bool;

    /// Aumenta immediatamente, di `extra`, la priorità di base dell'elemento associato,
    /// sommandosi a qualunque invecchiamento già maturato.
    /// Non ha effetto se l'elemento è già stato prelevato o annullato.
    fn boost(&self, extra: u32);
}

/// Trait che rappresenta uno scheduler prioritario concorrente con invecchiamento dinamico (*aging*).
pub trait AgingScheduler<T: Send>: Clone + Send + Sync {
    /// Inserisce `value` con priorità di base `priority` (valori più alti
    /// indicano priorità maggiore) e restituisce un ticket. Se lo scheduler
    /// è già stato chiuso, restituisce `None`.
    fn push(&self, value: T, priority: u32) -> Option<impl Ticket + 'static>;

    /// Preleva l'elemento non annullato con la priorità effettiva più alta
    /// al momento della chiamata, bloccando senza consumare cicli di CPU se
    /// non ce ne sono. La priorità effettiva di un elemento è la sua
    /// priorità di base (comprensiva di eventuali `boost`) incrementata di
    /// `aging_rate` unità per ogni secondo trascorso dal suo inserimento.
    /// Se lo scheduler è chiuso e non ci sono più elementi validi, restituisce `None`.
    fn pop(&self) -> Option<T>;

    /// Chiude lo scheduler, impedendo nuovi inserimenti e risvegliando
    /// tutti i consumatori bloccati in attesa.
    fn close(&self);
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE DELLO SCHEDULER AD INVECCHIAMENTO DINAMICO
// =================================================================================

/// Handle RAII del ticket restituito al produttore.
/// Consente operazioni `cancel()` e `boost()` 100% Lock-Free tramite atomici.
pub struct MyTicketHandle {
    /// Quantità di priorità aggiuntiva accumulata tramite chiamate a `boost()`.
    boost: Arc<AtomicU32>,
    /// Flag atomico che indica se l'elemento è ancora valido e attivo in coda.
    valid: Arc<AtomicBool>,
}

impl Ticket for MyTicketHandle {
    /// Annulla l'elemento associato in modo atomico.
    /// Restituisce `true` se l'elemento era attivo e viene annullato con successo,
    /// `false` se era già stato annullato o prelevato da `pop()`.
    fn cancel(&self) -> bool {
        self.valid.swap(false, Ordering::SeqCst)
    }

    /// Incrementa atomicamente la priorità di base dell'elemento.
    /// Non ha effetto se l'elemento è già stato estratto o annullato.
    fn boost(&self, extra: u32) {
        if self.valid.load(Ordering::SeqCst) {
            self.boost.fetch_add(extra, Ordering::SeqCst);
        }
    }
}

/// Struttura interna che rappresenta un singolo elemento presente nella coda dello scheduler.
pub struct QueueItem<T: Send> {
    /// Il valore del task/elemento.
    value: T,
    /// Priorità iniziale di base assegnata al momento dell'inserimento.
    base_priority: u32,
    /// Timestamp esatto dell'istante di inserimento.
    initial_timestamp: Instant,
    /// Flag condiviso di validità (condiviso con `MyTicketHandle`).
    valid: Arc<AtomicBool>,
    /// Boost cumulativo accumulato (condiviso con `MyTicketHandle`).
    boost: Arc<AtomicU32>,
}

impl<T: Send> QueueItem<T> {
    /// Crea un nuovo elemento di coda inizializzando il timestamp corrente.
    pub fn new(value: T, base_priority: u32) -> Self {
        Self {
            value,
            base_priority,
            initial_timestamp: Instant::now(),
            valid: Arc::new(AtomicBool::new(true)),
            boost: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Calcola la priorità effettiva dinamica dell'elemento all'istante `now`:
    /// $$\text{eff} = \text{base} + \text{boost} + \text{aging\_rate} \times \Delta t$$
    pub fn get_actual_priority(&self, aging_rate: f64, now: Instant) -> f64 {
        let elapsed_secs = now.duration_since(self.initial_timestamp).as_secs_f64();
        (self.base_priority + self.boost.load(Ordering::SeqCst)) as f64 + (elapsed_secs * aging_rate)
    }
}

/// Stato interno protetto da Mutex.
struct SchedulerState<T: Send> {
    /// Insieme degli elementi attualmente presenti in coda.
    items: Vec<QueueItem<T>>,
    /// Flag che indica se lo scheduler è stato chiuso tramite `close()`.
    closed: bool,
}

impl<T: Send> SchedulerState<T> {
    /// Ricerca l'indice dell'elemento con la priorità effettiva massima all'istante `now`.
    /// Considera esclusivamente gli elementi ancora validi (`valid == true`).
    pub fn find_highest_priority_index(&self, aging_rate: f64, now: Instant) -> Option<usize> {
        let mut best_idx = None;
        let mut best_priority = -1.0f64;

        for (index, item) in self.items.iter().enumerate() {
            if item.valid.load(Ordering::SeqCst) {
                let actual_priority = item.get_actual_priority(aging_rate, now);
                if actual_priority > best_priority {
                    best_idx = Some(index);
                    best_priority = actual_priority;
                }
            }
        }

        best_idx
    }
}

/// Implementazione concreta di `AgingScheduler` con invecchiamento continuo on-demand.
pub struct MyScheduler<T: Send> {
    /// Stato condiviso protetto da Mutex e variabile di condizione.
    inner: Arc<(Mutex<SchedulerState<T>>, Condvar)>,
    /// Tasso di invecchiamento globale espresso in unità di priorità per secondo.
    aging_rate: f64,
}

impl<T: Send> MyScheduler<T> {
    /// Inizializza un nuovo scheduler con il tasso di invecchiamento specificato.
    pub fn new(aging_rate: f64) -> Self {
        let state = SchedulerState {
            items: Vec::new(),
            closed: false,
        };
        Self {
            inner: Arc::new((Mutex::new(state), Condvar::new())),
            aging_rate,
        }
    }
}

impl<T: Send + Sync + 'static> AgingScheduler<T> for MyScheduler<T> {
    /// Inserisce un nuovo elemento nella coda con la priorità specificata.
    /// Se lo scheduler è chiuso, restituisce `None`.
    fn push(&self, value: T, priority: u32) -> Option<impl Ticket + 'static> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            return None;
        }

        let new_item = QueueItem::new(value, priority);
        let handle = MyTicketHandle {
            valid: Arc::clone(&new_item.valid),
            boost: Arc::clone(&new_item.boost),
        };

        guard.items.push(new_item);
        drop(guard);

        // Notifica un consumatore in attesa
        cvar.notify_one();

        Some(handle)
    }

    /// Preleva l'elemento valido con la priorità effettiva massima all'istante esatto della chiamata.
    /// Si blocca se non ci sono elementi validi e la coda è aperta.
    /// Restituisce `None` solo se la coda è chiusa e non contiene più elementi validi.
    fn pop(&self) -> Option<T> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            // Attende finché non c'è almeno un elemento valido OPPURE la coda viene chiusa
            guard = cvar
                .wait_while(guard, |state| {
                    !state.closed && !state.items.iter().any(|item| item.valid.load(Ordering::SeqCst))
                })
                .unwrap();

            let now = Instant::now();
            let best_idx = guard.find_highest_priority_index(self.aging_rate, now);

            if let Some(idx) = best_idx {
                let item = guard.items.swap_remove(idx);
                // Marca atomicamente l'elemento come estratto (non più annullabile)
                if item.valid.swap(false, Ordering::SeqCst) {
                    return Some(item.value);
                }
            }
            else if guard.closed {
                // Se la coda è chiusa e non contiene più elementi validi, restituisce None
                return None;
            }
        }
    }

    /// Chiude lo scheduler, impedendo nuovi inserimenti e risvegliando tutti i thread in attesa.
    fn close(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);

        cvar.notify_all();
    }
}

impl<T: Send> Clone for MyScheduler<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            aging_rate: self.aging_rate,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Crea e restituisce una nuova istanza di uno scheduler ad invecchiamento (`AgingScheduler`).
pub fn make_aging_scheduler<T: Send + Sync + 'static>(aging_rate: f64) -> impl AgingScheduler<T> {
    MyScheduler::new(aging_rate)
}
