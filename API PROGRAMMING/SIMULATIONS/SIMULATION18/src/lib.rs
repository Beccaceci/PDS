//! # Simulazione 018 — TicketLock
//!
//! `std::sync::Condvar` non garantisce alcun ordine tra i thread risvegliati da `notify_one`:
//! non è detto che sia il thread in attesa da più tempo a essere scelto. Un mutex "equo" (*fair*),
//! che garantisca l'accesso in stretto ordine di arrivo delle richieste, richiede quindi un meccanismo
//! che non si affidi all'ordine di risveglio del sistema operativo, ma lo verifichi esplicitamente
//! — è lo schema del *ticket lock*: ogni richiedente estrae un numero progressivo e attende che sia il suo turno.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `LockGuard<T>` e `TicketLock<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait LockGuard<T: Send> {
//!     fn get(&self) -> &T;
//!     fn get_mut(&mut self) -> &mut T;
//! }
//!
//! pub trait TicketLock<T: Send>: Send + Sync {
//!     fn lock(&self) -> impl LockGuard<T> + 'static;
//! }
//!
//! pub fn make_ticket_lock<T: Send + 'static>(value: T) -> impl TicketLock<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread (`Send + Sync`).
//! - L'ordine di accesso concesso deve rispettare rigorosamente l'ordine di chiamata a `lock()`, non un ordine arbitrario né l'ordine di risveglio scelto dal sistema operativo.
//! - Al più un `LockGuard<T>` può essere attivo alla volta.
//! - Quando il `LockGuard<T>` esce dallo scope, l'accesso viene rilasciato (RAII, tramite `Drop`), permettendo alla richiesta successiva in ordine di procedere.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::cell::UnsafeCell;
use std::sync::{Arc, Condvar, Mutex};

/// Trait che rappresenta una guardia RAII esclusiva sul valore protetto dal `TicketLock`.
pub trait LockGuard<T: Send> {
    /// Restituisce un riferimento immutabile al valore protetto.
    fn get(&self) -> &T;

    /// Restituisce un riferimento mutabile esclusivo al valore protetto.
    fn get_mut(&mut self) -> &mut T;
}

/// Trait che rappresenta un lock con garanzia di accodamento equo (FIFO / Ticket Lock).
pub trait TicketLock<T: Send>: Send + Sync {
    /// Acquisisce l'accesso esclusivo al valore protetto in ordine strettamente FIFO.
    /// Blocca il chiamante in modo efficiente senza consumo di cicli di CPU finché non è il suo turno.
    fn lock(&self) -> impl LockGuard<T> + 'static;
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE TICKET LOCK (STUDENT WORKSPACE)
// =================================================================================

/// Struttura dati interna per il coordinamento dei turni dei ticket.
struct TicketState {
    /// Prossimo numero progressivo di biglietto da distribuire.
    next_ticket: usize,
    /// Numero del biglietto correntemente servito ed autorizzato ad accedere alla sezione critica.
    now_serving: usize,
}

/// Stato condiviso del TicketLock.
///
/// ## Separazione tra Coordinamento e Valore Protetto:
/// - `state` e `cvar` gestiscono l'estrazione rapida e l'attesa dei ticket. Il mutex su `state`
///   viene trattenuto solo per microsecondi (estrazione/avanzamento del turno) e rilasciato subito.
/// - `value` è racchiuso in un `UnsafeCell<T>`: poiché la logica dei ticket garantisce per costruzione
///   matematica che al più una guardia `MyGuard` alla volta esista, l'accesso a `value` è garantito
///   essere safe al 100% senza alcuna data race.
struct TicketLockInner<T> {
    state: Mutex<TicketState>,
    cvar: Condvar,
    value: UnsafeCell<T>,
}

// È sicuro implementare `Sync` su `TicketLockInner<T>` se `T: Send`,
// poiché l'accesso concorrente a `UnsafeCell<T>` è rigidamente serializzato dal protocollo a biglietti.
unsafe impl<T: Send> Sync for TicketLockInner<T> {}

/// Guardia RAII che garantisce accesso esclusivo al dato protetto.
pub struct MyGuard<T: Send + 'static> {
    inner: Arc<TicketLockInner<T>>,
}

impl<T: Send + 'static> LockGuard<T> for MyGuard<T> {
    /// Restituisce un riferimento immutabile `&T` dereferenziando l'`UnsafeCell`.
    fn get(&self) -> &T {
        unsafe { &*self.inner.value.get() }
    }

    /// Restituisce un riferimento mutabile esclusivo `&mut T` dereferenziando l'`UnsafeCell`.
    fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.value.get() }
    }
}

impl<T: Send + 'static> Drop for MyGuard<T> {
    /// Al rilascio della guardia, incrementa il turno `now_serving` e risveglia tutti i thread in attesa.
    ///
    /// ## Perché `notify_all()` è obbligatoria:
    /// Ciascun thread in attesa aspetta un valore specifico di `now_serving` (`now_serving == my_ticket`).
    /// Se si usasse `notify_one()`, il sistema operativo potrebbe risvegliare un thread con un biglietto
    /// futuro, che tornerebbe a dormire "consumando" la notifica e causando uno stallo permanente.
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.now_serving += 1;
        drop(state);
        self.inner.cvar.notify_all();
    }
}

/// Implementazione concreta del trait `TicketLock`.
pub struct MyTicketLock<T: Send> {
    inner: Arc<TicketLockInner<T>>,
}

impl<T: Send> MyTicketLock<T> {
    /// Inizializza un nuovo `TicketLock` con i contatori partendo da zero.
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(TicketLockInner {
                state: Mutex::new(TicketState {
                    next_ticket: 0,
                    now_serving: 0,
                }),
                cvar: Condvar::new(),
                value: UnsafeCell::new(value),
            }),
        }
    }
}

impl<T: Send + 'static> TicketLock<T> for MyTicketLock<T> {
    /// Estrae un numero progressivo di biglietto e attende che `now_serving` raggiunga il proprio numero.
    fn lock(&self) -> impl LockGuard<T> + 'static {
        let mut state = self.inner.state.lock().unwrap();
        let my_ticket = state.next_ticket;
        state.next_ticket += 1;

        // Attesa passiva finché il turno corrente non coincide con il proprio biglietto
        state = self
            .inner
            .cvar
            .wait_while(state, |s| s.now_serving != my_ticket)
            .unwrap();

        // Rilascia il lock di coordinamento: gli altri thread possono continuare a estrarre biglietti!
        drop(state);

        MyGuard {
            inner: Arc::clone(&self.inner),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `TicketLock` proteggendo il valore iniziale specificato.
pub fn make_ticket_lock<T: Send + 'static>(value: T) -> impl TicketLock<T> {
    MyTicketLock::new(value)
}
