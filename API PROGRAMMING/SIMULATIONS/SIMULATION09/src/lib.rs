//! # Simulazione 009 — WaitGroup
//!
//! Quando un thread principale avvia un numero di compiti indipendenti su altri thread e deve proseguire solo dopo che **tutti** sono terminati, non è sufficiente un semplice contatore: bisogna anche garantire che ogni compito venga contato esattamente una volta, indipendentemente dal fatto che segnali il proprio completamento esplicitamente o che il proprio "segnaposto" venga semplicemente lasciato uscire dallo scope.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `Token` e `WaitGroup` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! use std::time::Duration;
//!
//! pub trait Token: Send {
//!     fn done(self);
//! }
//!
//! pub trait WaitGroup: Clone + Send + Sync {
//!     fn add(&self) -> impl Token + Send + 'static;
//!     fn wait(&self);
//!     fn wait_timeout(&self, timeout: Duration) -> bool;
//! }
//!
//! pub fn make_wait_group() -> impl WaitGroup {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread (da cui `Clone` sul tratto `WaitGroup` stesso).
//! - Ogni `add()` incrementa il contatore interno di uno; ogni completamento — tramite `done()` esplicito oppure tramite `Drop` del token — lo decrementa di uno; il contatore non deve mai scendere sotto zero, né essere decrementato due volte per lo stesso token.
//! - `wait()` e `wait_timeout()` devono sbloccarsi non appena il contatore torna a zero. Se nuovi `add()` vengono registrati dopo che il contatore è già tornato a zero, i thread la cui `wait()` era già ritornata non ne sono influenzati, ma una nuova chiamata a `wait()`/`wait_timeout()` successiva a quei nuovi `add()` deve attendere il nuovo azzeramento.
//! - Nessuna attesa attiva in nessun punto del sistema.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

/// Trait che rappresenta un token di compito appartenente a un [`WaitGroup`].
pub trait Token: Send {
    /// Segnala che il compito rappresentato da questo token è terminato, decrementando di uno
    /// il contatore del gruppo di attesa. Se questo metodo non viene mai chiamato esplicitamente
    /// e il token esce dallo scope, il completamento viene comunque segnalato automaticamente
    /// (RAII, tramite il tratto `Drop`). In nessun caso lo stesso token deve poter decrementare
    /// il contatore più di una volta.
    fn done(self);
}

/// Trait che rappresenta un gruppo di sincronizzazione per compiti concorrenti (WaitGroup).
pub trait WaitGroup: Clone + Send + Sync {
    /// Registra un nuovo compito da attendere, incrementando il contatore interno di uno,
    /// e restituisce il token corrispondente.
    fn add(&self) -> impl Token + Send + 'static;

    /// Blocca il chiamante, senza consumare cicli di CPU, finché il contatore interno non torna a zero.
    fn wait(&self);

    /// Variante con attesa limitata: come `wait`, ma se il contatore non torna a zero entro `timeout`
    /// rinuncia e restituisce `false`; restituisce `true` se il contatore è tornato a zero in tempo.
    fn wait_timeout(&self, timeout: Duration) -> bool;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================

/// Token RAII che rappresenta un compito attivo all'interno del `WaitGroup`.
/// Quando il token viene consumato esplicitamente (`done(self)`) o esce dallo scope (`Drop`),
/// decrementa atomicamente il contatore dei compiti pendenti nel gruppo condiviso.
pub struct MyToken {
    /// Riferimento condiviso al contatore dei compiti pendenti e alla variabile di condizione.
    inner: Arc<(Mutex<usize>, Condvar)>,
    /// Flag atomico per garantire l'idempotenza: evita doppi decrementi se `done()` e `Drop` scattano in sequenza.
    dropped: AtomicBool,
}

impl Token for MyToken {
    /// Segnala esplicitamente il completamento del compito.
    /// Poiché `done(self)` consuma l'istanza `self` per valore (`by value`), al termine di questo blocco
    /// Rust invoca automaticamente il distruttore `Drop::drop(&mut self)`.
    /// Questo elimina qualsiasi duplicazione di codice, centralizzando il decremento in `Drop`.
    fn done(self) {
        // Il corpo è intenzionalmente vuoto: il consumo di `self` delega il lavoro a `Drop::drop`.
    }
}

impl Drop for MyToken {
    /// Distruttore RAII: decrementa il contatore esattamente una volta.
    fn drop(&mut self) {
        // `swap(true, SeqCst)` restituisce il valore precedente:
        // - Se era `false`, questa è la prima volta che il token viene completato/distrutto.
        // - Se era già `true`, significa che è già stato contato, evitando l'underflow del contatore.
        if !self.dropped.swap(true, SeqCst) {
            let (mutex, cvar) = &*self.inner;
            let mut guard = mutex.lock().unwrap();
            *guard -= 1;

            // Se questo era l'ultimo compito pendente, risvegliamo TUTTI i thread bloccati su `wait()`
            if *guard == 0 {
                drop(guard);
                cvar.notify_all();
            }
        }
    }
}

/// Implementazione concreta del `WaitGroup` basata su `Mutex<usize>` e `Condvar`.
pub struct MyGroup {
    /// Stato interno condiviso tra il gruppo e tutti i token emessi.
    inner: Arc<(Mutex<usize>, Condvar)>,
}

impl MyGroup {
    /// Crea una nuova istanza di `MyGroup` con contatore iniziale a 0.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(0usize), Condvar::new())),
        }
    }
}

impl Clone for MyGroup {
    /// Clona l'`Arc` interno, consentendo la condivisione della medesima barriera tra più thread.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl WaitGroup for MyGroup {
    /// Registra un nuovo compito da attendere, incrementando il contatore interno di 1 prima di restituire il token.
    fn add(&self) -> impl Token + Send + 'static {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        *guard += 1;

        MyToken {
            inner: Arc::clone(&self.inner),
            dropped: AtomicBool::new(false),
        }
    }

    /// Blocca il thread chiamante senza consumo di CPU finché il contatore dei compiti pendenti non torna a 0.
    fn wait(&self) {
        let (mutex, cvar) = &*self.inner;
        let guard = mutex.lock().unwrap();

        // `wait_while` gestisce automaticamente i risvegli spuri, sospendendo il thread finché `*c > 0`.
        drop(cvar.wait_while(guard, |c| *c > 0).unwrap());
    }

    /// Variante con timeout: attende che il contatore scenda a 0 entro `timeout`.
    /// Restituisce `true` se il contatore è tornato a 0, `false` se è scaduto il timeout.
    fn wait_timeout(&self, timeout: Duration) -> bool {
        let (mutex, cvar) = &*self.inner;
        let guard = mutex.lock().unwrap();

        let (guard, _) = cvar
            .wait_timeout_while(guard, timeout, |c| *c > 0)
            .unwrap();

        *guard == 0
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che inizializza un nuovo `WaitGroup`.
pub fn make_wait_group() -> impl WaitGroup {
    MyGroup::new()
}
