//! # Simulazione 020 — TickerService
//!
//! Un servizio che scandisce il tempo a intervalli regolari (un *ticker*) permette ad altri componenti
//! di registrare callback da eseguire ad ogni intervallo — che tipicamente devono accumulare stato
//! tra un'invocazione e l'altra, come un contatore — e, separatamente, un callback di pulizia da eseguire
//! una sola volta, quando il servizio viene fermato.
//!
//! Si scriva in Rust una struttura che implementi il tratto generico `TickerService` definito di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait TickerService: Clone + Send + Sync {
//!     fn on_tick(&self, callback: impl FnMut() + Send + 'static);
//!     fn on_stop(&self, callback: impl FnOnce() + Send + 'static);
//!     fn tick(&self);
//!     fn stop(&self);
//! }
//!
//! pub fn make_ticker_service() -> impl TickerService {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - I callback registrati con `on_tick` devono poter mutare stato tra un `tick()` e il successivo.
//! - I callback `on_tick` vengono eseguiti nell'ordine di registrazione (FIFO).
//! - Ogni callback registrato con `on_stop` deve essere invocato esattamente una volta quando `stop()` viene chiamato.
//! - Se `stop()` viene chiamato più volte, le chiamate successive non hanno alcun effetto.
//! - Dopo `stop()`, ulteriori chiamate a `tick()` non devono invocare i callback `on_tick`.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::sync::{Arc, Mutex};

/// Trait che rappresenta un gestore di eventi temporizzati con callback `FnMut` per i tick e `FnOnce` per lo stop.
pub trait TickerService: Clone + Send + Sync {
    /// Registra un callback `FnMut` da invocare ad ogni chiamata futura a `tick()`, finché il servizio non viene fermato.
    fn on_tick(&self, callback: impl FnMut() + Send + 'static);

    /// Registra un callback `FnOnce` da invocare esattamente una volta al momento della chiamata a `stop()`.
    fn on_stop(&self, callback: impl FnOnce() + Send + 'static);

    /// Invoca, una volta ciascuno e nell'ordine di registrazione, tutti i callback registrati tramite `on_tick`.
    fn tick(&self);

    /// Ferma il servizio: invoca tutti i callback `on_stop` ed impedisce a ulteriori chiamate a `tick()` di avere effetto.
    fn stop(&self);
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE TICKER SERVICE AVANZATA (STUDENT WORKSPACE)
// =================================================================================

/// Tipo alias per una callback di tick:
/// - `dyn FnMut() + Send + 'static`: Chiusura che può mutare il proprio ambiente catturato e viaggiare tra thread.
/// - `Mutex<...>`: Mutabilità interna a grana fine sul singolo callback, permettendo di ottenere `&mut`
///   senza dover bloccare l'intero catalogo del servizio.
/// - `Arc<...>`: Abilita la clonazione a costo $O(1)$ dei soli puntatori, permettendo a `tick()` di
///   rilasciare il lock globale del servizio *prima* di eseguire le chiusure dell'utente.
type TickCallback = Arc<Mutex<dyn FnMut() + Send + 'static>>;

/// Tipo alias per una callback di stop:
/// - `Box<dyn FnOnce() + Send + 'static>`: Chiusura one-shot che consuma se stessa per valore al momento dell'esecuzione.
type StopCallback = Box<dyn FnOnce() + Send + 'static>;

/// Stato interno condiviso del `TickerService`.
struct ServiceState {
    /// Lista ordinata (FIFO) delle callback di tick registrate.
    tick_functions: Vec<TickCallback>,
    /// Lista ordinata (FIFO) delle callback di stop da invocare una sola volta.
    stop_functions: Vec<StopCallback>,
    /// Flag booleano che indica se il servizio è stato terminato.
    stopped: bool,
}

impl ServiceState {
    /// Inizializza un nuovo stato del servizio attivo e privo di callback.
    fn new() -> Self {
        Self {
            tick_functions: Vec::new(),
            stop_functions: Vec::new(),
            stopped: false,
        }
    }
}

/// Implementazione concreta e thread-safe del trait `TickerService`.
pub struct MyTickerService {
    inner: Arc<Mutex<ServiceState>>,
}

impl MyTickerService {
    /// Crea una nuova istanza di `MyTickerService`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ServiceState::new())),
        }
    }
}

impl TickerService for MyTickerService {
    /// Registra una nuova callback da invocare ad ogni futuro `tick()`.
    ///
    /// ## Incapsulamento con `Arc<Mutex<...>>`:
    /// La callback viene avvolta in un `Mutex` individuale e poi in un `Arc`.
    /// Questo disaccoppia la lista globale dalla singola chiusura, consentendo l'esecuzione
    /// completamente sbloccata e prevenendo deadlock reentranti.
    fn on_tick(&self, callback: impl FnMut() + Send + 'static) {
        let mut guard = self.inner.lock().unwrap();
        if !guard.stopped {
            guard.tick_functions.push(Arc::new(Mutex::new(callback)));
        }
    }

    /// Registra una callback di pulizia `FnOnce` da invocare all'arresto del servizio.
    ///
    /// ## Gestione Robusta Post-Arresto:
    /// Se il servizio è già stato fermato (`guard.stopped == true`), la callback viene eseguita
    /// immediatamente (fuori dal lock), rispettando il requisito di esecuzione "esattamente una volta".
    fn on_stop(&self, callback: impl FnOnce() + Send + 'static) {
        let mut guard = self.inner.lock().unwrap();
        if guard.stopped {
            // Se già fermato, rilascia il lock prima di eseguire il callback
            drop(guard);
            callback();
        }
        else {
            guard.stop_functions.push(Box::new(callback));
        }
    }

    /// Esegue tutte le callback registrate tramite `on_tick` in ordine FIFO.
    ///
    /// ## 🛡️ Prevenzione Deadlock & Alta Concorrenza (Lock Free Execution):
    /// 1. Sotto il lock principale, si verifica se il servizio è fermo e si clona la lista di `Arc`
    ///    (`guard.tick_functions.clone()`), un'operazione istantanea $O(N)$ che incrementa solo i reference count.
    /// 2. **Il lock principale viene rilasciato immediatamente (`drop(guard)`) PRIMA di eseguire qualsiasi callback**.
    /// 3. Ciascuna callback viene eseguita acquisendo solo il proprio lock dedicato:
    ///    - Se una callback chiama a sua volta `service.on_tick(...)`, non ci sarà alcun deadlock perché
    ///      il lock principale è già libero!
    ///    - Altri thread possono registrare nuovi callback o invocare `stop()` in parallelo senza blocchi.
    fn tick(&self) {
        let guard = self.inner.lock().unwrap();
        if guard.stopped {
            return;
        }

        // Clona la lista dei puntatori atomici in tempo O(N)
        let callbacks: Vec<TickCallback> = guard.tick_functions.clone();

        // 👈 RILASCIO IMMEDIATO DEL LOCK PRINCIPALE
        drop(guard);

        // Esecuzione sequenziale delle callback fuori dal lock del servizio
        for cb in callbacks {
            let mut func = cb.lock().unwrap();
            func();
        }
    }

    /// Ferma definitivamente il servizio ed esegue tutte le callback `on_stop` in ordine FIFO.
    ///
    /// ## 📦 Consumo Sicuro con `std::mem::take`:
    /// 1. Sotto il lock, si imposta `stopped = true` e si estrae l'intero vettore `stop_functions`
    ///    tramite `std::mem::take(&mut guard.stop_functions)`, lasciando un `Vec` vuoto valido in tempo $O(1)$.
    /// 2. **Il lock principale viene rilasciato subito (`drop(guard)`)**.
    /// 3. Tutte le chiusure `FnOnce` estratte vengono invocate per valore (`call_once`), consumandosi
    ///    senza mantenere alcun lock attivo e preservando il rigido ordine di registrazione FIFO.
    fn stop(&self) {
        let mut guard = self.inner.lock().unwrap();
        if guard.stopped {
            return;
        }

        guard.stopped = true;

        // Estrae il vettore di FnOnce lasciando un vettore vuoto al suo posto in O(1)
        let callbacks = std::mem::take(&mut guard.stop_functions);

        // 👈 RILASCIO IMMEDIATO DEL LOCK PRINCIPALE
        drop(guard);

        // Invocazione delle chiusure FnOnce per valore (consumo naturale)
        for stop_fn in callbacks {
            stop_fn();
        }
    }
}

impl Clone for MyTickerService {
    /// Clona il puntatore `Arc`, condividendo l'istanza del servizio tra più thread.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `TickerService`.
pub fn make_ticker_service() -> impl TickerService {
    MyTickerService::new()
}
