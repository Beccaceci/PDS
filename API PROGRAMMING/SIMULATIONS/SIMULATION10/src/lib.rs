//! # Simulazione 010 — AsyncResourcePool
//!
//! Nei server che gestiscono molte connessioni concorrenti tramite un runtime asincrono, un pool di risorse riutilizzabili (come connessioni a un database) non deve mai far bloccare l'intero thread del runtime mentre un task attende che una risorsa si liberi — farlo impedirebbe a tutti gli altri task in esecuzione sullo stesso thread di progredire, vanificando il vantaggio della programmazione asincrona.
//!
//! Si scriva in Rust, usando Tokio, una struttura che implementi il tratto generico `AsyncResourcePool<T: Send>`, che gestisce un insieme fisso di elementi riutilizzabili di tipo `T` e li concede in prestito esclusivo ai task che ne fanno richiesta, in modo interamente asincrono.
//!
//! ### API richiesta
//!
//! ```text
//! use std::time::Duration;
//!
//! pub trait AsyncResource<T: Send> {
//!     fn get(&self) -> &T;
//! }
//!
//! pub trait AsyncResourcePool<T: Send> {
//!     fn capacity(&self) -> usize;
//!     async fn acquire(&self) -> impl AsyncResource<T>;
//!     async fn acquire_timeout(&self, timeout: Duration) -> Option<impl AsyncResource<T>>;
//! }
//!
//! pub fn make_async_resource_pool<T: Send>(items: Vec<T>) -> impl AsyncResourcePool<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più task asincroni concorrenti, anche eseguiti su thread diversi dello stesso runtime multi-thread.
//! - Ogni elemento deve essere concesso ad al più un task alla volta.
//! - Quando l'oggetto che implementa `AsyncResource<T>` esce dallo scope, l'elemento deve tornare disponibile nel pool (RAII, tramite il tratto `Drop`) — **questo rilascio non deve mai richiedere `.await` né bloccare il thread del runtime**: deve essere immediato e completamente sincrono.
//! - Nessuna attesa attiva, e nessun uso di primitive di blocco sincrone che impedirebbero ad altri task sullo stesso thread di progredire mentre `acquire`/`acquire_timeout` sono in sospeso.
//! - I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`, con `#[tokio::test]`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

/// Trait che rappresenta un prestito esclusivo RAII di una risorsa dal pool asincrono.
pub trait AsyncResource<T: Send>: Send + Sync {
    /// Restituisce un riferimento immutabile all'elemento concesso in prestito esclusivo.
    fn get(&self) -> &T;
}

/// Trait che rappresenta un pool asincrono e thread-safe di risorse riutilizzabili.
#[allow(async_fn_in_trait)]
pub trait AsyncResourcePool<T: Send>: Send + Sync {
    /// Restituisce il numero totale di elementi gestiti dal pool.
    fn capacity(&self) -> usize;

    /// Attende in modo asincrono, senza bloccare il thread del runtime né consumare cicli di CPU,
    /// finché un elemento non è disponibile, quindi lo concede in prestito esclusivo al chiamante.
    async fn acquire(&self) -> impl AsyncResource<T>;

    /// Variante con attesa limitata: come `acquire`, ma se non ottiene un elemento entro `timeout`
    /// rinuncia e restituisce `None`. L'attesa non deve bloccare il thread del runtime.
    async fn acquire_timeout(&self, timeout: Duration) -> Option<impl AsyncResource<T>>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================

/// Handle RAII per una risorsa presa in prestito dal pool asincrono.
pub struct MyHandle<T: Send> {
    items: Arc<Mutex<Vec<T>>>,
    semaphore: Arc<Semaphore>,
    item: Option<T>,
}

impl<T: Send + Sync> AsyncResource<T> for MyHandle<T> {
    fn get(&self) -> &T {
        self.item.as_ref().unwrap()
    }
}

impl<T: Send> Drop for MyHandle<T> {
    /// Distruttore sincrono RAII: reinserisce la risorsa nella lista protetta da Mutex
    /// e incrementa i permessi del semaforo senza mai bloccare il runtime né richiedere `.await`.
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            let mut guard = self.items.lock().unwrap();
            guard.push(item);
            drop(guard);
            self.semaphore.add_permits(1);
        }
    }
}

/// Implementazione concreta del pool asincrono basata su `tokio::sync::Semaphore` e `std::sync::Mutex`.
pub struct MyResourcePool<T: Send> {
    items: Arc<Mutex<Vec<T>>>,
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl<T: Send> MyResourcePool<T> {
    pub fn new(items: Vec<T>) -> Self {
        let capacity = items.len();
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            items: Arc::new(Mutex::new(items)),
            capacity,
        }
    }
}

impl<T: Send + Sync + 'static> AsyncResourcePool<T> for MyResourcePool<T> {
    fn capacity(&self) -> usize {
        self.capacity
    }

    /// Acquisisce in modo asincrono un permesso dal semaforo (cedendo il controllo al runtime
    /// senza bloccare il thread OS) e preleva l'elemento disponibile dal buffer protetto.
    async fn acquire(&self) -> impl AsyncResource<T> {
        let permit = self.semaphore.acquire().await.unwrap();
        // Dimentichiamo il permit del semaforo per gestirne il rilascio manualmente nel Drop sincrono
        permit.forget();

        let item = self.items.lock().unwrap().pop().unwrap();
        MyHandle {
            items: Arc::clone(&self.items),
            semaphore: Arc::clone(&self.semaphore),
            item: Some(item),
        }
    }

    /// Variante temporizzata: attende al massimo `timeout` che un permesso diventi disponibile.
    async fn acquire_timeout(&self, timeout: Duration) -> Option<impl AsyncResource<T>> {
        match tokio::time::timeout(timeout, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                permit.forget();
                let item = self.items.lock().unwrap().pop().unwrap();
                Some(MyHandle {
                    items: Arc::clone(&self.items),
                    semaphore: Arc::clone(&self.semaphore),
                    item: Some(item),
                })
            }
            _ => None,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Funzione costruttore che inizializza un nuovo `AsyncResourcePool`.
pub fn make_async_resource_pool<T: Send + Sync + 'static>(items: Vec<T>) -> impl AsyncResourcePool<T> {
    MyResourcePool::new(items)
}
