//! # Simulazione 016 — BatchPool
//!
//! Un sistema che elabora lavori a lotti (ad esempio un motore di rendering che distribuisce i fotogrammi
//! su un certo numero di buffer di calcolo) a volte ha bisogno di più unità della stessa risorsa
//! contemporaneamente — non unità specifiche e identificate, ma un numero qualsiasi di unità equivalenti
//! tra loro, prese da un insieme comune.
//!
//! Si scrivano in Rust le strutture che implementano i tratti generici `ResourceBatch<T: Send>` e `BatchPool<T: Send>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! use std::time::Duration;
//!
//! pub trait ResourceBatch<T: Send>: Send {
//!     fn items(&self) -> &[T];
//! }
//!
//! pub trait BatchPool<T: Send>: Send + Sync {
//!     fn capacity(&self) -> usize;
//!     fn acquire_batch(&self, count: usize) -> impl ResourceBatch<T> + 'static;
//!     fn acquire_batch_timeout(&self, count: usize, timeout: Duration) -> Option<impl ResourceBatch<T> + 'static>;
//! }
//!
//! pub fn make_batch_pool<T: Send + Sync + 'static>(items: Vec<T>) -> impl BatchPool<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread.
//! - `acquire_batch(count)` deve restituire esattamente `count` elementi presi dal pool, senza vincoli sulla loro identità.
//! - Quando l'oggetto che implementa `ResourceBatch<T>` esce dallo scope, tutti gli elementi che conteneva tornano disponibili contemporaneamente (RAII, tramite il tratto `Drop`).
//! - Nessuna attesa attiva in nessun punto.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Trait che rappresenta un lotto (batch) di risorse acquisite contemporaneamente.
/// Fornisce accesso in sola lettura (slice `&[T]`) a tutte le unità ottenute in prestito.
/// Quando l'oggetto esce dallo scope (`Drop`), tutte le risorse ritornano contemporaneamente al pool in modo atomico.
pub trait ResourceBatch<T: Send>: Send {
    /// Restituisce un riferimento slice a tutti gli elementi appartenenti a questo lotto.
    fn items(&self) -> &[T];
}

/// Trait che rappresenta un gestore di pool di risorse anonime ed intercambiabili con acquisizione a lotti.
pub trait BatchPool<T: Send>: Send + Sync {
    /// Restituisce il numero totale di elementi gestiti dal pool.
    fn capacity(&self) -> usize;

    /// Preleva esattamente `count` elementi dal pool — un numero qualsiasi tra quelli disponibili —
    /// bloccando il chiamante, senza consumare cicli di CPU, finché non ce ne sono almeno `count` disponibili simultaneamente.
    fn acquire_batch(&self, count: usize) -> impl ResourceBatch<T> + 'static;

    /// Variante con attesa limitata: tenta di prelevare `count` elementi entro `timeout`.
    /// Se il timeout scade prima che `count` elementi siano disponibili, restituisce `None`.
    fn acquire_batch_timeout(&self, count: usize, timeout: Duration) -> Option<impl ResourceBatch<T> + 'static>;
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE DEL POOL DI RISORSE A LOTTI (BATCH POOL)
// =================================================================================

/// Lotto di risorse concesso in prestito a un thread chiamante.
/// Implementa il pattern RAII: al momento della distruzione (`Drop`), restituisce
/// tutti gli elementi in blocco al pool condiviso sotto un unico lock.
pub struct MyBatch<T: Send> {
    /// Riferimento condiviso al pool centrale e alla variabile di condizione.
    shared_state: Arc<(Mutex<Vec<T>>, Condvar)>,
    /// Insieme di risorse possedute da questo specifico lotto.
    items: Vec<T>,
}

impl<T: Send> ResourceBatch<T> for MyBatch<T> {
    /// Restituisce un riferimento slice immutabile agli elementi posseduti.
    fn items(&self) -> &[T] {
        self.items.as_slice()
    }
}

impl<T: Send> Drop for MyBatch<T> {
    /// Restituzione atomica del lotto:
    /// Inserisce tutti gli elementi posseduti nuovamente nel pool tramite `guard.append(&mut self.items)`
    /// in tempo O(1) e notifica tutti i thread in attesa (`cvar.notify_all()`).
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_state;
        let mut guard = mutex.lock().unwrap();

        // Trasferisce tutti gli elementi in blocco senza riallocazioni
        guard.append(&mut self.items);
        drop(guard);

        // Notifica tutti i thread in attesa (più thread potrebbero riuscire ad acquisire i rispettivi count)
        cvar.notify_all();
    }
}

/// Implementazione concreta del pool di risorse anonime ed intercambiabili.
pub struct MyBatchPool<T: Send> {
    /// Stato condiviso: un `Mutex` che protegge il vettore delle risorse disponibili,
    /// e una `Condvar` per coordinare l'attesa dei thread che richiedono lotti.
    inner: Arc<(Mutex<Vec<T>>, Condvar)>,
    /// Capacità totale costante del pool.
    capacity: usize,
}

impl<T: Send + Sync> MyBatchPool<T> {
    /// Inizializza il pool con l'insieme iniziale di risorse.
    pub fn new(items: Vec<T>) -> Self {
        Self {
            capacity: items.len(),
            inner: Arc::new((Mutex::new(items), Condvar::new())),
        }
    }
}

impl<T: Send + Sync + 'static> BatchPool<T> for MyBatchPool<T> {
    /// Restituisce la capacità massima totale del pool.
    fn capacity(&self) -> usize {
        self.capacity
    }

    /// Acquisisce un lotto di `count` risorse in modo indivisibile.
    ///
    /// ## Immunità dai Deadlock:
    /// Poiché le risorse sono **completamente anonime ed intercambiabili** (a differenza di `MultiResourceManager`),
    /// non esiste competizione per identità specifiche: un thread deve semplicemente attendere che
    /// `guard.len() >= count`. Una singola condizione sotto lock è priva di qualsiasi rischio di stallo.
    fn acquire_batch(&self, count: usize) -> impl ResourceBatch<T> + 'static {
        if count > self.capacity {
            panic!("La quantità richiesta ({}) supera la capacità totale ({})", count, self.capacity);
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa non attiva finché non sono disponibili almeno `count` elementi
        guard = cvar.wait_while(guard, |c| c.len() < count).unwrap();

        // Estrazione rapida di `count` elementi dal fondo del vettore
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(guard.pop().unwrap());
        }

        MyBatch {
            shared_state: Arc::clone(&self.inner),
            items,
        }
    }

    /// Variante con timeout: tenta di acquisire `count` elementi entro il tempo limite specificato.
    fn acquire_batch_timeout(&self, count: usize, timeout: Duration) -> Option<impl ResourceBatch<T> + 'static> {
        if count > self.capacity {
            panic!("La quantità richiesta ({}) supera la capacità totale ({})", count, self.capacity);
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa limitata nel tempo con wait_timeout_while
        (guard, _) = cvar
            .wait_timeout_while(guard, timeout, |c| c.len() < count)
            .unwrap();

        // Se la quantità richiesta è disponibile, estrae il lotto; altrimenti ritorna None
        if guard.len() >= count {
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(guard.pop().unwrap());
            }

            Some(MyBatch {
                shared_state: Arc::clone(&self.inner),
                items,
            })
        } else {
            None
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `BatchPool`.
pub fn make_batch_pool<T: Send + Sync + 'static>(items: Vec<T>) -> impl BatchPool<T> {
    MyBatchPool::new(items)
}
