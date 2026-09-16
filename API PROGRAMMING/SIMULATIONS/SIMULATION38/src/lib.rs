//! # Simulazione 038 — HierarchicalPool (capstone)
//!
//! Un pool di risorse condiviso da più worker paga un costo di contesa ogni volta che un worker deve attendere
//! il lock usato da tutti gli altri, anche quando la maggior parte delle richieste potrebbe essere soddisfatta localmente.
//! Dare a ciascun worker una piccola riserva propria, rifornita da un pool condiviso di riserva solo quando necessario,
//! riduce drasticamente quella contesa — a patto che le riserve locali non crescano senza limite, altrimenti gli elementi
//! restano intrappolati presso un worker inattivo mentre altri ne restano privi.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Resource<T>`, `LocalPool<T>` e `HierarchicalPool<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait Resource<T: Send> {
//!     fn get(&self) -> &T;
//! }
//!
//! pub trait LocalPool<T: Send> {
//!     fn acquire(&self) -> impl Resource<T>;
//! }
//!
//! pub trait HierarchicalPool<T: Send>: Clone + Send + Sync {
//!     fn local(&self, worker_id: usize) -> impl LocalPool<T>;
//!     fn total_capacity(&self) -> usize;
//! }
//!
//! pub fn make_hierarchical_pool<T: Send + 'static>(
//!     items: Vec<T>,
//!     worker_count: usize,
//!     local_capacity: usize,
//! ) -> impl HierarchicalPool<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Alla creazione, tutti gli elementi forniti iniziano nel pool condiviso di overflow; le riserve locali partono vuote.
//! - Quando un `Resource<T>` ottenuto tramite `local(worker_id).acquire()` esce dallo scope (RAII, `Drop`):
//!   se la riserva locale di `worker_id` ha in quel momento **meno di** `local_capacity` elementi, l'elemento vi ritorna;
//!   altrimenti va nel pool condiviso di overflow.
//! - Un elemento che ritorna nella riserva locale di `worker_id` (perché sotto `local_capacity`) non deve diventare visibile
//!   né disponibile per `acquire()` chiamato su un pool locale diverso, né deve necessariamente sbloccare un'attesa in corso
//!   sul pool condiviso di overflow.
//! - L'accesso alla riserva locale di un worker non deve mai contendere, né essere bloccato da, un'operazione in corso
//!   sulla riserva locale di un worker diverso — solo l'accesso al pool condiviso di overflow è un punto di possibile contesa comune.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::sync::{Arc, Condvar, Mutex};

/// Trait che rappresenta l'handle per una risorsa prelevata dal pool.
pub trait Resource<T: Send> {
    /// Restituisce un riferimento immutabile all'elemento protetto.
    fn get(&self) -> &T;
}

/// Trait che rappresenta il pool locale associato a uno specifico worker.
pub trait LocalPool<T: Send> {
    /// Preleva un elemento: dalla riserva locale del worker se non vuota;
    /// altrimenti dal pool condiviso di overflow, bloccando il chiamante se anch'esso è vuoto.
    fn acquire(&self) -> impl Resource<T>;
}

/// Trait che rappresenta il pool gerarchico a due livelli (locale + condiviso).
pub trait HierarchicalPool<T: Send>: Clone + Send + Sync {
    /// Restituisce il pool locale associato al worker identificato da `worker_id` (`0..worker_count`).
    fn local(&self, worker_id: usize) -> impl LocalPool<T>;

    /// Restituisce il numero totale di elementi gestiti (invariante nel tempo).
    fn total_capacity(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyResource<T: Send> {
    value: Option<T>,
    worker_id: usize,
    shared_manager: MyPoolManager<T>
}

impl<T: Send> Resource<T> for MyResource<T> {
    fn get(&self) -> &T {
        self.value.as_ref().unwrap()
    }
}

impl<T: Send> Drop for MyResource<T> {
    fn drop(&mut self) {
        let mutex = &*self.shared_manager.local_pools[self.worker_id];
        let mut guard = mutex.lock().unwrap();

        let value = self.value.take().unwrap();
        if guard.len() < self.shared_manager.local_capacity {
            guard.push(value);
            drop(guard);
        }
        else {
            let (manager_mutex, manager_cvar) = &*self.shared_manager.shared_pool;
            let mut manager_guard = manager_mutex.lock().unwrap();
            manager_guard.push(value);
            drop(manager_guard);
            manager_cvar.notify_one();
        }
    }
}

pub struct MyLocalPool<T: Send> {
    pool_id: usize,
    shared_manager: MyPoolManager<T>
}

impl<T: Send> MyLocalPool<T> {
    pub fn local_capacity (&self) -> usize {
        let target_pool_mutex = &*self.shared_manager.local_pools[self.pool_id];
        let pool_guard = target_pool_mutex.lock().unwrap();
        pool_guard.len()
    }
}

impl<T: Send> LocalPool<T> for MyLocalPool<T> {
    fn acquire(&self) -> impl Resource<T> {
        let target_pool_mutex = &*self.shared_manager.local_pools[self.pool_id];
        let mut pool_guard = target_pool_mutex.lock().unwrap();

        if let Some(value) = pool_guard.pop() {
            drop(pool_guard);
            MyResource {
                value: Some(value),
                worker_id: self.pool_id,
                shared_manager: self.shared_manager.clone()
            }
        }
        else {
            let (manager_mutex, manager_cvar) = &*self.shared_manager.shared_pool;
            let mut manager_guard = manager_mutex.lock().unwrap();
            manager_guard = manager_cvar.wait_while(manager_guard, |c| {
                c.is_empty()
            }).unwrap();

            let value = manager_guard.pop();
            MyResource {
                value,
                worker_id: self.pool_id,
                shared_manager: self.shared_manager.clone()
            }
        }
    }
}

impl<T: Send> Clone for MyLocalPool<T> {
    fn clone(&self) -> Self {
        Self {
            pool_id: self.pool_id,
            shared_manager: self.shared_manager.clone()
        }
    }
}


pub struct MyPoolManager<T: Send> {
    shared_pool: Arc<(Mutex<Vec<T>>, Condvar)>,
    local_pools: Vec<Arc<Mutex<Vec<T>>>>,
    local_capacity: usize,
    total_capacity: usize
}

impl<T: Send> HierarchicalPool<T> for MyPoolManager<T> {
    fn local(&self, worker_id: usize) -> impl LocalPool<T> {
        if worker_id >= self.local_pools.len() {
            panic!()
        }

        MyLocalPool {
            pool_id: worker_id,
            shared_manager: self.clone()
        }
    }

    fn total_capacity(&self) -> usize {
        self.total_capacity
    }
}

impl<T: Send> Clone for MyPoolManager<T> {
    fn clone(&self) -> Self {
        let mut local_pools = Vec::new();
        for pool in self.local_pools.iter() {
            local_pools.push(pool.clone());
        }

        Self {
            shared_pool: self.shared_pool.clone(),
            local_pools,
            local_capacity: self.local_capacity,
            total_capacity: self.total_capacity
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `HierarchicalPool`.
pub fn make_hierarchical_pool<T: Send + 'static>(
    _items: Vec<T>,
    _worker_count: usize,
    _local_capacity: usize,
) -> impl HierarchicalPool<T> {
    let mut local_pools = Vec::new();
    for _ in 0.._worker_count {
        local_pools.push(Arc::new(Mutex::new(Vec::new())));
    }

    MyPoolManager {
        total_capacity: _items.len(),
        shared_pool: Arc::new((Mutex::new(_items), Condvar::new())),
        local_pools,
        local_capacity: _local_capacity
    }
}
