//! # Simulazione 034 — MvccStore
//!
//! Un archivio letto molto più spesso di quanto venga scritto trae vantaggio dal non far mai attendere i lettori —
//! nemmeno per la durata di una scrittura — dando a ciascun lettore una fotografia coerente dell'intero stato in un istante preciso,
//! mentre le scritture continuano a creare nuove versioni senza toccare quelle già osservate da uno snapshot in corso.
//! Le versioni superate, però, non sono spazzatura immediata: vanno conservate finché anche un solo snapshot ancora vivo
//! potrebbe ancora averne bisogno.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Snapshot<K, V>` e `MvccStore<K, V>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! use std::hash::Hash;
//!
//! pub trait Snapshot<K, V: Clone + Send>: Send + Sync {
//!     fn get(&self, key: &K) -> Option<V>;
//! }
//!
//! pub trait MvccStore<K: Eq + Hash + Clone + Send + Sync, V: Clone + Send + Sync>: Clone + Send + Sync {
//!     fn write(&self, key: K, value: V);
//!     fn snapshot(&self) -> impl Snapshot<K, V> + 'static;
//!     fn version_count(&self, key: &K) -> usize;
//! }
//!
//! pub fn make_mvcc_store<K: Eq + Hash + Clone + Send + Sync + 'static, V: Clone + Send + Sync + 'static>() -> impl MvccStore<K, V> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Ogni `get()` su uno `Snapshot` restituisce, per l'intera vita di quello snapshot, esattamente il valore che la chiave aveva nell'istante in cui `snapshot()` è stato chiamato.
//! - Una versione di una chiave può essere effettivamente rimossa dalla memoria solo quando **nessuno** snapshot ancora vivo potrebbe più averne bisogno.
//! - Quando l'ultimo snapshot che rendeva necessaria una versione superata esce dallo scope ([`Drop`]), quella versione viene rimossa (osservabile tramite una diminuzione di `version_count`).
//! - `write()` e `snapshot()` non devono mai bloccare il chiamante.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::HashMap, hash::Hash, sync::{Arc, RwLock}};

/// Trait che rappresenta una fotografia immutabile e coerente dell'intero store a un istante logico preciso.
pub trait Snapshot<K, V: Clone + Send>: Send + Sync {
    /// Legge il valore associato a `key` così come appariva nell'istante di cattura dello snapshot.
    /// Restituisce `None` se la chiave non esisteva ancora a quel tempo logico.
    fn get(&self, key: &K) -> Option<V>;
}

/// Trait che rappresenta il database multi-versione (MVCC).
pub trait MvccStore<K: Eq + Hash + Clone + Send + Sync, V: Clone + Send + Sync>: Clone + Send + Sync {
    /// Scrive un nuovo valore per `key`, creando una nuova versione logica senza bloccare i lettori.
    fn write(&self, key: K, value: V);

    /// Cattura uno snapshot coerente dell'intero store all'istante logico corrente. Non blocca.
    fn snapshot(&self) -> impl Snapshot<K, V> + 'static;

    /// Restituisce il numero di versioni fisicamente conservate in memoria per `key`.
    fn version_count(&self, key: &K) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================


pub struct MySnapshot<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    snap: Arc<HashMap<K, V>>
}

impl<K, V> Snapshot<K, V> for MySnapshot<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    fn get(&self, key: &K) -> Option<V> {
        if let Some(value) = self.snap.get(key) {
            Some(value.clone())
        }
        else {
            None
        }
    }
}


pub struct MyMvccStore<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    map: Arc<RwLock<HashMap<K, (V, usize)>>>
}

impl<K, V> MyMvccStore<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    pub fn new () -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new()))
        }
    }
}

impl<K, V> MvccStore<K, V> for MyMvccStore<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    fn write(&self, key: K, value: V) {
        let mut guard = self.map.write().unwrap();
        if let Some((old_value, version)) = guard.get_mut(&key) {
            *old_value = value;
            *version += 1;
        }
        else {
            guard.insert(key, (value, 1));
        }
    }

    fn snapshot(&self) -> impl Snapshot<K, V> + 'static {
        let guard = self.map.read().unwrap();
        
        let mut copy_hashmap = HashMap::new();
        for (key, (value, _)) in guard.iter() {
            copy_hashmap.insert(key.clone(), value.clone());
        }

        MySnapshot {
            snap: Arc::new(copy_hashmap)
        }
    }

    fn version_count(&self, key: &K) -> usize {
        let guard = self.map.read().unwrap();
        let (_, version) = guard.get(key).unwrap();
        *version
    }
}

impl<K, V> Clone for MyMvccStore<K, V> where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `MvccStore`.
pub fn make_mvcc_store<
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
>() -> impl MvccStore<K, V> {
    MyMvccStore::new()
}
