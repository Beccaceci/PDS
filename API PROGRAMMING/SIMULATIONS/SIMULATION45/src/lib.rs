//! # Simulazione 045 — ShardedCache (capstone)
//!
//! Cache concorrente partizionata a shard indipendenti[cite: 20].
//! Fornisce isolamento della contesa per operazioni ordinarie (`get`, `put`) tra shard distinti[cite: 20],
//! e coordinazione globale "stop-the-world" non attiva (`RwLock` / coordinatore) durante
//! la ridistribuzione dinamica della mappa (`rehash`)[cite: 20].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile (`Clone + Send + Sync`)[cite: 20].
//! - Operazioni `get`/`put` su chiavi instradate a shard diversi non contendono alcun lock[cite: 20].
//! - `rehash()` ridistribuisce atomicamente tutte le voci secondo il nuovo numero di shard,
//!   escludendo `get`/`put` in corso senza perdite né duplicati[cite: 20].
//! - Nessuna attesa attiva durante `rehash()` o in attesa di esso[cite: 20].

use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}, sync::{Arc, Condvar, Mutex}};

/// Tratto che definisce una cache partizionata con ridimensionamento dinamico[cite: 20].
pub trait ShardedCache<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone + Send + Sync {
    /// Restituisce il valore associato a `key` se presente, bloccando se un `rehash` è in corso[cite: 20].
    fn get(&self, key: &K) -> Option<V>;

    /// Inserisce o aggiorna la coppia (chiave, valore) nello shard competente[cite: 20].
    fn put(&self, key: K, value: V);

    /// Restituisce il numero di shard attualmente attivi[cite: 20].
    fn shard_count(&self) -> usize;

    /// Ridistribuisce tutte le voci esistenti secondo il nuovo conteggio di shard[cite: 20].
    fn rehash(&self, new_shard_count: usize);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================


pub struct MyShard<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    map: HashMap<K, V>,
    num_keys: usize
}

pub struct CacheState<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    shards: Vec<Arc<Mutex<MyShard<K, V>>>>,
    actual_rehash: bool
}

impl<K, V> CacheState<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    pub fn new () -> Self {
        Self {
            shards: Vec::new(),
            actual_rehash: false
        }
    }
}

pub fn get_shard_index<K: Hash>(key: &K, num_shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_shards
}

pub struct MyShardedCache<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    inner: Arc<(Mutex<CacheState<K, V>>, Condvar)>
}

impl<K, V> MyShardedCache<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(CacheState::new()), Condvar::new()))
        }
    }

    pub fn with_initial_shards (_shards_count: usize) -> Self {
        let mut shards = Vec::new();
        for _ in 0.._shards_count {
            shards.push(Arc::new(Mutex::new(MyShard {
                map: HashMap::new(),
                num_keys: 0
            })));
        }

        Self {
            inner: Arc::new((Mutex::new(CacheState {
                shards,
                actual_rehash: false
            }), Condvar::new()))
        }
    }
}

impl<K, V> ShardedCache<K, V> for MyShardedCache<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    fn get(&self, key: &K) -> Option<V> {
        let (cache_mutex, cache_cvar) = &*self.inner;
        let mut cache_guard = cache_mutex.lock().unwrap();
        cache_guard = cache_cvar.wait_while(cache_guard, |c| {
            c.actual_rehash
        }).unwrap();

        let index = get_shard_index(&key, cache_guard.shards.len());
        let cloned_mutex_shard = cache_guard.shards[index].clone();
        drop(cache_guard);

        let guard_shard = cloned_mutex_shard.lock().unwrap();
        if let Some(value) = guard_shard.map.get(key) {
            Some(value.clone())
        }
        else {
            None
        }
    }

    fn put(&self, key: K, value: V) {
        let (cache_mutex, cache_cvar) = &*self.inner;
        let mut cache_guard = cache_mutex.lock().unwrap();
        cache_guard = cache_cvar.wait_while(cache_guard, |c| {
            c.actual_rehash
        }).unwrap();

        let index = get_shard_index(&key, cache_guard.shards.len());
        let cloned_mutex_shard = cache_guard.shards[index].clone();
        drop(cache_guard);

        let mut guard_shard = cloned_mutex_shard.lock().unwrap();

        if !guard_shard.map.contains_key(&key) {
            guard_shard.num_keys += 1;
        }

        guard_shard.map.insert(key, value);
    }

    fn rehash(&self, new_shard_count: usize) {
        let (cache_mutex, cache_cvar) = &*self.inner;
        let mut cache_guard = cache_mutex.lock().unwrap();
        cache_guard.actual_rehash = true;

        let mut wrapped = Vec::new();
        let _ = cache_guard.shards.iter()
            .for_each(|shard| {
                wrapped.push(shard.clone());
            });
        drop(cache_guard);
        

        let mut entries = Vec::new();
        let _ = wrapped.iter()
                        .for_each(|mutex_shard| {
                            let guard_shard = mutex_shard.lock().unwrap();
                            for (key, value) in guard_shard.map.iter() {
                                entries.push((key.clone(), value.clone()));
                            }
                        });

        let mut shards = Vec::new();
        for _ in 0..new_shard_count {
            shards.push(Arc::new(Mutex::new(MyShard {
                map: HashMap::<K, V>::new(),
                num_keys: 0
            })));
        }

        let mut cache_guard = cache_mutex.lock().unwrap();
        cache_guard.shards = shards;

        for (key, value) in entries {
            let index = get_shard_index(&key, new_shard_count);
            let shard_mutex = cache_guard.shards[index].clone();
            let mut shard_guard = shard_mutex.lock().unwrap();
            shard_guard.map.insert(key, value);
            shard_guard.num_keys += 1;
        }
        
        cache_guard.actual_rehash = false;
        drop(cache_guard);
        cache_cvar.notify_all();
    }

    fn shard_count(&self) -> usize {
        let (cache_mutex, _) = &*self.inner;
        let cache_guard = cache_mutex.lock().unwrap();
        cache_guard.shards.len()
    }
}

impl<K, V> Clone for MyShardedCache<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Clone + Send + 'static
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova `ShardedCache` con `initial_shard_count` partizioni[cite: 20].
pub fn make_sharded_cache<K: Eq + Hash + Clone + Send + 'static, V: Clone + Send + 'static>(
    _initial_shard_count: usize,
) -> impl ShardedCache<K, V> {
    MyShardedCache::with_initial_shards(_initial_shard_count)
}