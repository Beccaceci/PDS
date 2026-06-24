#![allow(dead_code)]
//! Exercise 6 - Shared Cache with Expiration Policy
//!
//! Implementation of a generic `Cache<V>` that maps `String` keys to values of type `V`.
//! Features include TTL-based expiration and thread-safe access using `RwLock`.[cite: 9]

use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Represents a single cache entry with its metadata.[cite: 9]
pub struct Entry<V> {
    pub key: String,
    pub value: V,
    pub timestamp: Instant,
}

impl<V> Entry<V> {
    /// Creates a new entry with the current timestamp.[cite: 9]
    pub fn new(_key: String, _value: V) -> Self {
        Self {
            key: _key,
            value: _value,
            timestamp: Instant::now(),
        }
    }
}

/// Thread-safe cache using RwLock for concurrent access.[cite: 9]
pub struct Cache<V> {
    entries: RwLock<Vec<Entry<V>>>,
    ttl: Duration,
}

impl<V> Cache<V>
where
    V: Clone,
{
    /// Initializes a new empty cache with a specified Time-To-Live (TTL).[cite: 9]
    pub fn new(_ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            ttl: _ttl,
        }
    }

    /// Inserts a new value or updates an existing one.
    /// If the key exists, it updates the value and resets the timestamp to "now".[cite: 9]
    pub fn insert(&self, _key: String, _value: V) {
        // Acquire write lock to modify the internal vector.[cite: 9]
        let mut entries = self.entries.write().unwrap();

        // If the key is found, update its contents; otherwise, add a new Entry.[cite: 9]
        if let Some(entry) = entries.iter_mut().find(|e| e.key == _key) {
            entry.value = _value;
            entry.timestamp = Instant::now();
        } else {
            entries.push(Entry::new(_key, _value));
        }
    }

    /// Retrieves a value if it exists and hasn't expired.
    /// Performs "lazy eviction" by removing the entry if it is found to be expired.[cite: 9]
    pub fn get(&self, _key: &str) -> Option<V> {
        // Acquire write lock because lazy eviction requires modifying the vector.[cite: 9]
        let mut entries = self.entries.write().unwrap();

        // Locate the index of the key.[cite: 9]
        if let Some(index) = entries.iter().position(|e| e.key == _key) {
            // Check if the time elapsed since insertion exceeds the TTL.[cite: 9]
            if entries[index].timestamp.elapsed() > self.ttl {
                // Remove expired entry and return None.[cite: 9]
                entries.remove(index);
                None
            } else {
                // Return a clone of the valid value.[cite: 9]
                Some(entries[index].value.clone())
            }
        } else {
            None
        }
    }

    /// Returns the total number of entries, including those that might be expired.[cite: 9]
    pub fn len(&self) -> usize {
        // Acquire read lock for inspection.[cite: 9]
        let entries = self.entries.read().unwrap();
        entries.len()
    }

    /// Returns true if the cache contains no entries.[cite: 9]
    pub fn is_empty(&self) -> bool {
        // Acquire read lock for inspection.[cite: 9]
        let entries = self.entries.read().unwrap();
        entries.is_empty()
    }

    /// Explicitly removes all expired entries from the cache.
    /// Returns the count of removed items.[cite: 9]
    pub fn purge_expired(&self) -> usize {
        // Acquire write lock to perform batch removal.[cite: 9]
        let mut entries = self.entries.write().unwrap();
        let initial_count = entries.len();

        // Retain only items that have not yet reached their TTL.[cite: 9]
        entries.retain(|e| e.timestamp.elapsed() < self.ttl);

        initial_count - entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn insert_e_get() {
        let c: Cache<String> = Cache::new(Duration::from_secs(60));
        c.insert("a".to_string(), "uno".to_string());
        c.insert("b".to_string(), "due".to_string());
        assert_eq!(c.get("a"), Some("uno".to_string()));
        assert_eq!(c.get("b"), Some("due".to_string()));
        assert_eq!(c.get("c"), None);
    }

    #[test]
    fn insert_sovrascrive() {
        let c: Cache<i32> = Cache::new(Duration::from_secs(60));
        c.insert("k".to_string(), 1);
        c.insert("k".to_string(), 2);
        assert_eq!(c.get("k"), Some(2));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn voce_scaduta_non_visibile() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(50));
        c.insert("x".to_string(), 42);
        thread::sleep(Duration::from_millis(120));
        assert_eq!(c.get("x"), None);
    }

    #[test]
    fn lazy_eviction_su_get() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(50));
        c.insert("x".to_string(), 1);
        assert_eq!(c.len(), 1);
        thread::sleep(Duration::from_millis(120));
        assert_eq!(c.get("x"), None);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn purge_expired_rimuove_solo_scadute() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(80));
        c.insert("vecchia".to_string(), 1);
        thread::sleep(Duration::from_millis(120));
        c.insert("nuova".to_string(), 2);
        let rimosse = c.purge_expired();
        assert_eq!(rimosse, 1);
        assert_eq!(c.len(), 1);
        assert_eq!(c.get("nuova"), Some(2));
        assert_eq!(c.get("vecchia"), None);
    }

    #[test]
    fn uso_concorrente() {
        let c: Arc<Cache<i32>> = Arc::new(Cache::new(Duration::from_secs(5)));
        let mut handles = vec![];

        for t in 0..4 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    c.insert(format!("t{}_k{}", t, i), i);
                }
            }));
        }
        for _ in 0..4 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = c.get("t0_k0");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(c.len(), 400);
    }

    #[test]
    fn get_su_chiave_inesistente() {
        let c: Cache<i32> = Cache::new(Duration::from_secs(60));
        assert_eq!(c.get("non_esiste"), None);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn cache_appena_creata_e_vuota() {
        let c: Cache<i32> = Cache::new(Duration::from_secs(60));
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());
    }

    #[test]
    fn purge_expired_su_cache_vuota() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(10));
        assert_eq!(c.purge_expired(), 0);
    }

    #[test]
    fn purge_expired_senza_voci_scadute() {
        let c: Cache<i32> = Cache::new(Duration::from_secs(60));
        c.insert("a".to_string(), 1);
        c.insert("b".to_string(), 2);
        assert_eq!(c.purge_expired(), 0);
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn purge_expired_rimuove_tutte_le_voci_scadute() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(50));
        c.insert("a".to_string(), 1);
        c.insert("b".to_string(), 2);
        c.insert("c".to_string(), 3);
        thread::sleep(Duration::from_millis(120));
        assert_eq!(c.purge_expired(), 3);
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn insert_aggiorna_listante_di_inserimento() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(50));
        c.insert("k".to_string(), 1);
        thread::sleep(Duration::from_millis(120));
        c.insert("k".to_string(), 2);
        assert_eq!(c.get("k"), Some(2));
    }

    #[test]
    fn voci_diverse_scadenze_indipendenti() {
        let c: Cache<i32> = Cache::new(Duration::from_millis(80));
        c.insert("vecchia".to_string(), 1);
        thread::sleep(Duration::from_millis(60));
        c.insert("recente".to_string(), 2);
        assert_eq!(c.get("vecchia"), Some(1));
        assert_eq!(c.get("recente"), Some(2));
        thread::sleep(Duration::from_millis(50));
        assert_eq!(c.get("vecchia"), None);
        assert_eq!(c.get("recente"), Some(2));
    }

    #[test]
    fn cache_generica_con_tipi_diversi() {
        let c: Cache<Vec<u8>> = Cache::new(Duration::from_secs(60));
        c.insert("payload".to_string(), vec![1, 2, 3]);
        assert_eq!(c.get("payload"), Some(vec![1, 2, 3]));

        #[derive(Clone, Debug, PartialEq)]
        struct Persona {
            nome: String,
            eta: u32,
        }
        let c2: Cache<Persona> = Cache::new(Duration::from_secs(60));
        let p = Persona { nome: "Mario".to_string(), eta: 30 };
        c2.insert("user".to_string(), p.clone());
        assert_eq!(c2.get("user"), Some(p));
    }

    #[test]
    fn lettori_concorrenti_multipli() {
        let c: Arc<Cache<i32>> = Arc::new(Cache::new(Duration::from_secs(60)));
        for i in 0..50 {
            c.insert(format!("k{}", i), i);
        }
        let mut handles = vec![];
        for _ in 0..16 {
            let c = Arc::clone(&c);
            handles.push(thread::spawn(move || {
                let mut hits = 0;
                for i in 0..50 {
                    if c.get(&format!("k{}", i)).is_some() {
                        hits += 1;
                    }
                }
                hits
            }));
        }
        for h in handles {
            assert_eq!(h.join().unwrap(), 50);
        }
        assert_eq!(c.len(), 50);
    }
}