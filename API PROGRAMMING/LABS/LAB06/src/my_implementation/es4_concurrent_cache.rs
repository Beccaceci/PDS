#![allow(dead_code)]
//! Esercizio 4 - Cache concorrente con eviction in background
//!
//! Implementare una cache thread-safe in cui le voci scadute sono rimosse
//! automaticamente da un thread di pulizia in background, e in cui la
//! distruzione dell'istanza termina rapidamente il thread di pulizia
//! senza attendere la scadenza del prossimo intervallo.
//!
//! Vedi il file `Lab6.md` per la specifica completa.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};


/// Tratto generico per una cache concorrente con scadenza automatica delle voci.
pub trait ConcurrentCache<V> {
    /// Crea una nuova cache con la durata di validità specificata.
    fn new(ttl: Duration) -> Self
    where
        Self: Sized;

    /// Restituisce il valore associato a `key` se presente e non scaduto.
    fn get(&self, key: &str) -> Option<Arc<V>>;

    /// Inserisce o sovrascrive la voce associata a `key`.
    fn set(&self, key: &str, value: V);
}

/// The internal data stored for each cache entry
pub struct Entry<V> {
    pub value: Arc<V>,
    pub timestamp: Instant
}
impl<V> Entry<V> {
    pub fn new(_value: V) -> Self {
        Self {
            value: Arc::new(_value),
            timestamp: Instant::now()
        }
    }
}

/// The state shared between the Cache handle and the background cleanup thread
pub struct CacheState<V> {
    /// A map of keys to entries and a boolean flag 'is_running'
    entries: Mutex<(HashMap<String, Entry<V>>, bool)>,
    cv: Condvar
}
impl<V> CacheState<V> {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new((HashMap::new(), true)),
            cv: Condvar::new()
        }
    }
}

/// A thread-safe cache where expired entries are removed automatically
pub struct ConcurrentCacheImpl<V> {
    inner: Arc<CacheState<V>>,
    ttl: Duration,
    cleanup_thread: Option<thread::JoinHandle<()>>
}

impl<V> ConcurrentCache<V> for ConcurrentCacheImpl<V> where V: Send + Sync + 'static, {
    /// Initializes a new empty cache with a specified Time-To-Live (TTL)
    fn new(_ttl: Duration) -> Self {
        // Creates a new cache and spawns the background cleanup thread
        let state = Arc::new(CacheState::new());
        let thread_state = state.clone();
        let thread_ttl = _ttl;

        // Spawn the cleanup thread
        let cleanup_thread = thread::spawn(move || {
            let mut lock = thread_state.entries.lock().unwrap();

            // Iterate until the cache is not dropped
            while lock.1 {
                // Wait until the next TTL interval or a shutdown signal
                let (new_lock, wait_result) = thread_state.cv.wait_timeout(lock, thread_ttl).unwrap();
                lock = new_lock;

                // If shutdown was signaled, exit the loop immediately
                if !lock.1 {
                    break;
                }

                if wait_result.timed_out() {
                    // Identify expired keys while holding the lock briefly
                    let now = Instant::now();
                    let expired_entries:Vec<String> = lock.0.iter()
                                                            .filter(|(_, entry)| now.duration_since(entry.timestamp) > thread_ttl)
                                                            .map(|(k, _)| k.clone())
                                                            .collect();

                    // Remove the identified keys
                    for key in expired_entries {
                        lock.0.remove(&key);
                    }
                }
            }
        });

        Self {
            inner: state,
            ttl: _ttl,
            cleanup_thread: Some(cleanup_thread),
        }
    }


    /// Retrieves an item if it exists and hasn't expired
    fn get(&self, _key: &str) -> Option<Arc<V>> {
        let mut entries = self.inner.entries.lock().unwrap();
        if let Some(entry) = entries.0.get_mut(_key) {
            if entry.timestamp.elapsed() > self.ttl {
                entries.0.remove(_key);
                None
            }
            else {
                // entry.timestamp = Instant::now();
                Some(Arc::clone(&entry.value))
            }
        }
        else {
            None
        }

    }

    /// Inserts or updates an entry in the cache
    fn set(&self, _key: &str, _value: V) {
        let mut entries = self.inner.entries.lock().unwrap();
        entries.0.insert(_key.to_string(), Entry::new(_value));
    }
}

impl<V> Drop for ConcurrentCacheImpl<V> {
    /// Signals the cleanup thread to stop and joins it
    fn drop(&mut self) {
        // Use a block to ensure the MutexGuard is dropped before trying to join the thread
        {
            let mut entries = self.inner.entries.lock().unwrap();
            entries.1 = false;
            self.inner.cv.notify_all();
        }

        if let Some(handle) = self.cleanup_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn test_basic_storage_and_retrieval() {
        let cache = ConcurrentCacheImpl::<String>::new(Duration::from_secs(10));

        // Inserimento e lettura.
        cache.set("key1", "value1".to_string());
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("value1")
        );

        // Lettura di una chiave inesistente.
        assert!(cache.get("non_existent_key").is_none());

        // Sovrascrittura.
        cache.set("key1", "new_value".to_string());
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("new_value")
        );
    }

    #[test]
    fn test_expiration() {
        // TTL molto breve.
        let cache = ConcurrentCacheImpl::<String>::new(Duration::from_millis(100));

        cache.set("key1", "value1".to_string());

        // Subito dopo l'inserimento la chiave è ancora disponibile.
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("value1")
        );

        // Attesa oltre il TTL.
        thread::sleep(Duration::from_millis(150));

        // La chiave deve essere considerata scaduta.
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_background_cleanup() {
        // Le voci scadute devono essere rimosse anche senza alcuna chiamata a get.
        let cache = ConcurrentCacheImpl::<String>::new(Duration::from_millis(50));

        for i in 0..5 {
            cache.set(&format!("key{i}"), format!("value{i}"));
        }
        // Verifica iniziale.
        for i in 0..5 {
            assert_eq!(
                cache.get(&format!("key{i}")).as_deref().map(|s| s.as_str()),
                Some(format!("value{i}").as_str())
            );
        }

        // Attesa abbastanza lunga da permettere al thread di background
        // di passare ed eliminare tutte le voci.
        thread::sleep(Duration::from_millis(300));

        for i in 0..5 {
            assert!(cache.get(&format!("key{i}")).is_none());
        }
    }

    #[test]
    fn test_different_expiration_times() {
        let cache = ConcurrentCacheImpl::<String>::new(Duration::from_millis(200));

        // Voce inserita "presto": scadrà per prima.
        cache.set("short_lived", "value1".to_string());

        thread::sleep(Duration::from_millis(100));

        // Voce inserita "tardi": scadrà per ultima.
        cache.set("long_lived", "value2".to_string());

        // Aspettiamo finché la prima non è scaduta ma la seconda non ancora.
        thread::sleep(Duration::from_millis(150));

        assert!(cache.get("short_lived").is_none());
        assert_eq!(
            cache.get("long_lived").as_deref().map(|s| s.as_str()),
            Some("value2")
        );
    }

    #[test]
    fn test_concurrent_access() {
        let cache = Arc::new(ConcurrentCacheImpl::<String>::new(Duration::from_secs(5)));

        // Scrittori concorrenti.
        let mut handles = vec![];
        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                cache.set(&format!("thread_key{i}"), format!("thread_value{i}"));
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Tutte le voci scritte devono essere recuperabili.
        for i in 0..10 {
            assert_eq!(
                cache
                    .get(&format!("thread_key{i}"))
                    .as_deref()
                    .map(|s| s.as_str()),
                Some(format!("thread_value{i}").as_str())
            );
        }
    }

    #[test]
    fn test_memory_cleanup() {
        // Verifica che il thread di background sia correttamente terminato
        // alla distruzione della cache. Se Drop non joinasse il thread, questo
        // test potrebbe bloccarsi indefinitamente o lasciare risorse appese.
        {
            let _cache = ConcurrentCacheImpl::<String>::new(Duration::from_millis(10));
            // _cache esce di scope qui.
        }
        // Se siamo arrivati fino a qui, il test è passato.
    }

    #[test]
    fn test_fast_cleanup() {
        // La distruzione deve essere rapida anche con TTL grandi:
        // il thread di background non deve attendere la scadenza
        // del prossimo intervallo di pulizia per terminare.
        let t1 = Instant::now();
        {
            let _cache = ConcurrentCacheImpl::<String>::new(Duration::from_secs(3));
            thread::sleep(Duration::from_millis(10));
        }
        let t2 = Instant::now();
        assert!(
            t2 - t1 < Duration::from_secs(1),
            "Drop ha impiegato troppo tempo: {:?}",
            t2 - t1
        );
    }
}
