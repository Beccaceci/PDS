mod concurrent_cache {
    use std::collections::HashMap;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::thread::{JoinHandle};
    use std::time::{Duration, Instant};

    /**
     * ConcurrentCache — A trait defining a thread-safe cache with automatic expiration of entries.
     * 
     * This trait defines the interface for a key-value store where entries automatically expire after
     * a specified duration. Implementations should maintain thread safety through synchronization primitives
     * and run a background thread to periodically clean up expired entries.
     * 
     * # Features
     * 
     * - Thread-safe access to cached values
     * - Automatic expiration of entries after a specified duration
     * - Background cleanup of expired entries
     * - Proper resource management when the cache is dropped
     * 
     * # Thread Safety
     * 
     * Implementations should be designed to be safely shared across multiple threads.
     * They should use synchronization primitives to ensure thread-safe
     * access to the cached data. Multiple threads should be able to concurrently call `get` and `set`
     * methods without causing data races or other concurrency issues.
     * 
     * # Memory Management
     * 
     * Implementations should automatically manage memory by:
     * 
     * 1. Running a background thread that periodically removes expired entries
     * 2. Properly cleaning up resources when the cache is dropped
     * 
     * This ensures that memory usage doesn't grow unbounded even with frequent
     * cache operations and that no resources are leaked when the cache is no longer needed.
     */
    pub trait ConcurrentCache {
        /// Creates a new cache with the specified expiration duration.
        ///
        /// # Parameters
        ///
        /// * `d` - The duration for which cache entries remain valid. When a value
        ///   is added to the cache, it will expire after this duration has elapsed.
        ///
        /// # Returns
        /// A new instance of the implementing type.
        ///
        fn new(d: Duration) -> Self where Self: Sized;

        /// Retrieves a value from the cache if it exists and hasn't expired.
        ///
        /// # Parameters
        /// * `key` - The key to look up in the cache.
        /// # Returns
        /// * `Some(Arc<String>)` - If the key exists and the value hasn't expired.
        /// * `None` - If the key doesn't exist, or the value has expired.
        ///
        /// Notes
        /// Returning `Arc<String>` allows inexpensive cloning and sharing of the value across threads
        /// without reallocating or copying the underlying string data, minimizing memory allocations.
        fn get(&self, key: &str) -> Option<Arc<String>>;

        /// Stores a value in the cache with an expiration time.
        ///
        /// # Parameters
        /// * `key` - The key under which to store the value.
        /// * `value` - The value to store (borrowed). The cache will perform internal allocation.
        ///
        fn set(&self, key: &str, value: &str);
    }

    /**
     * ConcurrentCacheImpl - An implementation of the ConcurrentCache trait.
     */
    pub struct ConcurrentCacheImpl {
        state: Arc<(Mutex<SharedState>, Condvar)>,
        join_handle: Option<JoinHandle<()>>,
        duration: Duration
    }
    impl Drop for ConcurrentCacheImpl {
        fn drop(&mut self) {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.running = false;
            drop(state);
            cvar.notify_one();

            if let Some(handle) = self.join_handle.take() {
                handle.join().unwrap();
            }
        }
    }

    struct SharedState {
        map: HashMap<String, (Arc<String>, Instant)>,
        running: bool
    }
    impl SharedState {
        pub fn new () -> Self {
            Self {
                map: HashMap::new(),
                running: false
            }
        }
    }

    impl ConcurrentCache for ConcurrentCacheImpl {
        fn new (_d: Duration) -> Self {
            let mutex = Mutex::new(SharedState::new());
            let cvar = Condvar::new();
            let reference = Arc::new((mutex, cvar));
            let cloned_reference = Arc::clone(&reference);

            let handle = thread::spawn(move || {
                loop {
                    let (lock, cvar) = &*cloned_reference;
                    let mut state = lock.lock().unwrap();

                    let now = Instant::now();
                    let timeout: Duration;
                    if let Some(next_expiration) = state.map.iter().map(|(_, (_, instant))| *instant - now).min() {
                        timeout = if next_expiration > _d { _d } else { next_expiration };
                    }
                    else {
                        timeout = _d;
                    }

                    (state, _) = cvar.wait_timeout_while(state, timeout, |c| {
                        c.running
                    }).unwrap();

                    if !state.running {
                        state.map.clear();
                        break;
                    }

                    state.map.retain(|_: &String, (_, exp_instant)| {
                        *exp_instant > Instant::now()
                    });
                    
                }
            });

            Self {
                state: reference,
                join_handle: Some(handle),
                duration: _d
            }
        }

        fn get (&self, _key: &str) -> Option<Arc<String>> {
            let state = self.state.0.lock().unwrap();

            if let Some((string, instant)) = state.map.get(_key) {
                if *instant > Instant::now() {
                    return Some(Arc::clone(string));
                }
            }

            None
        }

        fn set (&self, _key: &str, value: &str) {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock().unwrap();

            state.map.insert(
                _key.to_string(),
                (Arc::new(value.to_string()), Instant::now() + self.duration)
            );

            drop(state);
            cvar.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::concurrent_cache::{ConcurrentCache, ConcurrentCacheImpl};
    use std::time::{Duration, Instant};
    use std::thread;

    #[test]
    fn test_basic_storage_and_retrieval() {
        let cache = ConcurrentCacheImpl::new(Duration::from_secs(10));
        
        // Test storing and retrieving a value
        cache.set("key1", "value1");
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("value1")
        );
        
        // Test retrieving a non-existent key
        assert_eq!(cache.get("non_existent_key"), None);
        
        // Test overwriting a value
        cache.set("key1", "new_value");
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("new_value")
        );
    }
    
    #[test]
    fn test_expiration() {
        // Create a cache with a very short expiration time
        let cache = ConcurrentCacheImpl::new(Duration::from_millis(100));
        
        // Store a value
        cache.set("key1", "value1");
        
        // Verify it exists immediately
        assert_eq!(
            cache.get("key1").as_deref().map(|s| s.as_str()),
            Some("value1")
        );
        
        // Wait for expiration
        thread::sleep(Duration::from_millis(150));
        
        // Verify it has expired
        assert_eq!(cache.get("key1"), None);
    }
    
    #[test]
    fn test_background_cleanup() {
        // Create a cache with a very short expiration time
        let cache = ConcurrentCacheImpl::new(Duration::from_millis(50));
        
        // Store multiple values
        for i in 0..5 {
            cache.set(&format!("key{}", i), &format!("value{}", i));
        }
        
        // Verify all values exist
        for i in 0..5 {
            assert_eq!(
                cache.get(&format!("key{}", i)).as_deref().map(|s| s.as_str()),
                Some(format!("value{}", i).as_str())
            );
        }
        
        // Wait for background cleanup (longer than expiration time)
        thread::sleep(Duration::from_millis(150));
        
        // Verify all values have been cleaned up
        for i in 0..5 {
            assert_eq!(cache.get(&format!("key{}", i)), None);
        }
    }
    
    #[test]
    fn test_different_expiration_times() {
        let cache = ConcurrentCacheImpl::new(Duration::from_millis(200));
        
        // Set a value that will expire quickly
        cache.set("short_lived", "value1");
        
        // Wait a bit
        thread::sleep(Duration::from_millis(100));
        
        // Set another value
        cache.set("long_lived", "value2");
        
        // Wait for the first value to expire
        thread::sleep(Duration::from_millis(150));
        
        // The first value should be gone, the second should remain
        assert_eq!(cache.get("short_lived"), None);
        assert_eq!(
            cache.get("long_lived").as_deref().map(|s| s.as_str()),
            Some("value2")
        );
    }
    
    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        
        let cache = Arc::new(ConcurrentCacheImpl::new(Duration::from_secs(5)));
        
        // Spawn threads to write to cache
        let mut handles = vec![];
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                cache_clone.set(&format!("thread_key{}", i), &format!("thread_value{}", i));
            });
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify all values were stored correctly
        for i in 0..10 {
            assert_eq!(
                cache.get(&format!("thread_key{}", i)).as_deref().map(|s| s.as_str()),
                Some(format!("thread_value{}", i).as_str())
            );
        }
    }
    
    #[test]
    fn test_memory_cleanup() {
        // This test verifies that the background thread is properly terminated
        {
            // Create a cache in a nested scope
            let _cache = ConcurrentCacheImpl::new(Duration::from_millis(10));
            // Cache will be dropped when it goes out of scope
        }

        // If the Drop implementation is working correctly, the background thread
        // should be terminated and this test will complete without hanging

        // We can't directly test for memory leaks in a unit test, but this
        // at least verifies that the background thread is properly joined
        assert!(true);
    }
    #[test]
    fn test_fast_cleanup() {
        // This test verifies that the background thread is properly terminated as soon as possible
        let t1 = Instant::now();
        {
            let _cache = ConcurrentCacheImpl::new(Duration::from_secs(5));
            thread::sleep(Duration::from_millis(10));
        }
        let t2 = Instant::now();
        assert!(t2 - t1 < Duration::from_secs(1));
    }
}