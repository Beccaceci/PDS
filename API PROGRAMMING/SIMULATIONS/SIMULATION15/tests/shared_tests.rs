use simulation_015::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

macro_rules! generate_single_flight_cache_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_cache<K, V>() -> impl SingleFlightCache<K, V>
            where
                K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
                V: Clone + Send + Sync + 'static,
            {
                ($factory)()
            }

            #[test]
            fn test_basic_compute_and_cache_hit() {
                let cache = get_cache::<String, i32>();
                let call_count = Arc::new(AtomicUsize::new(0));

                let c1 = Arc::clone(&call_count);
                let val1 = cache.get_or_compute("counter".to_string(), move || {
                    c1.fetch_add(1, Ordering::SeqCst);
                    42
                });
                assert_eq!(val1, 42);
                assert_eq!(call_count.load(Ordering::SeqCst), 1);

                let c2 = Arc::clone(&call_count);
                let val2 = cache.get_or_compute("counter".to_string(), move || {
                    c2.fetch_add(1, Ordering::SeqCst);
                    999
                });
                assert_eq!(val2, 42, "Cached value must be returned");
                assert_eq!(call_count.load(Ordering::SeqCst), 1, "Compute must not be called again");
            }

            #[test]
            fn test_compute_called_only_once_for_concurrent_callers() {
                let cache = get_cache::<String, String>();
                let compute_count = Arc::new(AtomicUsize::new(0));
                const NUM_THREADS: usize = 12;

                let mut handles = Vec::new();
                for _ in 0..NUM_THREADS {
                    let c = cache.clone();
                    let cnt = Arc::clone(&compute_count);
                    handles.push(thread::spawn(move || {
                        c.get_or_compute("slow_resource".to_string(), || {
                            cnt.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(100));
                            "computed_data".to_string()
                        })
                    }));
                }

                for h in handles {
                    let res = h.join().unwrap();
                    assert_eq!(res, "computed_data");
                }

                assert_eq!(
                    compute_count.load(Ordering::SeqCst),
                    1,
                    "Compute must be executed exactly ONCE across all concurrent callers"
                );
            }

            #[test]
            fn test_independent_keys_do_not_block_each_other() {
                let cache = get_cache::<String, String>();

                let c_slow = cache.clone();
                let slow_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let slow_started_clone = Arc::clone(&slow_started);

                let h_slow = thread::spawn(move || {
                    c_slow.get_or_compute("slow_key".to_string(), || {
                        slow_started_clone.store(true, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(250));
                        "slow_result".to_string()
                    })
                });

                while !slow_started.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(5));
                }

                let c_fast = cache.clone();
                let start_fast = Instant::now();
                let fast_result = c_fast.get_or_compute("fast_key".to_string(), || {
                    "fast_result".to_string()
                });
                let elapsed_fast = start_fast.elapsed();

                assert_eq!(fast_result, "fast_result");
                assert!(
                    elapsed_fast < Duration::from_millis(100),
                    "Independent key computation must not be blocked while slow_key is computing! Took {:?}",
                    elapsed_fast
                );

                let slow_result = h_slow.join().unwrap();
                assert_eq!(slow_result, "slow_result");
            }

            #[test]
            fn test_cloned_cache_instances_share_same_cache() {
                let cache1 = get_cache::<i32, i32>();
                let cache2 = cache1.clone();

                cache1.get_or_compute(10, || 100);

                let count = Arc::new(AtomicUsize::new(0));
                let cnt = Arc::clone(&count);
                let val = cache2.get_or_compute(10, move || {
                    cnt.fetch_add(1, Ordering::SeqCst);
                    999
                });

                assert_eq!(val, 100);
                assert_eq!(count.load(Ordering::SeqCst), 0, "Cloned cache must share entries");
            }

            #[test]
            fn test_multiple_distinct_keys_coalescing_stress() {
                let cache = get_cache::<usize, usize>();
                const NUM_KEYS: usize = 5;
                const CALLERS_PER_KEY: usize = 6;

                let compute_counts = Arc::new(
                    (0..NUM_KEYS)
                        .map(|i| (i, AtomicUsize::new(0)))
                        .collect::<HashMap<usize, AtomicUsize>>(),
                );

                let mut handles = Vec::new();
                for key in 0..NUM_KEYS {
                    for _ in 0..CALLERS_PER_KEY {
                        let c = cache.clone();
                        let counts = Arc::clone(&compute_counts);
                        handles.push(thread::spawn(move || {
                            c.get_or_compute(key, || {
                                counts.get(&key).unwrap().fetch_add(1, Ordering::SeqCst);
                                thread::sleep(Duration::from_millis(60));
                                key * 10
                            })
                        }));
                    }
                }

                for h in handles {
                    let _ = h.join().unwrap();
                }

                for key in 0..NUM_KEYS {
                    let count = compute_counts.get(&key).unwrap().load(Ordering::SeqCst);
                    assert_eq!(count, 1, "Key {} must be computed exactly once", key);
                    assert_eq!(cache.get_or_compute(key, || 0), key * 10);
                }
            }

            #[test]
            fn test_subsequent_request_after_compute_finished_is_immediate_cache_hit() {
                let cache = get_cache::<&'static str, &'static str>();

                let res1 = cache.get_or_compute("item", || "val");
                assert_eq!(res1, "val");

                let compute_attempted = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let ca = Arc::clone(&compute_attempted);

                let res2 = cache.get_or_compute("item", move || {
                    ca.store(true, Ordering::SeqCst);
                    "new_val"
                });

                assert_eq!(res2, "val");
                assert!(!compute_attempted.load(Ordering::SeqCst));
            }

            #[test]
            fn test_send_sync_bounds() {
                let cache = get_cache::<String, i32>();
                assert_send(&cache);
                assert_sync(&cache);
            }
        }
    };
}

// Suite per la versione a Condvar Globale (7 test)
generate_single_flight_cache_tests!(global_cvar_tests, make_global_cvar_cache);

// Suite per la versione a Condvar Dedicata Per-Key (7 test)
generate_single_flight_cache_tests!(per_key_cvar_tests, make_per_key_cvar_cache);
