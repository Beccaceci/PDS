use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use simulation_003::*;

// =================================================================================
// PARAMETERIZED TEST SUITE FOR CONDVAR & MPSC IMPLEMENTATIONS
// =================================================================================

macro_rules! generate_exchanger_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_exchanger<T: Send + 'static>() -> impl Exchanger<T> {
                ($factory)()
            }

            /// Test 1: Basic 2-thread rendezvous exchange with integer values.
            #[test]
            fn test_basic_rendezvous_exchange() {
                let exchanger = Arc::new(get_exchanger::<i32>());

                let ex1 = Arc::clone(&exchanger);
                let handle1 = thread::spawn(move || ex1.exchange(100));

                let ex2 = Arc::clone(&exchanger);
                let handle2 = thread::spawn(move || ex2.exchange(200));

                let res1 = handle1.join().expect("Thread 1 failed");
                let res2 = handle2.join().expect("Thread 2 failed");

                assert_eq!(res1, 200, "Thread 1 should receive Thread 2's value");
                assert_eq!(res2, 100, "Thread 2 should receive Thread 1's value");
            }

            /// Test 2: Exchange with non-Copy move-only types (String).
            #[test]
            fn test_exchange_non_copy_type() {
                let exchanger = Arc::new(get_exchanger::<String>());

                let ex1 = Arc::clone(&exchanger);
                let h1 = thread::spawn(move || ex1.exchange("Hello".to_string()));

                let ex2 = Arc::clone(&exchanger);
                let h2 = thread::spawn(move || ex2.exchange("World".to_string()));

                let res1 = h1.join().expect("Thread 1 failed");
                let res2 = h2.join().expect("Thread 2 failed");

                assert_eq!(res1, "World");
                assert_eq!(res2, "Hello");
            }

            /// Test 3: Multiple sequential pairwise exchanges on the same Exchanger instance.
            #[test]
            fn test_multiple_sequential_exchanges() {
                let exchanger = Arc::new(get_exchanger::<usize>());

                for round in 1..=5 {
                    let val_a = round * 10;
                    let val_b = round * 100;

                    let ex1 = Arc::clone(&exchanger);
                    let h1 = thread::spawn(move || ex1.exchange(val_a));

                    let ex2 = Arc::clone(&exchanger);
                    let h2 = thread::spawn(move || ex2.exchange(val_b));

                    let res_a = h1.join().expect("Thread A failed");
                    let res_b = h2.join().expect("Thread B failed");

                    assert_eq!(res_a, val_b, "Round {round}: Thread A should get B's value");
                    assert_eq!(res_b, val_a, "Round {round}: Thread B should get A's value");
                }
            }

            /// Test 4: 10+ concurrent threads pairwise matching stress test.
            #[test]
            fn test_twenty_concurrent_threads_pairwise_matching() {
                const THREAD_COUNT: usize = 20;
                let exchanger = Arc::new(get_exchanger::<usize>());
                let results = Arc::new(Mutex::new(Vec::new()));

                let mut handles = Vec::new();
                for i in 0..THREAD_COUNT {
                    let ex = Arc::clone(&exchanger);
                    let res_store = Arc::clone(&results);
                    handles.push(thread::spawn(move || {
                        let received = ex.exchange(i);
                        res_store.lock().unwrap().push((i, received));
                    }));
                }

                for h in handles {
                    h.join().expect("Stress test thread failed");
                }

                let pairs = results.lock().unwrap();
                assert_eq!(pairs.len(), THREAD_COUNT);

                let mut given_set = HashSet::new();
                let mut received_set = HashSet::new();

                for &(sent, received) in pairs.iter() {
                    assert_ne!(sent, received, "A thread must not receive its own value");
                    given_set.insert(sent);
                    received_set.insert(received);

                    let counterpart = pairs.iter().find(|(s, _)| *s == received);
                    assert!(
                        counterpart.is_some(),
                        "Counterpart thread for value {received} not found"
                    );
                    let (_, counterpart_received) = counterpart.unwrap();
                    assert_eq!(
                        *counterpart_received, sent,
                        "Pairwise symmetry broken: Thread {sent} received {received}, but Thread {received} received {counterpart_received}"
                    );
                }

                assert_eq!(given_set.len(), THREAD_COUNT);
                assert_eq!(received_set.len(), THREAD_COUNT);
            }

            /// Test 5: exchange_timeout failure returning Err(value) when no peer arrives.
            #[test]
            fn test_exchange_timeout_failure() {
                let exchanger = get_exchanger::<i32>();
                let start = Instant::now();
                let res = exchanger.exchange_timeout(42, Duration::from_millis(100));
                let elapsed = start.elapsed();

                assert_eq!(res, Err(42), "Timeout must return Err with original value");
                assert!(
                    elapsed >= Duration::from_millis(80),
                    "exchange_timeout should wait for requested duration before failing (elapsed: {elapsed:?})"
                );
            }

            /// Test 6: exchange_timeout success when first thread waits with timeout and second thread arrives.
            #[test]
            fn test_exchange_timeout_success_first_waits() {
                let exchanger = Arc::new(get_exchanger::<String>());

                let ex1 = Arc::clone(&exchanger);
                let h1 = thread::spawn(move || {
                    ex1.exchange_timeout("WaitSec".to_string(), Duration::from_secs(2))
                });

                thread::sleep(Duration::from_millis(50));

                let ex2 = Arc::clone(&exchanger);
                let h2 = thread::spawn(move || ex2.exchange("ArriveSecond".to_string()));

                let res1 = h1.join().expect("h1 failed");
                let res2 = h2.join().expect("h2 failed");

                assert_eq!(res1, Ok("ArriveSecond".to_string()));
                assert_eq!(res2, "WaitSec".to_string());
            }

            /// Test 7: exchange_timeout success when second thread uses timeout and arrives after first thread.
            #[test]
            fn test_exchange_timeout_success_second_arrives_first() {
                let exchanger = Arc::new(get_exchanger::<i32>());

                let ex1 = Arc::clone(&exchanger);
                let h1 = thread::spawn(move || ex1.exchange(10));

                thread::sleep(Duration::from_millis(20));

                let ex2 = Arc::clone(&exchanger);
                let h2 = thread::spawn(move || ex2.exchange_timeout(20, Duration::from_secs(2)));

                let res1 = h1.join().expect("h1 failed");
                let res2 = h2.join().expect("h2 failed");

                assert_eq!(res1, 20);
                assert_eq!(res2, Ok(10));
            }

            /// Test 8: Isolation & cleanup: timed-out thread must not corrupt subsequent exchanges.
            #[test]
            fn test_timed_out_thread_does_not_corrupt_subsequent_exchange() {
                let exchanger = Arc::new(get_exchanger::<i32>());

                let ex_a = Arc::clone(&exchanger);
                let res_a = ex_a.exchange_timeout(999, Duration::from_millis(40));
                assert_eq!(res_a, Err(999));

                let ex_b = Arc::clone(&exchanger);
                let h_b = thread::spawn(move || ex_b.exchange(10));

                let ex_c = Arc::clone(&exchanger);
                let h_c = thread::spawn(move || ex_c.exchange(20));

                let res_b = h_b.join().expect("h_b failed");
                let res_c = h_c.join().expect("h_c failed");

                assert_eq!(res_b, 20);
                assert_eq!(res_c, 10);
            }

            /// Test 9: Mixed exchanges and timeouts stress test under concurrency.
            #[test]
            fn test_mixed_concurrent_exchanges_and_timeouts() {
                let exchanger = Arc::new(get_exchanger::<usize>());
                let mut handles = Vec::new();

                for i in 0..12 {
                    let ex = Arc::clone(&exchanger);
                    let h = thread::spawn(move || {
                        if i % 2 == 0 {
                            let res = ex.exchange(i);
                            assert_ne!(res, i);
                        } else {
                            let res = ex.exchange_timeout(i, Duration::from_millis(150));
                            match res {
                                Ok(val) => assert_ne!(val, i),
                                Err(val) => assert_eq!(val, i),
                            }
                        }
                    });
                    handles.push(h);
                }

                for h in handles {
                    h.join().expect("Thread panicked");
                }
            }

            /// Test 10: Heavy multi-round stress test.
            #[test]
            fn test_heavy_multi_round_stress() {
                const ROUNDS: usize = 5;
                const THREADS_PER_ROUND: usize = 16;
                let exchanger = Arc::new(get_exchanger::<usize>());

                for round in 0..ROUNDS {
                    let mut handles = Vec::new();
                    let results = Arc::new(Mutex::new(Vec::new()));

                    for t in 0..THREADS_PER_ROUND {
                        let ex = Arc::clone(&exchanger);
                        let res_store = Arc::clone(&results);
                        let val = round * 1000 + t;
                        handles.push(thread::spawn(move || {
                            let rec = ex.exchange(val);
                            res_store.lock().unwrap().push((val, rec));
                        }));
                    }

                    for h in handles {
                        h.join().expect("Thread in round failed");
                    }

                    let pairs = results.lock().unwrap();
                    assert_eq!(pairs.len(), THREADS_PER_ROUND);

                    for &(sent, rec) in pairs.iter() {
                        let peer = pairs.iter().find(|(s, _)| *s == rec).unwrap();
                        assert_eq!(peer.1, sent);
                    }
                }
            }
        }
    };
}

// Generate test suite for Condvar implementation (10 tests)
generate_exchanger_tests!(condvar_tests, make_condvar_exchanger);

// Generate test suite for MPSC Channel implementation (10 tests)
generate_exchanger_tests!(mpsc_tests, make_mpsc_exchanger);
