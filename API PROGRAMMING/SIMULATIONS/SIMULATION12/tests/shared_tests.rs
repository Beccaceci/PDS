use simulation_012::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// =================================================================================
// PARAMETERIZED TEST SUITE FOR CONDVAR & ATOMIC IMPLEMENTATIONS
// =================================================================================

macro_rules! generate_metrics_board_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_board() -> impl MetricsBoard {
                ($factory)()
            }

            #[test]
            fn test_basic_register_and_record() {
                let board = get_board();

                let handle = board.register("cpu_usage").expect("Failed to register metric");
                handle.record(42.5);

                let snap = board.snapshot();
                assert_eq!(snap.len(), 1);
                assert_eq!(snap[0], ("cpu_usage".to_string(), 42.5));
            }

            #[test]
            fn test_duplicate_registration_rejected() {
                let board = get_board();

                let _h1 = board.register("temperature").expect("First register should succeed");
                let err = board.register("temperature");

                match err {
                    Err(e) => assert_eq!(e, NameAlreadyUsed),
                    Ok(_) => panic!("Expected duplicate registration to return Err(NameAlreadyUsed)"),
                }
            }

            #[test]
            fn test_name_recycling_on_handle_drop() {
                let board = get_board();

                let h1 = board.register("voltage").expect("Registration should succeed");
                h1.record(3.3);

                // While h1 is alive, duplicate registration fails
                match board.register("voltage") {
                    Err(e) => assert_eq!(e, NameAlreadyUsed),
                    Ok(_) => panic!("Expected duplicate registration to fail"),
                }

                // Drop h1 -> name becomes available again
                drop(h1);

                // Snapshot should now be empty
                let snap = board.snapshot();
                assert_eq!(snap.len(), 0);

                // Re-registration with the same name must now succeed
                let h2 = board.register("voltage").expect("Re-registration after drop must succeed");
                h2.record(5.0);

                let snap2 = board.snapshot();
                assert_eq!(snap2.len(), 1);
                assert_eq!(snap2[0], ("voltage".to_string(), 5.0));
            }

            #[test]
            fn test_multiple_distinct_metrics_lifecycle() {
                let board = get_board();

                let h_cpu = board.register("cpu").unwrap();
                let h_mem = board.register("mem").unwrap();
                let h_disk = board.register("disk").unwrap();

                h_cpu.record(75.0);
                h_mem.record(60.5);
                h_disk.record(12.0);

                let snap: HashMap<String, f64> = board.snapshot().into_iter().collect();
                assert_eq!(snap.len(), 3);
                assert_eq!(snap.get("cpu"), Some(&75.0));
                assert_eq!(snap.get("mem"), Some(&60.5));
                assert_eq!(snap.get("disk"), Some(&12.0));

                drop(h_mem);

                let snap2: HashMap<String, f64> = board.snapshot().into_iter().collect();
                assert_eq!(snap2.len(), 2);
                assert_eq!(snap2.get("cpu"), Some(&75.0));
                assert_eq!(snap2.get("mem"), None);
                assert_eq!(snap2.get("disk"), Some(&12.0));
            }

            #[test]
            fn test_wait_for_threshold_immediate_if_already_met() {
                let board = get_board();
                let h = board.register("temp").unwrap();
                h.record(100.0);

                // Threshold 90.0 is already exceeded -> must return immediately
                board.wait_for_threshold("temp", 90.0);
            }

            #[test]
            fn test_wait_for_threshold_blocks_until_reached() {
                let board = Arc::new(get_board());
                let handle = Arc::new(board.register("speed").unwrap());
                handle.record(10.0);

                let b_clone = Arc::clone(&board);
                let unblocked = Arc::new(AtomicBool::new(false));
                let u_clone = Arc::clone(&unblocked);

                let waiter = thread::spawn(move || {
                    b_clone.wait_for_threshold("speed", 50.0);
                    u_clone.store(true, Ordering::SeqCst);
                });

                thread::sleep(Duration::from_millis(50));
                assert!(!unblocked.load(Ordering::SeqCst), "Waiter should be blocked");

                // Update to 30.0 (still below 50.0)
                handle.record(30.0);
                thread::sleep(Duration::from_millis(50));
                assert!(!unblocked.load(Ordering::SeqCst), "Waiter should still be blocked");

                // Update to 55.0 (meets threshold >= 50.0)
                handle.record(55.0);
                waiter.join().unwrap();
                assert!(unblocked.load(Ordering::SeqCst));
            }

            #[test]
            fn test_multiple_waiters_different_thresholds() {
                let board = Arc::new(get_board());
                let handle = Arc::new(board.register("pressure").unwrap());
                handle.record(0.0);

                let stage1 = Arc::new(AtomicBool::new(false));
                let stage2 = Arc::new(AtomicBool::new(false));
                let stage3 = Arc::new(AtomicBool::new(false));

                let b1 = Arc::clone(&board);
                let s1 = Arc::clone(&stage1);
                let w1 = thread::spawn(move || {
                    b1.wait_for_threshold("pressure", 10.0);
                    s1.store(true, Ordering::SeqCst);
                });

                let b2 = Arc::clone(&board);
                let s2 = Arc::clone(&stage2);
                let w2 = thread::spawn(move || {
                    b2.wait_for_threshold("pressure", 20.0);
                    s2.store(true, Ordering::SeqCst);
                });

                let b3 = Arc::clone(&board);
                let s3 = Arc::clone(&stage3);
                let w3 = thread::spawn(move || {
                    b3.wait_for_threshold("pressure", 30.0);
                    s3.store(true, Ordering::SeqCst);
                });

                thread::sleep(Duration::from_millis(40));
                assert!(!stage1.load(Ordering::SeqCst));

                handle.record(15.0);
                w1.join().unwrap();
                assert!(stage1.load(Ordering::SeqCst));
                assert!(!stage2.load(Ordering::SeqCst));

                handle.record(25.0);
                w2.join().unwrap();
                assert!(stage2.load(Ordering::SeqCst));
                assert!(!stage3.load(Ordering::SeqCst));

                handle.record(35.0);
                w3.join().unwrap();
                assert!(stage3.load(Ordering::SeqCst));
            }

            #[test]
            fn test_concurrent_atomic_snapshot_consistency() {
                let board = Arc::new(get_board());
                let handle_x = Arc::new(board.register("x").unwrap());
                let handle_y = Arc::new(board.register("y").unwrap());

                handle_x.record(0.0);
                handle_y.record(0.0);

                let stop = Arc::new(AtomicBool::new(false));

                let stop_updater = Arc::clone(&stop);
                let hx = Arc::clone(&handle_x);
                let hy = Arc::clone(&handle_y);
                let updater = thread::spawn(move || {
                    let mut val = 1.0;
                    while !stop_updater.load(Ordering::Relaxed) {
                        hx.record(val);
                        hy.record(val);
                        val += 1.0;
                    }
                });

                let mut readers = vec![];
                for _ in 0..4 {
                    let b = Arc::clone(&board);
                    let s = Arc::clone(&stop);
                    readers.push(thread::spawn(move || {
                        let mut count = 0;
                        let mut last_seen_x = 0.0;
                        let mut last_seen_y = 0.0;
                        while !s.load(Ordering::Relaxed) && count < 500 {
                            let snap = b.snapshot();
                            let map: HashMap<String, f64> = snap.into_iter().collect();
                            if let (Some(&vx), Some(&vy)) = (map.get("x"), map.get("y")) {
                                assert!(vx >= last_seen_x, "Metric x went backwards: {} < {}", vx, last_seen_x);
                                assert!(vy >= last_seen_y, "Metric y went backwards: {} < {}", vy, last_seen_y);
                                last_seen_x = vx;
                                last_seen_y = vy;
                            }
                            count += 1;
                        }
                    }));
                }

                for r in readers {
                    r.join().unwrap();
                }

                stop.store(true, Ordering::SeqCst);
                updater.join().unwrap();
            }

            #[test]
            fn test_concurrent_multi_metric_stress() {
                let board = Arc::new(get_board());
                const NUM_METRICS: usize = 8;
                const UPDATES_PER_THREAD: usize = 500;

                let mut handles = vec![];
                for i in 0..NUM_METRICS {
                    let name = format!("metric_{}", i);
                    let h = Arc::new(board.register(&name).unwrap());
                    handles.push(h);
                }

                let mut thread_handles = vec![];
                for h in &handles {
                    let h_clone = Arc::clone(h);
                    thread_handles.push(thread::spawn(move || {
                        for val in 0..UPDATES_PER_THREAD {
                            h_clone.record(val as f64);
                        }
                    }));
                }

                for th in thread_handles {
                    th.join().unwrap();
                }

                let snap = board.snapshot();
                assert_eq!(snap.len(), NUM_METRICS);
                for (_, val) in snap {
                    assert_eq!(val, (UPDATES_PER_THREAD - 1) as f64);
                }
            }

            #[test]
            fn test_concurrent_registration_and_recycling_stress() {
                let board = Arc::new(get_board());
                const WORKERS: usize = 10;
                const ITERATIONS: usize = 50;

                let mut handles = vec![];
                for w in 0..WORKERS {
                    let b = Arc::clone(&board);
                    handles.push(thread::spawn(move || {
                        for i in 0..ITERATIONS {
                            let name = format!("worker_{}_metric", w);
                            let h = b.register(&name).expect("Register should succeed");
                            h.record(i as f64);
                            drop(h);
                        }
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }

                let final_snap = board.snapshot();
                assert_eq!(final_snap.len(), 0, "All temporary metrics should be dropped");
            }
        }
    };
}

// Generate test suite for Condvar implementation (10 tests)
generate_metrics_board_tests!(condvar_tests, make_condvar_metrics_board);

// Generate test suite for Atomic lock-free implementation (10 tests)
generate_metrics_board_tests!(atomic_tests, make_atomic_metrics_board);
