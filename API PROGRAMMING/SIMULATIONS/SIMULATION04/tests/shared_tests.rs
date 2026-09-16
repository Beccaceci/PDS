use simulation_004::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// =================================================================================
// PARAMETERIZED TEST SUITE FOR CONDVAR & MPSC IMPLEMENTATIONS
// =================================================================================

macro_rules! generate_executor_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_executor() -> impl TaskExecutor {
                ($factory)()
            }

            #[test]
            fn test_basic_single_task_execution() {
                let executor = get_executor();
                let executed = Arc::new(AtomicBool::new(false));
                let executed_clone = Arc::clone(&executed);

                let ok = executor.submit(move || {
                    executed_clone.store(true, Ordering::SeqCst);
                });
                assert!(ok, "submit should return true when executor is open");

                executor.close();
                executor.join();

                assert!(
                    executed.load(Ordering::SeqCst),
                    "Single submitted task should have been executed"
                );
            }

            #[test]
            fn test_fifo_ordering_verification() {
                let executor = get_executor();
                let sequence = Arc::new(Mutex::new(Vec::new()));

                const COUNT: usize = 100;
                for i in 0..COUNT {
                    let seq = Arc::clone(&sequence);
                    let ok = executor.submit(move || {
                        seq.lock().unwrap().push(i);
                    });
                    assert!(ok, "submit failed for task {}", i);
                }

                executor.close();
                executor.join();

                let seq = sequence.lock().unwrap();
                assert_eq!(seq.len(), COUNT);
                for (idx, &val) in seq.iter().enumerate() {
                    assert_eq!(idx, val, "Tasks must be executed in strict FIFO order");
                }
            }

            #[test]
            fn test_submit_after_close_returns_false() {
                let executor = get_executor();
                let executed = Arc::new(AtomicBool::new(false));
                let executed_clone = Arc::clone(&executed);

                let ok1 = executor.submit(move || {
                    executed_clone.store(true, Ordering::SeqCst);
                });
                assert!(ok1);

                executor.close();

                let rejected_executed = Arc::new(AtomicBool::new(false));
                let rejected_clone = Arc::clone(&rejected_executed);
                let ok2 = executor.submit(move || {
                    rejected_clone.store(true, Ordering::SeqCst);
                });
                assert!(!ok2, "submit after close should return false");

                executor.join();

                assert!(executed.load(Ordering::SeqCst));
                assert!(
                    !rejected_executed.load(Ordering::SeqCst),
                    "Task submitted after close must not be executed"
                );
            }

            #[test]
            fn test_queued_tasks_complete_after_close() {
                let executor = get_executor();
                let counter = Arc::new(AtomicUsize::new(0));

                const NUM_TASKS: usize = 50;
                for _ in 0..NUM_TASKS {
                    let c = Arc::clone(&counter);
                    executor.submit(move || {
                        c.fetch_add(1, Ordering::SeqCst);
                    });
                }

                executor.close();
                executor.join();

                assert_eq!(
                    counter.load(Ordering::SeqCst),
                    NUM_TASKS,
                    "All queued tasks prior to close must complete"
                );
            }

            #[test]
            fn test_join_blocks_until_worker_completes() {
                let executor = Arc::new(get_executor());
                let task_completed = Arc::new(AtomicBool::new(false));

                let t_comp = Arc::clone(&task_completed);
                executor.submit(move || {
                    thread::sleep(Duration::from_millis(150));
                    t_comp.store(true, Ordering::SeqCst);
                });

                executor.close();

                let start = Instant::now();
                let join_finished = Arc::new(AtomicBool::new(false));
                let jf_clone = Arc::clone(&join_finished);
                let exec_clone = Arc::clone(&executor);

                let joiner_handle = thread::spawn(move || {
                    exec_clone.join();
                    jf_clone.store(true, Ordering::SeqCst);
                });

                thread::sleep(Duration::from_millis(50));
                assert!(
                    !join_finished.load(Ordering::SeqCst),
                    "join() returned prematurely before worker completed task"
                );

                joiner_handle.join().unwrap();
                let elapsed = start.elapsed();

                assert!(
                    elapsed >= Duration::from_millis(140),
                    "join() returned too fast: {:?}",
                    elapsed
                );
                assert!(
                    task_completed.load(Ordering::SeqCst),
                    "Task must be completed before join returns"
                );
            }

            #[test]
            fn test_join_unblocks_when_closed_later() {
                let executor = Arc::new(get_executor());
                let joined = Arc::new(AtomicBool::new(false));

                let exec_clone = Arc::clone(&executor);
                let joined_clone = Arc::clone(&joined);

                let join_thread = thread::spawn(move || {
                    exec_clone.join();
                    joined_clone.store(true, Ordering::SeqCst);
                });

                thread::sleep(Duration::from_millis(100));
                assert!(
                    !joined.load(Ordering::SeqCst),
                    "join() should block even if queue is empty until close() is called"
                );

                executor.close();
                join_thread.join().unwrap();

                assert!(
                    joined.load(Ordering::SeqCst),
                    "join() should complete after close() is called"
                );
            }

            #[test]
            fn test_multiple_close_calls_safe() {
                let executor = get_executor();
                let counter = Arc::new(AtomicUsize::new(0));

                let c = Arc::clone(&counter);
                executor.submit(move || {
                    c.fetch_add(1, Ordering::SeqCst);
                });

                executor.close();
                executor.close();
                executor.close();

                let exec_arc = Arc::new(executor);
                let mut handles = vec![];
                for _ in 0..10 {
                    let ex = Arc::clone(&exec_arc);
                    handles.push(thread::spawn(move || {
                        ex.close();
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }

                exec_arc.join();
                assert_eq!(counter.load(Ordering::SeqCst), 1);
            }

            #[test]
            fn test_multiple_join_calls_safe() {
                let executor = Arc::new(get_executor());
                let counter = Arc::new(AtomicUsize::new(0));

                for _ in 0..10 {
                    let c = Arc::clone(&counter);
                    executor.submit(move || {
                        c.fetch_add(1, Ordering::SeqCst);
                    });
                }

                executor.close();

                let mut handles = vec![];
                for _ in 0..10 {
                    let ex = Arc::clone(&executor);
                    handles.push(thread::spawn(move || {
                        ex.join();
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }

                executor.join();
                assert_eq!(counter.load(Ordering::SeqCst), 10);
            }

            #[test]
            fn test_concurrent_submits() {
                let executor = Arc::new(get_executor());
                let total_tasks = Arc::new(AtomicUsize::new(0));

                const THREADS: usize = 10;
                const TASKS_PER_THREAD: usize = 50;

                let mut submit_handles = vec![];

                for _ in 0..THREADS {
                    let ex = Arc::clone(&executor);
                    let tt = Arc::clone(&total_tasks);
                    submit_handles.push(thread::spawn(move || {
                        for _ in 0..TASKS_PER_THREAD {
                            let tt_inner = Arc::clone(&tt);
                            if ex.submit(move || {
                                tt_inner.fetch_add(1, Ordering::SeqCst);
                            }) {
                                // task submitted
                            }
                        }
                    }));
                }

                for h in submit_handles {
                    h.join().unwrap();
                }

                executor.close();
                executor.join();

                assert_eq!(
                    total_tasks.load(Ordering::SeqCst),
                    THREADS * TASKS_PER_THREAD,
                    "All concurrently submitted tasks must be executed"
                );
            }

            #[test]
            fn test_concurrent_submits_close_and_joins() {
                let executor = Arc::new(get_executor());
                let task_count = Arc::new(AtomicUsize::new(0));

                const SUBMITTERS: usize = 8;
                const JOINERS: usize = 4;

                let mut handles = vec![];

                for _ in 0..SUBMITTERS {
                    let ex = Arc::clone(&executor);
                    let tc = Arc::clone(&task_count);
                    handles.push(thread::spawn(move || {
                        for _ in 0..20 {
                            let tc_inner = Arc::clone(&tc);
                            ex.submit(move || {
                                tc_inner.fetch_add(1, Ordering::SeqCst);
                            });
                            thread::sleep(Duration::from_millis(1));
                        }
                    }));
                }

                for _ in 0..JOINERS {
                    let ex = Arc::clone(&executor);
                    handles.push(thread::spawn(move || {
                        ex.join();
                    }));
                }

                let ex_close = Arc::clone(&executor);
                handles.push(thread::spawn(move || {
                    thread::sleep(Duration::from_millis(15));
                    ex_close.close();
                }));

                for h in handles {
                    h.join().unwrap();
                }

                let final_executed = task_count.load(Ordering::SeqCst);
                assert!(
                    final_executed > 0,
                    "At least some tasks should have executed"
                );
            }

            #[test]
            fn test_close_and_join_empty_executor() {
                let executor = get_executor();
                executor.close();
                executor.join();
            }
        }
    };
}

// Generate test suite for Condvar implementation (11 tests)
generate_executor_tests!(condvar_tests, make_condvar_task_executor);

// Generate test suite for MPSC Channel implementation (11 tests)
generate_executor_tests!(mpsc_tests, make_mpsc_task_executor);
