use simulation_006::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// =================================================================================
// PARAMETERIZED TEST SUITE FOR CONDVAR & SYNC_CHANNEL IMPLEMENTATIONS
// =================================================================================

macro_rules! generate_transactional_queue_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_queue<T: Send + 'static>(capacity: usize) -> impl TransactionalQueue<T> {
                ($factory)(capacity)
            }

            #[test]
            fn test_basic_reserve_commit_pop() {
                let queue = get_queue::<String>(5);
                assert_eq!(queue.capacity(), 5);

                let res = queue.reserve().expect("reserve failed on open queue");
                res.commit("Hello Transaction".to_string());

                assert_eq!(queue.pop(), Some("Hello Transaction".to_string()));
            }

            #[test]
            fn test_reservation_drop_without_commit_frees_slot() {
                let queue = get_queue::<i32>(2);

                // Reserve 2 slots (filling capacity)
                let res1 = queue.reserve().expect("reserve 1 failed");
                let res2 = queue.reserve().expect("reserve 2 failed");

                // Dropping res1 should free a slot
                drop(res1);

                // We should now be able to reserve another slot without blocking
                let res3 = queue.reserve().expect("reserve 3 failed");
                res3.commit(42);
                res2.commit(100);

                assert_eq!(queue.pop(), Some(42));
                assert_eq!(queue.pop(), Some(100));
            }

            #[test]
            fn test_commit_order_determines_pop_order() {
                let queue = get_queue::<i32>(3);

                let res1 = queue.reserve().expect("reserve 1 failed");
                let res2 = queue.reserve().expect("reserve 2 failed");

                // Commit res2 BEFORE res1
                res2.commit(200);
                res1.commit(100);

                // Pop should return elements in commit order (200 first, then 100)
                assert_eq!(queue.pop(), Some(200));
                assert_eq!(queue.pop(), Some(100));
            }

            #[test]
            fn test_reserve_blocks_on_full_capacity_unblocks_on_pop() {
                let queue = Arc::new(get_queue::<i32>(2));

                let res1 = queue.reserve().expect("reserve 1 failed");
                res1.commit(1);
                let res2 = queue.reserve().expect("reserve 2 failed");
                res2.commit(2);

                let q_clone = Arc::clone(&queue);
                let reserved_flag = Arc::new(AtomicBool::new(false));
                let rf_clone = Arc::clone(&reserved_flag);

                let handle = thread::spawn(move || {
                    let res3 = q_clone.reserve().expect("reserve 3 failed");
                    rf_clone.store(true, Ordering::SeqCst);
                    res3.commit(3);
                });

                thread::sleep(Duration::from_millis(50));
                assert!(
                    !reserved_flag.load(Ordering::SeqCst),
                    "reserve() must block when capacity is full"
                );

                // Pop 1 element to free space
                assert_eq!(queue.pop(), Some(1));

                handle.join().unwrap();
                assert!(reserved_flag.load(Ordering::SeqCst));
                assert_eq!(queue.pop(), Some(2));
                assert_eq!(queue.pop(), Some(3));
            }

            #[test]
            fn test_reserve_timeout_success_and_failure() {
                let queue = get_queue::<i32>(1);

                let res1 = queue.reserve().expect("reserve failed");

                let start = Instant::now();
                let res_opt = queue.reserve_timeout(Duration::from_millis(80));
                let elapsed = start.elapsed();

                assert!(res_opt.is_none(), "reserve_timeout on full queue should return None");
                assert!(elapsed >= Duration::from_millis(60));

                drop(res1); // Free space

                let res2 = queue.reserve_timeout(Duration::from_millis(80));
                assert!(res2.is_some(), "reserve_timeout on available queue should return Some");
                res2.unwrap().commit(99);

                assert_eq!(queue.pop(), Some(99));
            }

            #[test]
            fn test_pop_blocks_when_empty_unblocks_on_commit() {
                let queue = Arc::new(get_queue::<i32>(5));

                let q_clone = Arc::clone(&queue);
                let handle = thread::spawn(move || {
                    q_clone.pop()
                });

                thread::sleep(Duration::from_millis(50));
                let res = queue.reserve().expect("reserve failed");
                res.commit(777);

                assert_eq!(handle.join().unwrap(), Some(777));
            }

            #[test]
            fn test_close_drains_and_returns_none() {
                let queue = get_queue::<i32>(3);

                let res1 = queue.reserve().expect("reserve 1 failed");
                let res2 = queue.reserve().expect("reserve 2 failed");

                res1.commit(10);
                queue.close();

                // A pending reservation created before close can still commit
                res2.commit(20);

                assert_eq!(queue.pop(), Some(10));
                assert_eq!(queue.pop(), Some(20));
                assert_eq!(queue.pop(), None);
            }

            #[test]
            fn test_reserve_returns_none_after_close() {
                let queue = get_queue::<i32>(2);
                queue.close();

                assert!(queue.reserve().is_none(), "reserve() on closed queue must return None");
                assert!(queue.reserve_timeout(Duration::from_millis(50)).is_none(), "reserve_timeout() on closed queue must return None");
            }

            #[test]
            fn test_mpmc_concurrent_stress() {
                let queue = Arc::new(get_queue::<usize>(10));
                const NUM_PRODUCERS: usize = 15;
                const NUM_CONSUMERS: usize = 5;
                const ITEMS_PER_PRODUCER: usize = 40;

                let committed_count = Arc::new(AtomicUsize::new(0));
                let popped_count = Arc::new(AtomicUsize::new(0));

                let mut producer_handles = vec![];
                for p in 0..NUM_PRODUCERS {
                    let q = Arc::clone(&queue);
                    let comm_counter = Arc::clone(&committed_count);
                    producer_handles.push(thread::spawn(move || {
                        for i in 0..ITEMS_PER_PRODUCER {
                            if let Some(res) = q.reserve() {
                                if (p + i) % 4 == 0 {
                                    // Drop ~25% of reservations without committing
                                    drop(res);
                                } else {
                                    res.commit(p * 1000 + i);
                                    comm_counter.fetch_add(1, Ordering::SeqCst);
                                }
                            }
                        }
                    }));
                }

                let mut consumer_handles = vec![];
                for _ in 0..NUM_CONSUMERS {
                    let q = Arc::clone(&queue);
                    let pop_counter = Arc::clone(&popped_count);
                    consumer_handles.push(thread::spawn(move || {
                        let mut count = 0;
                        while let Some(_) = q.pop() {
                            count += 1;
                        }
                        pop_counter.fetch_add(count, Ordering::SeqCst);
                    }));
                }

                for h in producer_handles {
                    h.join().unwrap();
                }

                queue.close();

                for h in consumer_handles {
                    h.join().unwrap();
                }

                let total_committed = committed_count.load(Ordering::SeqCst);
                let total_popped = popped_count.load(Ordering::SeqCst);

                assert_eq!(total_committed, total_popped);
                assert!(total_committed > 0);
            }
        }
    };
}

// Generate test suite for Condvar implementation (9 tests)
generate_transactional_queue_tests!(condvar_tests, make_condvar_transactional_queue);

// Generate test suite for SyncChannel implementation (9 tests)
generate_transactional_queue_tests!(sync_channel_tests, make_sync_channel_transactional_queue);
