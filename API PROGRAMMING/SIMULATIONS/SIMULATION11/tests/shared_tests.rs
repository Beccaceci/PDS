use simulation_011::*;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_capacity() {
    let empty_mgr = make_multi_resource_manager::<i32>(vec![]);
    assert_eq!(empty_mgr.capacity(), 0);

    let mgr = make_multi_resource_manager(vec![10, 20, 30, 40, 50]);
    assert_eq!(mgr.capacity(), 5);
}

#[test]
fn test_single_resource_acquire_and_access() {
    let mgr = make_multi_resource_manager(vec!["res0".to_string(), "res1".to_string(), "res2".to_string()]);
    assert_eq!(mgr.capacity(), 3);

    let set = mgr.acquire_all(&[1]);
    assert_eq!(set.get(1), "res1");
}

#[test]
fn test_multi_resource_acquire_and_access() {
    let items = vec![100, 200, 300, 400, 500];
    let mgr = make_multi_resource_manager(items);

    let set = mgr.acquire_all(&[0, 2, 4]);
    assert_eq!(*set.get(0), 100);
    assert_eq!(*set.get(2), 300);
    assert_eq!(*set.get(4), 500);
}

#[test]
fn test_acquire_empty_ids() {
    let mgr = make_multi_resource_manager(vec![1, 2, 3]);
    let set = mgr.acquire_all(&[]);
    
    // Getting any index should panic since no resources were acquired
    let res = panic::catch_unwind(AssertUnwindSafe(|| {
        set.get(0);
    }));
    assert!(res.is_err(), "Accessing unacquired resource on empty set must panic");
}

#[test]
fn test_get_unheld_id_panics() {
    let mgr = make_multi_resource_manager(vec![10, 20, 30, 40]);
    let set = mgr.acquire_all(&[0, 2]);

    // ID 0 and 2 are held
    assert_eq!(*set.get(0), 10);
    assert_eq!(*set.get(2), 30);

    // ID 1 is within capacity but not in the acquired set -> must panic
    let res_1 = panic::catch_unwind(AssertUnwindSafe(|| {
        set.get(1);
    }));
    assert!(res_1.is_err(), "Accessing unheld id 1 must panic");

    // ID 3 is within capacity but not in the acquired set -> must panic
    let res_3 = panic::catch_unwind(AssertUnwindSafe(|| {
        set.get(3);
    }));
    assert!(res_3.is_err(), "Accessing unheld id 3 must panic");
}

#[test]
fn test_get_out_of_bounds_id_panics() {
    let mgr = make_multi_resource_manager(vec![10, 20]);
    let set = mgr.acquire_all(&[0]);

    let res = panic::catch_unwind(AssertUnwindSafe(|| {
        set.get(999);
    }));
    assert!(res.is_err(), "Accessing out of bounds id 999 must panic");
}

#[test]
fn test_raii_drop_releases_all_resources_simultaneously() {
    let mgr = Arc::new(make_multi_resource_manager(vec![10, 20, 30, 40]));
    
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();

    let mgr1 = Arc::clone(&mgr);
    let t1 = thread::spawn(move || {
        let set = mgr1.acquire_all(&[0, 1, 2]);
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(set); // Releases 0, 1, 2 simultaneously
    });

    // Wait until t1 holds [0, 1, 2]
    ready_rx.recv().unwrap();

    let mgr2 = Arc::clone(&mgr);
    let done_tx2 = done_tx.clone();
    let t2 = thread::spawn(move || {
        let set = mgr2.acquire_all(&[1]);
        assert_eq!(*set.get(1), 20);
        done_tx2.send(2).unwrap();
    });

    let mgr3 = Arc::clone(&mgr);
    let done_tx3 = done_tx.clone();
    let t3 = thread::spawn(move || {
        let set = mgr3.acquire_all(&[0, 2]);
        assert_eq!(*set.get(0), 10);
        assert_eq!(*set.get(2), 30);
        done_tx3.send(3).unwrap();
    });

    // Ensure neither t2 nor t3 can complete while t1 holds resources
    thread::sleep(Duration::from_millis(50));
    assert!(done_rx.try_recv().is_err(), "Threads 2 and 3 should be blocked waiting for resources");

    // Signal t1 to release its resources
    release_tx.send(()).unwrap();
    t1.join().unwrap();

    // Now both t2 and t3 must be able to complete
    let first = done_rx.recv_timeout(Duration::from_millis(500)).expect("One waiting thread should finish");
    let second = done_rx.recv_timeout(Duration::from_millis(500)).expect("Other waiting thread should finish");
    assert!((first == 2 && second == 3) || (first == 3 && second == 2));

    t2.join().unwrap();
    t3.join().unwrap();
}

#[test]
fn test_concurrent_disjoint_sets_do_not_block_each_other() {
    let mgr = Arc::new(make_multi_resource_manager(vec![1, 2, 3, 4]));
    let barrier = Arc::new(Barrier::new(2));

    let mgr1 = Arc::clone(&mgr);
    let b1 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || {
        let set = mgr1.acquire_all(&[0, 1]);
        b1.wait();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(*set.get(0), 1);
        assert_eq!(*set.get(1), 2);
    });

    let mgr2 = Arc::clone(&mgr);
    let b2 = Arc::clone(&barrier);
    let t2 = thread::spawn(move || {
        let set = mgr2.acquire_all(&[2, 3]);
        b2.wait();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(*set.get(2), 3);
        assert_eq!(*set.get(3), 4);
    });

    t1.join().unwrap();
    t2.join().unwrap();
}

#[test]
fn test_deadlock_prevention_reverse_order() {
    // Thread 1 acquires [0, 1] while Thread 2 acquires [1, 0] repeatedly.
    // If order normalization is missing, this triggers an immediate deadlock.
    let mgr = Arc::new(make_multi_resource_manager(vec![100, 200]));
    const ITERATIONS: usize = 200;

    let mgr1 = Arc::clone(&mgr);
    let t1 = thread::spawn(move || {
        for _ in 0..ITERATIONS {
            let set = mgr1.acquire_all(&[0, 1]);
            assert_eq!(*set.get(0), 100);
            assert_eq!(*set.get(1), 200);
            thread::yield_now();
        }
    });

    let mgr2 = Arc::clone(&mgr);
    let t2 = thread::spawn(move || {
        for _ in 0..ITERATIONS {
            let set = mgr2.acquire_all(&[1, 0]);
            assert_eq!(*set.get(0), 100);
            assert_eq!(*set.get(1), 200);
            thread::yield_now();
        }
    });

    t1.join().unwrap();
    t2.join().unwrap();
}

#[test]
fn test_deadlock_prevention_overlapping_sets() {
    // 3 threads requiring circular subsets: [0, 1], [1, 2], [2, 0]
    // plus a 4th thread requiring [2, 1]
    let mgr = Arc::new(make_multi_resource_manager(vec![10, 20, 30]));
    const ITERATIONS: usize = 150;

    let handles: Vec<_> = vec![
        (vec![0, 1], Arc::clone(&mgr)),
        (vec![1, 2], Arc::clone(&mgr)),
        (vec![2, 0], Arc::clone(&mgr)),
        (vec![2, 1], Arc::clone(&mgr)),
    ]
    .into_iter()
    .map(|(ids, m)| {
        thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let set = m.acquire_all(&ids);
                for &id in &ids {
                    assert_eq!(*set.get(id), (id as i32 + 1) * 10);
                }
                thread::yield_now();
            }
        })
    })
    .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_mutual_exclusion_guarantee() {
    const NUM_RESOURCES: usize = 5;
    let items: Vec<usize> = (0..NUM_RESOURCES).collect();
    let mgr = Arc::new(make_multi_resource_manager(items));
    
    // Shared state to verify that no two threads hold the same resource concurrently
    let in_use = Arc::new((0..NUM_RESOURCES).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
    const NUM_THREADS: usize = 8;
    const ITERS_PER_THREAD: usize = 80;

    let mut handles = Vec::new();

    for thread_id in 0..NUM_THREADS {
        let m = Arc::clone(&mgr);
        let u = Arc::clone(&in_use);
        handles.push(thread::spawn(move || {
            for iter in 0..ITERS_PER_THREAD {
                // Select 2 resources with different patterns
                let ids = match (thread_id + iter) % 4 {
                    0 => vec![0, 1],
                    1 => vec![1, 2, 3],
                    2 => vec![3, 4],
                    _ => vec![4, 0, 2],
                };

                let set = m.acquire_all(&ids);

                // Check that none of the acquired resources are currently marked in_use
                for &id in &ids {
                    let prev = u[id].swap(true, Ordering::SeqCst);
                    assert!(!prev, "Resource {} was already marked in use by another thread!", id);
                }

                // Verify access
                for &id in &ids {
                    assert_eq!(*set.get(id), id);
                }
                
                thread::sleep(Duration::from_micros(100));

                // Clear in_use marks before dropping
                for &id in &ids {
                    let prev = u[id].swap(false, Ordering::SeqCst);
                    assert!(prev, "Resource {} was unexpectedly cleared before release!", id);
                }

                drop(set);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_high_concurrency_stress() {
    const NUM_ITEMS: usize = 8;
    let items: Vec<usize> = (0..NUM_ITEMS).map(|i| i * 11).collect();
    let mgr = Arc::new(make_multi_resource_manager(items));
    
    const NUM_THREADS: usize = 12;
    const OPERATIONS_PER_THREAD: usize = 100;

    let completed_ops = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for t_idx in 0..NUM_THREADS {
        let m = Arc::clone(&mgr);
        let counter = Arc::clone(&completed_ops);

        handles.push(thread::spawn(move || {
            for op in 0..OPERATIONS_PER_THREAD {
                // Generate a pseudo-random combination of 1 to 3 distinct IDs
                let id1 = (t_idx + op) % NUM_ITEMS;
                let id2 = (t_idx * 3 + op + 1) % NUM_ITEMS;
                let id3 = (t_idx * 7 + op + 3) % NUM_ITEMS;

                let mut ids = vec![id1];
                if id2 != id1 {
                    ids.push(id2);
                }
                if id3 != id1 && id3 != id2 {
                    ids.push(id3);
                }

                let set = m.acquire_all(&ids);

                for &id in &ids {
                    assert_eq!(*set.get(id), id * 11);
                }

                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        completed_ops.load(Ordering::SeqCst),
        NUM_THREADS * OPERATIONS_PER_THREAD
    );
}

#[test]
fn test_custom_non_copy_types() {
    #[derive(Debug, PartialEq, Eq)]
    struct ResourcePayload {
        name: String,
        count: u64,
    }

    let items = vec![
        ResourcePayload { name: "ResourceA".to_string(), count: 1 },
        ResourcePayload { name: "ResourceB".to_string(), count: 2 },
        ResourcePayload { name: "ResourceC".to_string(), count: 3 },
    ];

    let mgr = make_multi_resource_manager(items);
    let set = mgr.acquire_all(&[0, 2]);

    assert_eq!(set.get(0).name, "ResourceA");
    assert_eq!(set.get(0).count, 1);
    assert_eq!(set.get(2).name, "ResourceC");
    assert_eq!(set.get(2).count, 3);
}
