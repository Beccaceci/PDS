use simulation_033::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_local_lifo_push_pop() {
    let pool = make_work_stealing_pool::<i32>(2);
    let q0 = pool.queue(0);

    assert_eq!(q0.pop(), None);

    q0.push(10);
    q0.push(20);
    q0.push(30);

    // LIFO order for owner
    assert_eq!(q0.pop(), Some(30));
    assert_eq!(q0.pop(), Some(20));
    assert_eq!(q0.pop(), Some(10));
    assert_eq!(q0.pop(), None);
}

#[test]
fn test_steal_fifo_order() {
    let pool = make_work_stealing_pool::<i32>(2);
    let q0 = pool.queue(0);

    q0.push(100);
    q0.push(200);
    q0.push(300);

    // FIFO order for thief
    assert_eq!(q0.steal(), Some(100));
    assert_eq!(q0.steal(), Some(200));
    assert_eq!(q0.steal(), Some(300));
    assert_eq!(q0.steal(), None);
}

#[test]
fn test_pop_and_steal_single_item_concurrency() {
    for _ in 0..50 {
        let pool = make_work_stealing_pool::<i32>(2);
        let q0 = pool.queue(0);
        q0.push(42);

        let q0_clone = pool.queue(0);

        let h_pop = thread::spawn(move || q0.pop());
        let h_steal = thread::spawn(move || q0_clone.steal());

        let res_pop = h_pop.join().unwrap();
        let res_steal = h_steal.join().unwrap();

        // Exactly one must succeed, the other must get None
        match (res_pop, res_steal) {
            (Some(42), None) => {}
            (None, Some(42)) => {}
            _ => panic!("Expected exactly one winner: pop={:?}, steal={:?}", res_pop, res_steal),
        }
    }
}

#[test]
fn test_steal_any_skips_own_and_rotates() {
    let pool = make_work_stealing_pool::<&'static str>(3);
    let q1 = pool.queue(1);
    let q2 = pool.queue(2);

    q1.push("from_1");
    q2.push("from_2");

    // Worker 0 steals from any other queue (1 or 2)
    let steal_1 = pool.steal_any(0);
    assert!(steal_1.is_some());
    let (victim_1, task_1) = steal_1.unwrap();
    assert!(victim_1 == 1 || victim_1 == 2);
    assert!(task_1 == "from_1" || task_1 == "from_2");

    let steal_2 = pool.steal_any(0);
    assert!(steal_2.is_some());
    let (victim_2, task_2) = steal_2.unwrap();
    assert_ne!(victim_1, victim_2);
    assert_ne!(task_1, task_2);

    // Both queues now empty
    assert_eq!(pool.steal_any(0), None);
}

#[test]
fn test_wait_for_work_unblocks_on_push() {
    let pool = make_work_stealing_pool::<i32>(2);

    let unblocked = Arc::new(AtomicBool::new(false));
    let pool_clone = pool.clone();
    let ub = Arc::clone(&unblocked);

    let handle = thread::spawn(move || {
        pool_clone.wait_for_work();
        ub.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!unblocked.load(Ordering::SeqCst), "Must block until work arrives");

    // Push work onto queue 1
    let q1 = pool.queue(1);
    q1.push(999);

    handle.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst), "Must unblock when work is pushed");
}

#[test]
fn test_high_concurrency_work_stealing_stress() {
    let pool = make_work_stealing_pool::<usize>(4);
    let items_per_worker = 100;
    let total_executed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for worker_id in 0..4 {
        let p = pool.clone();
        let q = pool.queue(worker_id);
        let te = Arc::clone(&total_executed);

        handles.push(thread::spawn(move || {
            // Push local tasks
            for i in 0..items_per_worker {
                q.push(worker_id * 1000 + i);
            }

            let mut processed = 0;
            while processed < items_per_worker {
                // Try local pop first (LIFO)
                if let Some(_) = q.pop() {
                    processed += 1;
                    te.fetch_add(1, Ordering::Relaxed);
                } else if let Some((_, _)) = p.steal_any(worker_id) {
                    // Steal from others
                    processed += 1;
                    te.fetch_add(1, Ordering::Relaxed);
                } else {
                    thread::yield_now();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        total_executed.load(Ordering::SeqCst),
        4 * items_per_worker,
        "All pushed tasks must be executed without loss or duplication"
    );
}

#[test]
fn test_send_sync_bounds() {
    let pool = make_work_stealing_pool::<i32>(2);
    let q = pool.queue(0);

    assert_send(&pool);
    assert_sync(&pool);
    assert_send(&q);
    assert_sync(&q);
}
