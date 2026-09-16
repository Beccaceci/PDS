use simulation_048::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let manager = make_lock_manager();
    assert_send(&manager);
    assert_sync(&manager);

    let (_id, handle) = manager.begin_tx();
    assert_send(&handle);
}

#[test]
fn test_single_tx_exclusive_lock_and_commit() {
    let manager = make_lock_manager();
    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);

    let (_id, tx) = manager.begin_tx();
    assert_eq!(manager.active_tx_count(), 1);

    assert_eq!(tx.acquire(10, LockMode::Exclusive), Ok(()));
    assert!(tx.release(10));
    // Re-releasing the same resource should return false
    assert!(!tx.release(10));

    assert!(tx.commit());
    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_concurrent_shared_locks() {
    let manager = make_lock_manager();
    let thread_count = 5;
    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::new();

    for _ in 0..thread_count {
        let mgr = manager.clone();
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let (_id, tx) = mgr.begin_tx();
            // All threads should acquire Shared on resource 42 without blocking each other
            assert_eq!(tx.acquire(42, LockMode::Shared), Ok(()));
            b.wait();
            assert!(tx.commit());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_exclusive_blocks_until_shared_holders_release() {
    let manager = make_lock_manager();
    let (_id1, tx1) = manager.begin_tx();
    let (_id2, tx2) = manager.begin_tx();

    assert_eq!(tx1.acquire(1, LockMode::Shared), Ok(()));
    assert_eq!(tx2.acquire(1, LockMode::Shared), Ok(()));

    let tx3_acquired = Arc::new(AtomicBool::new(false));
    let tx3_acquired_clone = Arc::clone(&tx3_acquired);
    let mgr_clone = manager.clone();

    let h3 = thread::spawn(move || {
        let (_id3, tx3) = mgr_clone.begin_tx();
        // Should block because tx1 and tx2 hold Shared
        assert_eq!(tx3.acquire(1, LockMode::Exclusive), Ok(()));
        tx3_acquired_clone.store(true, Ordering::SeqCst);
        assert!(tx3.commit());
    });

    thread::sleep(Duration::from_millis(30));
    assert!(
        !tx3_acquired.load(Ordering::SeqCst),
        "Exclusive lock should not be granted while Shared holders are active"
    );

    // Release tx1: tx3 should still be blocked by tx2
    assert!(tx1.release(1));
    thread::sleep(Duration::from_millis(20));
    assert!(!tx3_acquired.load(Ordering::SeqCst));

    // Release tx2: now tx3 can finally proceed
    assert!(tx2.release(1));
    h3.join().unwrap();
    assert!(tx3_acquired.load(Ordering::SeqCst));

    assert!(tx1.commit());
    assert!(tx2.commit());
    assert_eq!(manager.active_tx_count(), 0);
}

#[test]
fn test_deadlock_detection_two_transactions_cycle() {
    let manager = make_lock_manager();
    let barrier = Arc::new(Barrier::new(2));

    let mgr1 = manager.clone();
    let b1 = Arc::clone(&barrier);
    let h1 = thread::spawn(move || {
        let (_id1, tx1) = mgr1.begin_tx();
        assert_eq!(tx1.acquire(1, LockMode::Exclusive), Ok(()));
        b1.wait(); // Both hold their first resource
        thread::sleep(Duration::from_millis(20)); // Ensure tx2 also reaches wait
        let res = tx1.acquire(2, LockMode::Exclusive);
        let committed = tx1.commit();
        (res, committed)
    });

    let mgr2 = manager.clone();
    let b2 = Arc::clone(&barrier);
    let h2 = thread::spawn(move || {
        let (_id2, tx2) = mgr2.begin_tx();
        assert_eq!(tx2.acquire(2, LockMode::Exclusive), Ok(()));
        b2.wait();
        let res = tx2.acquire(1, LockMode::Exclusive);
        let committed = tx2.commit();
        (res, committed)
    });

    let (res1, comm1) = h1.join().unwrap();
    let (res2, comm2) = h2.join().unwrap();

    // Exactly one must be selected as DeadlockVictim, while the other succeeds!
    let victim_count = (if res1 == Err(LockError::DeadlockVictim) { 1 } else { 0 })
        + (if res2 == Err(LockError::DeadlockVictim) { 1 } else { 0 });
    assert_eq!(
        victim_count, 1,
        "Exactly one transaction should be chosen as victim"
    );

    if res1 == Err(LockError::DeadlockVictim) {
        assert_eq!(res2, Ok(()));
        assert!(comm2);
        assert!(!comm1);
    } else {
        assert_eq!(res1, Ok(()));
        assert!(comm1);
        assert!(!comm2);
    }

    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_deadlock_detection_three_transactions_cycle() {
    let manager = make_lock_manager();
    let barrier = Arc::new(Barrier::new(3));

    // T1: holds R1, requests R2
    // T2: holds R2, requests R3
    // T3: holds R3, requests R1
    let make_thread = |res_hold: ResourceId, res_want: ResourceId| {
        let mgr = manager.clone();
        let b = Arc::clone(&barrier);
        thread::spawn(move || {
            let (_id, tx) = mgr.begin_tx();
            assert_eq!(tx.acquire(res_hold, LockMode::Exclusive), Ok(()));
            b.wait();
            thread::sleep(Duration::from_millis(20));
            let res = tx.acquire(res_want, LockMode::Exclusive);
            let comm = tx.commit();
            (res, comm)
        })
    };

    let h1 = make_thread(10, 20);
    let h2 = make_thread(20, 30);
    let h3 = make_thread(30, 10);

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();
    let r3 = h3.join().unwrap();

    let results = [r1, r2, r3];
    let victims = results
        .iter()
        .filter(|(res, _)| *res == Err(LockError::DeadlockVictim))
        .count();
    let successes = results
        .iter()
        .filter(|(res, comm)| *res == Ok(()) && *comm)
        .count();

    // At least one victim is selected, allowing the remaining transactions to complete
    assert!(victims >= 1, "At least one victim should be aborted");
    assert_eq!(victims + successes, 3);

    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_raii_drop_aborts_and_releases_held_resources() {
    let manager = make_lock_manager();
    let acquired_after_drop = Arc::new(AtomicBool::new(false));
    let aad_clone = Arc::clone(&acquired_after_drop);
    let mgr_clone = manager.clone();

    let h = thread::spawn(move || {
        let (_id2, tx2) = mgr_clone.begin_tx();
        // Waits on resource 99
        assert_eq!(tx2.acquire(99, LockMode::Exclusive), Ok(()));
        aad_clone.store(true, Ordering::SeqCst);
        assert!(tx2.commit());
    });

    {
        let (_id1, tx1) = manager.begin_tx();
        assert_eq!(tx1.acquire(99, LockMode::Exclusive), Ok(()));
        thread::sleep(Duration::from_millis(30));
        assert!(!acquired_after_drop.load(Ordering::SeqCst));
        // tx1 exits scope without commit() -> Drop must clean up and awaken tx2!
    }

    h.join().unwrap();
    assert!(acquired_after_drop.load(Ordering::SeqCst));
    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_wait_edge_count_lifecycle() {
    let manager = make_lock_manager();
    assert_eq!(manager.wait_edge_count(), 0);

    let (_id1, tx1) = manager.begin_tx();
    assert_eq!(tx1.acquire(50, LockMode::Exclusive), Ok(()));

    let mgr2 = manager.clone();
    let h2 = thread::spawn(move || {
        let (_id2, tx2) = mgr2.begin_tx();
        let _ = tx2.acquire(50, LockMode::Exclusive);
        let _ = tx2.commit();
    });

    thread::sleep(Duration::from_millis(30));
    assert_eq!(
        manager.wait_edge_count(),
        1,
        "One wait edge should exist while tx2 waits on tx1"
    );

    assert!(tx1.commit());
    h2.join().unwrap();

    assert_eq!(manager.wait_edge_count(), 0);
}

#[test]
fn test_already_aborted_transaction_behavior() {
    let manager = make_lock_manager();
    let (_id1, tx1) = manager.begin_tx();
    let (_id2, tx2) = manager.begin_tx();

    assert_eq!(tx1.acquire(1, LockMode::Exclusive), Ok(()));
    assert_eq!(tx2.acquire(2, LockMode::Exclusive), Ok(()));

    // Force deadlock between tx1 and tx2
    let barrier = Arc::new(Barrier::new(2));
    let b_clone = Arc::clone(&barrier);

    let h = thread::spawn(move || {
        b_clone.wait();
        let res = tx2.acquire(1, LockMode::Exclusive);
        (tx2, res)
    });

    barrier.wait();
    let res1 = tx1.acquire(2, LockMode::Exclusive);
    let (tx2, res2) = h.join().unwrap();

    let (victim_tx, survivor_tx) = if res1 == Err(LockError::DeadlockVictim) {
        (tx1, tx2)
    } else {
        assert_eq!(res2, Err(LockError::DeadlockVictim));
        (tx2, tx1)
    };

    // Victim trying to acquire another resource should get AlreadyAborted
    assert_eq!(
        victim_tx.acquire(3, LockMode::Shared),
        Err(LockError::AlreadyAborted)
    );
    assert!(!victim_tx.release(1));
    assert!(!victim_tx.commit());

    assert!(survivor_tx.commit());
}

#[test]
fn test_stress_concurrent_random_transactions() {
    let manager = make_lock_manager();
    let thread_count = 12;
    let iterations_per_thread = 15;
    let resource_count = 4;
    let mut handles = Vec::new();

    for thread_idx in 0..thread_count {
        let mgr = manager.clone();
        handles.push(thread::spawn(move || {
            for i in 0..iterations_per_thread {
                let (_id, tx) = mgr.begin_tx();
                let r1 = ((thread_idx + i) % resource_count) as ResourceId;
                let r2 = ((thread_idx + i + 1) % resource_count) as ResourceId;
                let mode = if (thread_idx + i) % 2 == 0 {
                    LockMode::Shared
                } else {
                    LockMode::Exclusive
                };

                if tx.acquire(r1, mode).is_ok() {
                    thread::sleep(Duration::from_millis(2));
                    let _ = tx.acquire(r2, LockMode::Shared);
                    thread::sleep(Duration::from_millis(2));
                    let _ = tx.commit();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All transactions must eventually terminate without lingering deadlocks
    assert_eq!(manager.active_tx_count(), 0);
    assert_eq!(manager.wait_edge_count(), 0);
}
