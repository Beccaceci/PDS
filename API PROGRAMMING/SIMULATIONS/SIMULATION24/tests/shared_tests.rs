use simulation_024::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_acquire_and_drop_release() {
    let manager = make_lease_lock_manager();

    let lease1 = manager.acquire("resource_1", Duration::from_secs(10));
    let acquired_second = Arc::new(AtomicBool::new(false));

    let mgr_clone = manager.clone();
    let acq2 = Arc::clone(&acquired_second);
    let handle = thread::spawn(move || {
        let _lease2 = mgr_clone.acquire("resource_1", Duration::from_secs(10));
        acq2.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!acquired_second.load(Ordering::SeqCst));

    // Dropping lease1 must immediately release the lock
    drop(lease1);

    handle.join().unwrap();
    assert!(acquired_second.load(Ordering::SeqCst));
}

#[test]
fn test_automatic_lease_expiration() {
    let manager = make_lease_lock_manager();

    // Acquire with short 60ms lease
    let _lease1 = manager.acquire("auto_expire", Duration::from_millis(60));
    let acquired_second = Arc::new(AtomicBool::new(false));

    let mgr_clone = manager.clone();
    let acq2 = Arc::clone(&acquired_second);
    let handle = thread::spawn(move || {
        let _lease2 = mgr_clone.acquire("auto_expire", Duration::from_secs(5));
        acq2.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!acquired_second.load(Ordering::SeqCst));

    // Wait for lease1 to expire automatically without drop
    handle.join().unwrap();
    assert!(acquired_second.load(Ordering::SeqCst));
}

#[test]
fn test_renew_extends_lease() {
    let manager = make_lease_lock_manager();

    let lease1 = manager.acquire("renew_test", Duration::from_millis(60));
    let acquired_second = Arc::new(AtomicBool::new(false));

    let mgr_clone = manager.clone();
    let acq2 = Arc::clone(&acquired_second);
    let handle = thread::spawn(move || {
        let _lease2 = mgr_clone.acquire("renew_test", Duration::from_secs(5));
        acq2.store(true, Ordering::SeqCst);
    });

    // Before 60ms expires, renew by another 100ms
    thread::sleep(Duration::from_millis(30));
    assert!(lease1.renew(Duration::from_millis(100)));

    // At 70ms (after original expiration), lock is still held
    thread::sleep(Duration::from_millis(40));
    assert!(!acquired_second.load(Ordering::SeqCst));

    // Drop lease1 to let thread2 finish
    drop(lease1);
    handle.join().unwrap();
    assert!(acquired_second.load(Ordering::SeqCst));
}

#[test]
fn test_renew_after_expiration_fails() {
    let manager = make_lease_lock_manager();

    let lease1 = manager.acquire("stale_renew", Duration::from_millis(40));

    // Wait for expiration
    thread::sleep(Duration::from_millis(80));

    // Renew must return false
    assert!(!lease1.renew(Duration::from_millis(100)));
}

#[test]
fn test_drop_stale_lease_does_not_release_new_owner() {
    let manager = make_lease_lock_manager();

    let lease1 = manager.acquire("aba_test", Duration::from_millis(40));

    // Wait for lease1 to expire
    thread::sleep(Duration::from_millis(80));

    // Thread 2 acquires the lock
    let lease2 = manager.acquire("aba_test", Duration::from_secs(10));

    // Stale lease1 dropped now
    drop(lease1);

    // Thread 3 tries to acquire, must still be blocked because lease2 is alive!
    let thread3_acquired = Arc::new(AtomicBool::new(false));
    let mgr_clone = manager.clone();
    let t3_acq = Arc::clone(&thread3_acquired);
    let handle = thread::spawn(move || {
        let _lease3 = mgr_clone.acquire("aba_test", Duration::from_secs(5));
        t3_acq.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(
        !thread3_acquired.load(Ordering::SeqCst),
        "Dropping stale lease1 must NOT release the lock held by lease2!"
    );

    drop(lease2);
    handle.join().unwrap();
    assert!(thread3_acquired.load(Ordering::SeqCst));
}

#[test]
fn test_independent_lock_names() {
    let manager = make_lease_lock_manager();

    let _lease_a = manager.acquire("res_a", Duration::from_secs(10));
    let _lease_b = manager.acquire("res_b", Duration::from_secs(10));
}

#[test]
fn test_concurrent_multi_thread_stress() {
    let manager = make_lease_lock_manager();
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..8 {
        let mgr = manager.clone();
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let lease = mgr.acquire("shared_counter", Duration::from_millis(50));
                let val = c.load(Ordering::SeqCst);
                thread::sleep(Duration::from_millis(2));
                c.store(val + 1, Ordering::SeqCst);
                drop(lease);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 80);
}

#[test]
fn test_send_sync_bounds() {
    let manager = make_lease_lock_manager();
    assert_send(&manager);
    assert_sync(&manager);
}
