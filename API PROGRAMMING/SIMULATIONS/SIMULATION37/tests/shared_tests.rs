use simulation_037::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let lock = make_priority_lock(42);
    assert_send(&lock);
    assert_sync(&lock);
}

#[test]
fn test_uncontended_acquire_and_release() {
    let lock = make_priority_lock(100);

    // Initial state: free
    assert_eq!(lock.current_holder_priority(), None);

    {
        let mut guard = lock.acquire(10);
        assert_eq!(lock.current_holder_priority(), Some(10));
        assert_eq!(*guard.get(), 100);
        *guard.get_mut() = 200;
        assert_eq!(*guard.get(), 200);
    }

    // After drop, lock is free again
    assert_eq!(lock.current_holder_priority(), None);

    {
        let guard = lock.acquire(25);
        assert_eq!(lock.current_holder_priority(), Some(25));
        assert_eq!(*guard.get(), 200);
    }
}

#[test]
fn test_immediate_priority_elevation_while_waiting() {
    let lock = make_priority_lock(0);

    // Step 1: Thread A acquires with low priority (10)
    let mut guard_a = Some(lock.acquire(10));
    assert_eq!(lock.current_holder_priority(), Some(10));

    // Step 2: Thread B tries to acquire with priority 50 and blocks
    let lock_b = lock.clone();
    let b_acquired = Arc::new(AtomicBool::new(false));
    let b_acq_clone = Arc::clone(&b_acquired);
    let handle_b = thread::spawn(move || {
        let _guard = lock_b.acquire(50);
        b_acq_clone.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(40));
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!b_acquired.load(Ordering::SeqCst));
    // Thread A's effective priority is immediately boosted to 50!
    assert_eq!(
        lock.current_holder_priority(),
        Some(50),
        "Holder priority must immediately boost to waiting caller's priority (50)"
    );

    // Step 3: Thread C tries to acquire with priority 90 and blocks
    let lock_c = lock.clone();
    let c_acquired = Arc::new(AtomicBool::new(false));
    let c_acq_clone = Arc::clone(&c_acquired);
    let handle_c = thread::spawn(move || {
        let _guard = lock_c.acquire(90);
        c_acq_clone.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(40));
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!c_acquired.load(Ordering::SeqCst));
    // Thread A's effective priority is now boosted to 90!
    assert_eq!(
        lock.current_holder_priority(),
        Some(90),
        "Holder priority must immediately boost to 90"
    );

    // Step 4: Thread A drops its guard
    drop(guard_a.take());

    // Thread C (priority 90) must acquire first (highest priority waiter wins!)
    thread::sleep(Duration::from_millis(20));
    assert!(c_acquired.load(Ordering::SeqCst));
    assert!(!b_acquired.load(Ordering::SeqCst));

    // While C holds the lock (acq 90) and B is waiting (50), holder priority is max(90, 50) = 90
    assert_eq!(lock.current_holder_priority(), Some(90));

    handle_c.join().unwrap();

    // Now Thread B (priority 50) acquires
    thread::sleep(Duration::from_millis(20));
    assert!(b_acquired.load(Ordering::SeqCst));
    assert_eq!(
        lock.current_holder_priority(),
        Some(50),
        "Holder priority must be 50 once C finishes and B holds the lock"
    );

    handle_b.join().unwrap();

    // Finally free
    assert_eq!(lock.current_holder_priority(), None);
}

#[test]
fn test_highest_priority_waiter_selected_over_arrival_order() {
    let lock = make_priority_lock(Vec::new());

    let mut guard = Some(lock.acquire(5));

    let mut handles = Vec::new();
    let priorities = vec![20, 80, 40, 95, 30];

    for &p in &priorities {
        let l = lock.clone();
        handles.push(thread::spawn(move || {
            let mut g = l.acquire(p);
            g.get_mut().push(p);
            thread::sleep(Duration::from_millis(20));
        }));
        // Give time for each thread to block and enter waiting set in sequence
        thread::sleep(Duration::from_millis(15));
    }

    // Release initial lock
    drop(guard.take());

    for h in handles {
        h.join().unwrap();
    }

    // Check acquisition order: must be sorted by priority descending: [95, 80, 40, 30, 20]
    let final_guard = lock.acquire(1);
    assert_eq!(
        *final_guard.get(),
        vec![95, 80, 40, 30, 20],
        "Waiters must acquire in strict priority order regardless of arrival order"
    );
}

#[test]
fn test_waiter_with_lower_priority_does_not_lower_holder_priority() {
    let lock = make_priority_lock("data");

    let guard = lock.acquire(100);
    assert_eq!(lock.current_holder_priority(), Some(100));

    let lock_clone = lock.clone();
    let handle = thread::spawn(move || {
        let _g = lock_clone.acquire(20);
    });

    thread::sleep(Duration::from_millis(25));
    // Holder has 100, waiter has 20 -> holder effective priority is max(100, 20) = 100
    assert_eq!(lock.current_holder_priority(), Some(100));

    drop(guard);
    handle.join().unwrap();
    assert_eq!(lock.current_holder_priority(), None);
}

#[test]
fn test_concurrent_multi_thread_stress() {
    let lock = make_priority_lock(0u64);
    let mut handles = Vec::new();

    for thread_id in 0..10 {
        let l = lock.clone();
        handles.push(thread::spawn(move || {
            for i in 0..30 {
                let priority = ((thread_id * 17 + i * 3) % 100) as u32 + 1;
                let mut guard = l.acquire(priority);
                *guard.get_mut() += 1;
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_guard = lock.acquire(1);
    assert_eq!(*final_guard.get(), 300);
}
