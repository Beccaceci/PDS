use simulation_042::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let pair = make_transactional_pair::<i32>(10, 20);
    assert_send_sync(&pair);
}

#[test]
fn test_read_both_returns_initial_values() {
    let pair = make_transactional_pair::<String>("hello".into(), "world".into());
    assert_eq!(pair.read_both(), ("hello".into(), "world".into()));
}

#[test]
fn test_atomically_single_thread_commit() {
    let pair = make_transactional_pair::<i32>(100, 200);
    let outcome = pair.atomically(|a, b| (a + 10, b - 10));
    assert_eq!(outcome, (110, 190));
    assert_eq!(pair.read_both(), (110, 190));
}

#[test]
fn test_transaction_closure_executed_outside_of_lock() {
    let pair = make_transactional_pair::<i32>(1, 2);
    let pair_clone = pair.clone();

    let handle = thread::spawn(move || {
        pair_clone.atomically(|a, b| {
            thread::sleep(Duration::from_millis(50));
            (a + 10, b + 10)
        });
    });

    thread::sleep(Duration::from_millis(15));
    // read_both must not be blocked while transaction closure is computing
    let start = std::time::Instant::now();
    let (v1, v2) = pair.read_both();
    let elapsed = start.elapsed();

    assert_eq!((v1, v2), (1, 2));
    assert!(elapsed < Duration::from_millis(20), "read_both blocked on transaction closure");

    handle.join().unwrap();
    assert_eq!(pair.read_both(), (11, 12));
}

#[test]
fn test_concurrent_conflict_triggers_retry() {
    let pair = Arc::new(make_transactional_pair::<i32>(10, 20));
    let attempts = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let p1 = Arc::clone(&pair);
    let b1 = Arc::clone(&barrier);
    let att = Arc::clone(&attempts);

    let h1 = thread::spawn(move || {
        p1.atomically(|a, b| {
            let cur = att.fetch_add(1, Ordering::SeqCst);
            if cur == 0 {
                // First attempt: pause to force conflict
                b1.wait();
                thread::sleep(Duration::from_millis(40));
            }
            (a + 5, b + 5)
        })
    });

    let p2 = Arc::clone(&pair);
    let b2 = Arc::clone(&barrier);

    let h2 = thread::spawn(move || {
        b2.wait();
        // Commits quickly, changing values and causing h1's first attempt to conflict
        p2.atomically(|a, b| (a * 2, b * 2))
    });

    h2.join().unwrap();
    let res1 = h1.join().unwrap();

    // h2 did (10*2, 20*2) = (20, 40)
    // h1 retried on (20, 40) -> (25, 45)
    assert_eq!(res1, (25, 45));
    assert_eq!(pair.read_both(), (25, 45));
    assert!(attempts.load(Ordering::SeqCst) >= 2, "Transaction must have retried upon conflict");
}

#[test]
fn test_bank_transfer_invariant_stress() {
    let initial_sum = 10_000;
    let pair = Arc::new(make_transactional_pair::<i64>(5_000, 5_000));
    let mut handles = Vec::new();

    for i in 0..8 {
        let p = Arc::clone(&pair);
        handles.push(thread::spawn(move || {
            for j in 0..60 {
                if (i + j) % 2 == 0 {
                    p.atomically(|a, b| (a - 10, b + 10));
                } else {
                    p.atomically(|a, b| (a + 10, b - 10));
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let (a, b) = pair.read_both();
    assert_eq!(a + b, initial_sum, "Total balance must remain invariant");
}