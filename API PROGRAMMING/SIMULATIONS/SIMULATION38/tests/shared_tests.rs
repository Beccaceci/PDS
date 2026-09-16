use simulation_038::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let pool = make_hierarchical_pool(vec![1, 2, 3], 2, 2);
    assert_send(&pool);
    assert_sync(&pool);
}

#[test]
fn test_total_capacity_invariant() {
    let pool = make_hierarchical_pool(vec![10, 20, 30, 40, 50], 3, 2);
    assert_eq!(pool.total_capacity(), 5);

    let w0 = pool.local(0);
    let r1 = w0.acquire();
    assert_eq!(pool.total_capacity(), 5);
    drop(r1);
    assert_eq!(pool.total_capacity(), 5);
}

#[test]
fn test_acquire_from_overflow_returns_to_local_reserve() {
    // 2 worker, local capacity 2, 2 elementi
    let pool = make_hierarchical_pool(vec![100, 200], 2, 2);

    let w0 = pool.local(0);
    let r = w0.acquire();
    let val = *r.get();
    drop(r);

    // L'elemento rilasciato è andato nella riserva locale di w0 (perché 0 < local_capacity 2)
    // Riacquisendo da w0, dobbiamo ottenere subito l'elemento
    let r2 = w0.acquire();
    assert_eq!(*r2.get(), val);
}

#[test]
fn test_local_capacity_overflow_returns_to_shared_pool() {
    // worker_count = 2, local_capacity = 1, 2 elementi
    let pool = make_hierarchical_pool(vec![1, 2], 2, 1);

    let w0 = pool.local(0);
    let w1 = pool.local(1);

    // w0 preleva entrambi gli elementi dallo shared pool
    let r1 = w0.acquire();
    let r2 = w0.acquire();

    // w0 rilascia r1 -> va nella riserva locale di w0 (len diventa 1 == capacity)
    drop(r1);

    // w0 rilascia r2 -> riserva locale piena (1 >= 1), quindi r2 va nello shared overflow!
    drop(r2);

    // w1 deve poter prelevare r2 dallo shared overflow senza bloccarsi
    let r_w1 = w1.acquire();
    assert!(*r_w1.get() == 1 || *r_w1.get() == 2);
}

#[test]
fn test_local_reserves_are_strictly_isolated() {
    // 1 solo elemento, worker_count = 2, local_capacity = 1
    let pool = make_hierarchical_pool(vec![42], 2, 1);

    let w0 = pool.local(0);

    // w0 prende l'unico elemento e lo rilascia nella propria riserva locale
    let r = w0.acquire();
    drop(r);

    // Ora lo shared pool è vuoto, e l'elemento è nella riserva locale privata di w0.
    // w1 prova ad acquisire su un thread separato: DEVE BLOCCARSI (non deve rubare da w0!)
    let w1_acquired = Arc::new(AtomicBool::new(false));
    let w1_acq_clone = Arc::clone(&w1_acquired);

    let pool_clone = pool.clone();
    let _handle = thread::spawn(move || {
        let w1_sub = pool_clone.local(1);
        let _r_blocked = w1_sub.acquire();
        w1_acq_clone.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!w1_acquired.load(Ordering::SeqCst), "w1 must block and NOT steal from w0's local reserve");

    // w0 invece può riacquisire istantaneamente dalla propria riserva locale senza bloccarsi!
    let r_w0 = w0.acquire();
    assert_eq!(*r_w0.get(), 42);
}

#[test]
fn test_unblocking_waiter_when_item_overflows_to_shared() {
    // 1 elemento, worker_count = 2, local_capacity = 0 (ogni drop va subito a shared)
    let pool = make_hierarchical_pool(vec![99], 2, 0);

    let w0 = pool.local(0);
    let r0 = w0.acquire();

    let unblocked = Arc::new(AtomicBool::new(false));
    let ub_clone = Arc::clone(&unblocked);

    let pool_clone = pool.clone();
    let handle = thread::spawn(move || {
        let w1 = pool_clone.local(1);
        let r1 = w1.acquire();
        ub_clone.store(true, Ordering::SeqCst);
        assert_eq!(*r1.get(), 99);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!unblocked.load(Ordering::SeqCst));

    // w0 rilascia -> con local_capacity = 0, va direttamente a shared overflow e sblocca w1!
    drop(r0);

    handle.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));
}

#[test]
fn test_high_concurrency_multi_worker_stress() {
    let pool = make_hierarchical_pool((0..10).collect::<Vec<usize>>(), 4, 2);
    let mut handles = Vec::new();

    for worker_id in 0..4 {
        for _ in 0..3 {
            let pool_clone = pool.clone();
            handles.push(thread::spawn(move || {
                let local_p = pool_clone.local(worker_id);
                for _ in 0..40 {
                    let r = local_p.acquire();
                    thread::sleep(Duration::from_micros(100));
                    drop(r);
                }
            }));
        }
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(pool.total_capacity(), 10);
}
