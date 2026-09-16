use simulation_028::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_per_tenant_limit_enforced() {
    let qm = make_quota_manager(2, 10);

    let _p1 = qm.acquire("tenant_A");
    let _p2 = qm.acquire("tenant_A");

    let third_acquired = Arc::new(AtomicBool::new(false));
    let qm_clone = qm.clone();
    let ta = Arc::clone(&third_acquired);
    let handle = thread::spawn(move || {
        let _p3 = qm_clone.acquire("tenant_A");
        ta.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!third_acquired.load(Ordering::SeqCst));

    drop(_p1);
    handle.join().unwrap();
    assert!(third_acquired.load(Ordering::SeqCst));
}

#[test]
fn test_global_limit_enforced_across_different_tenants() {
    let qm = make_quota_manager(5, 3); // Per-tenant limit is 5, but global limit is only 3

    let _p1 = qm.acquire("tenant_A");
    let _p2 = qm.acquire("tenant_B");
    let _p3 = qm.acquire("tenant_C");

    let fourth_acquired = Arc::new(AtomicBool::new(false));
    let qm_clone = qm.clone();
    let fa = Arc::clone(&fourth_acquired);
    let handle = thread::spawn(move || {
        let _p4 = qm_clone.acquire("tenant_D");
        fa.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(
        !fourth_acquired.load(Ordering::SeqCst),
        "Global limit reached -> fourth tenant must block"
    );

    drop(_p2);
    handle.join().unwrap();
    assert!(fourth_acquired.load(Ordering::SeqCst));
}

#[test]
fn test_drop_permit_releases_capacity() {
    let qm = make_quota_manager(1, 1);

    let p1 = qm.acquire("t1");
    let unblocked = Arc::new(AtomicBool::new(false));

    let qm_clone = qm.clone();
    let ub = Arc::clone(&unblocked);
    let handle = thread::spawn(move || {
        let _p2 = qm_clone.acquire("t1");
        ub.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!unblocked.load(Ordering::SeqCst));

    drop(p1);
    handle.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));
}

#[test]
fn test_high_concurrency_multi_tenant_stress() {
    let qm = make_quota_manager(3, 8);
    let active_global = Arc::new(AtomicUsize::new(0));
    let max_observed_global = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for thread_id in 0..16 {
        let qm_clone = qm.clone();
        let ag = Arc::clone(&active_global);
        let mog = Arc::clone(&max_observed_global);
        let tenant_name = format!("tenant_{}", thread_id % 4);

        handles.push(thread::spawn(move || {
            for _ in 0..20 {
                let permit = qm_clone.acquire(&tenant_name);
                let current = ag.fetch_add(1, Ordering::SeqCst) + 1;
                mog.fetch_max(current, Ordering::SeqCst);

                thread::sleep(Duration::from_millis(2));

                ag.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(max_observed_global.load(Ordering::SeqCst) <= 8);
}

#[test]
fn test_send_sync_bounds() {
    let qm = make_quota_manager(2, 4);
    assert_send(&qm);
    assert_sync(&qm);
}
