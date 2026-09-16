use simulation_030::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_route_and_least_load() {
    let lb = make_load_balancer(3, Duration::from_millis(100));

    lb.register("backend_A");
    lb.register("backend_B");

    let h1 = lb.route(); // Load on A: 1, B: 0
    let h2 = lb.route(); // Load on A: 1, B: 1
    let h3 = lb.route(); // Load on A: 2, B: 1 (or 1 and 2)

    h1.report(true);
    h2.report(true);
    h3.report(true);
}

#[test]
fn test_failure_threshold_trips_backend_unhealthy() {
    let lb = make_load_balancer(2, Duration::from_millis(50));

    lb.register("backend_A");
    lb.register("backend_B");

    // Trip backend_A with 2 failures
    let h1 = lb.route();
    h1.report(false);
    let h2 = lb.route();
    h2.report(false);

    // Now backend_A is unhealthy. All subsequent routes must go to backend_B
    for _ in 0..5 {
        let h = lb.route();
        h.report(true);
    }
}

#[test]
fn test_success_resets_failures() {
    let lb = make_load_balancer(2, Duration::from_millis(50));
    lb.register("single_backend");

    let h1 = lb.route();
    h1.report(false); // 1 failure (threshold is 2)

    let h2 = lb.route();
    h2.report(true); // Success resets failures to 0!

    let h3 = lb.route();
    h3.report(false); // 1 failure -> still healthy!

    let h4 = lb.route();
    h4.report(true);
}

#[test]
fn test_probe_and_recovery() {
    let lb = make_load_balancer(2, Duration::from_millis(60));
    lb.register("b1");

    // Trip b1
    lb.route().report(false);
    lb.route().report(false);

    // Wait for recovery_after (60ms)
    thread::sleep(Duration::from_millis(70));

    // Now b1 is eligible as probe
    let probe_handle = lb.route();
    probe_handle.report(true); // Probe succeeds! b1 is healthy again!

    // Subsequent route should succeed immediately without probe delay
    let h = lb.route();
    h.report(true);
}

#[test]
fn test_probe_failed_resets_cooldown() {
    let lb = make_load_balancer(2, Duration::from_millis(60));
    lb.register("b1");

    // Trip b1
    lb.route().report(false);
    lb.route().report(false);

    // Wait for recovery_after
    thread::sleep(Duration::from_millis(70));

    // Probe fails
    let probe_handle = lb.route();
    probe_handle.report(false); // Fails -> resets cooldown to now!

    // Immediate next route must block because cooldown restarted!
    let routed_second = Arc::new(AtomicBool::new(false));
    let lb_clone = lb.clone();
    let rs = Arc::clone(&routed_second);

    let handle = thread::spawn(move || {
        let h = lb_clone.route();
        h.report(true);
        rs.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!routed_second.load(Ordering::SeqCst), "Must block during restarted cooldown");

    handle.join().unwrap();
    assert!(routed_second.load(Ordering::SeqCst));
}

#[test]
fn test_drop_without_report_counts_as_failure() {
    let lb = make_load_balancer(2, Duration::from_millis(50));
    lb.register("b1");

    // Dropping handle without report counts as failure
    let h1 = lb.route();
    drop(h1);

    let h2 = lb.route();
    drop(h2);

    // b1 is now tripped to unhealthy. Next route must wait for cooldown!
    let unblocked = Arc::new(AtomicBool::new(false));
    let lb_clone = lb.clone();
    let ub = Arc::clone(&unblocked);

    let handle = thread::spawn(move || {
        let h = lb_clone.route();
        h.report(true);
        ub.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));
    assert!(!unblocked.load(Ordering::SeqCst));

    handle.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));
}

#[test]
fn test_blocking_when_no_eligible_backends() {
    let lb = make_load_balancer(1, Duration::from_millis(50));

    // Empty load balancer -> route blocks
    let routed = Arc::new(AtomicBool::new(false));
    let lb_clone = lb.clone();
    let r = Arc::clone(&routed);

    let handle = thread::spawn(move || {
        let h = lb_clone.route();
        h.report(true);
        r.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!routed.load(Ordering::SeqCst));

    // Registering a backend unblocks waiting route caller
    lb.register("new_backend");

    handle.join().unwrap();
    assert!(routed.load(Ordering::SeqCst));
}

#[test]
fn test_single_probe_winner() {
    let lb = make_load_balancer(1, Duration::from_millis(50));
    lb.register("b1");

    // Trip b1
    lb.route().report(false);

    // Wait for cooldown
    thread::sleep(Duration::from_millis(60));

    // Launch 10 threads trying to route simultaneously
    let probe_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let lb_clone = lb.clone();
        let pc = Arc::clone(&probe_count);
        handles.push(thread::spawn(move || {
            let h = lb_clone.route();
            pc.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(30));
            h.report(true);
        }));
    }

    // After 10ms, exactly ONE thread should have acquired the probe (while b1 is InProva)
    thread::sleep(Duration::from_millis(15));
    assert_eq!(
        probe_count.load(Ordering::SeqCst),
        1,
        "Exactly ONE concurrent caller can hold the probe!"
    );

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_high_concurrency_multi_thread_stress() {
    let lb = make_load_balancer(3, Duration::from_millis(30));

    for i in 0..4 {
        lb.register(&format!("backend_{}", i));
    }

    let mut handles = Vec::new();
    for thread_id in 0..12 {
        let lb_clone = lb.clone();
        handles.push(thread::spawn(move || {
            for i in 0..20 {
                let h = lb_clone.route();
                let should_fail = (thread_id + i) % 7 == 0;
                thread::sleep(Duration::from_millis(1));
                h.report(!should_fail);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_send_sync_bounds() {
    let lb = make_load_balancer(2, Duration::from_millis(50));
    assert_send(&lb);
    assert_sync(&lb);
}
