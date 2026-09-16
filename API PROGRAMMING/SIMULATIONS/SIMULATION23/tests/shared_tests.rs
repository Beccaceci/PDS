use simulation_023::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_closed_successful_calls() {
    let cb = make_circuit_breaker::<i32>(3, Duration::from_millis(50));

    let res1 = cb.call(|| Ok(42));
    assert_eq!(res1, Ok(42));

    let res2 = cb.call(|| Ok(100));
    assert_eq!(res2, Ok(100));
}

#[test]
fn test_threshold_failures_trips_circuit_open() {
    let cb = make_circuit_breaker::<i32>(3, Duration::from_millis(100));
    let attempts = Arc::new(AtomicUsize::new(0));

    // 1st failure: closure is executed
    let a1 = Arc::clone(&attempts);
    let _ = cb.call(move || {
        a1.fetch_add(1, Ordering::SeqCst);
        Err(())
    });
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // 2nd failure: closure is executed
    let a2 = Arc::clone(&attempts);
    let _ = cb.call(move || {
        a2.fetch_add(1, Ordering::SeqCst);
        Err(())
    });
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // 3rd failure: reaches threshold -> circuit trips to Open
    let a3 = Arc::clone(&attempts);
    let _ = cb.call(move || {
        a3.fetch_add(1, Ordering::SeqCst);
        Err(())
    });
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    // 4th call: circuit is OPEN -> fails immediately without executing closure!
    let a4 = Arc::clone(&attempts);
    let res = cb.call(move || {
        a4.fetch_add(1, Ordering::SeqCst);
        Ok(999)
    });

    assert_eq!(res, Err(CallError::CircuitOpen));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "Closure must NOT be executed when circuit is open"
    );
}

#[test]
fn test_success_resets_consecutive_failures() {
    let cb = make_circuit_breaker::<i32>(2, Duration::from_millis(100));

    // 1 failure
    let _ = cb.call(|| Err(()));

    // 1 success -> resets consecutive failures to 0
    let res = cb.call(|| Ok(10));
    assert_eq!(res, Ok(10));

    // 1 failure again -> does not trip (needs 2 consecutive)
    let _ = cb.call(|| Err(()));

    let res2 = cb.call(|| Ok(20));
    assert_eq!(res2, Ok(20));
}

#[test]
fn test_cooldown_and_successful_probe_resets_to_closed() {
    let cb = make_circuit_breaker::<&'static str>(2, Duration::from_millis(50));

    // Trip circuit to Open
    let _ = cb.call(|| Err(()));
    let _ = cb.call(|| Err(()));

    // Immediate call -> rejected
    assert_eq!(cb.call(|| Ok("service")), Err(CallError::CircuitOpen));

    // Sleep until cooldown expires
    thread::sleep(Duration::from_millis(80));

    // Probe call is allowed to execute
    let probe_executed = Arc::new(AtomicBool::new(false));
    let pe = Arc::clone(&probe_executed);
    let res = cb.call(move || {
        pe.store(true, Ordering::SeqCst);
        Ok("service recovered")
    });

    assert_eq!(res, Ok("service recovered"));
    assert!(probe_executed.load(Ordering::SeqCst));

    // Circuit is now CLOSED again: subsequent calls work normally
    assert_eq!(cb.call(|| Ok("healthy")), Ok("healthy"));
}

#[test]
fn test_cooldown_and_failed_probe_restarts_cooldown() {
    let cb = make_circuit_breaker::<i32>(1, Duration::from_millis(50));

    // Trip circuit immediately
    let _ = cb.call(|| Err(()));
    assert_eq!(cb.call(|| Ok(1)), Err(CallError::CircuitOpen));

    // Wait cooldown
    thread::sleep(Duration::from_millis(70));

    // Probe call runs and FAILS
    let probe_called = Arc::new(AtomicBool::new(false));
    let pc = Arc::clone(&probe_called);
    let _ = cb.call(move || {
        pc.store(true, Ordering::SeqCst);
        Err(())
    });
    assert!(probe_called.load(Ordering::SeqCst));

    // Circuit must be back to OPEN with a fresh cooldown starting now
    let rejected_immediately = cb.call(|| Ok(99));
    assert_eq!(rejected_immediately, Err(CallError::CircuitOpen));

    // After waiting new cooldown, a new probe is allowed
    thread::sleep(Duration::from_millis(70));
    let recovered = cb.call(|| Ok(200));
    assert_eq!(recovered, Ok(200));
}

#[test]
fn test_concurrent_probe_only_one_winner() {
    let cb = make_circuit_breaker::<i32>(1, Duration::from_millis(50));

    // Trip circuit to Open
    let _ = cb.call(|| Err(()));

    // Wait cooldown
    thread::sleep(Duration::from_millis(70));

    // Launch 10 threads trying to call at the same time
    let probe_executions = Arc::new(AtomicUsize::new(0));
    let open_rejections = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let cb_clone = cb.clone();
        let pe = Arc::clone(&probe_executions);
        let ore = Arc::clone(&open_rejections);
        handles.push(thread::spawn(move || {
            let res = cb_clone.call(|| {
                pe.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(30));
                Ok(123)
            });
            if res == Err(CallError::CircuitOpen) {
                ore.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        probe_executions.load(Ordering::SeqCst),
        1,
        "Exactly ONE concurrent caller must be allowed to execute the probe!"
    );
    assert_eq!(
        open_rejections.load(Ordering::SeqCst),
        9,
        "All other 9 concurrent callers must be rejected immediately with CircuitOpen"
    );
}

#[test]
fn test_parallel_execution_when_closed() {
    let cb = make_circuit_breaker::<()>(5, Duration::from_secs(1));
    let concurrent_running = Arc::new(AtomicUsize::new(0));
    let max_concurrency = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let cb_clone = cb.clone();
        let cr = Arc::clone(&concurrent_running);
        let mc = Arc::clone(&max_concurrency);
        handles.push(thread::spawn(move || {
            let _ = cb_clone.call(|| {
                let current = cr.fetch_add(1, Ordering::SeqCst) + 1;
                mc.fetch_max(current, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(50));
                cr.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            });
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        max_concurrency.load(Ordering::SeqCst) >= 2,
        "Closed circuit breaker must allow concurrent calls to execute in parallel without lock serialisation!"
    );
}

#[test]
fn test_lock_free_circuit_breaker_probe_and_cooldown() {
    use simulation_023::lock_free_circuit_breaker::LockFreeCircuitBreaker;

    let cb = LockFreeCircuitBreaker::new(2, Duration::from_millis(50));

    // Trip circuit with 2 failures
    assert_eq!(cb.call(|| Err::<&'static str, ()>(())), Err(CallError::CircuitOpen));
    assert_eq!(cb.call(|| Err::<&'static str, ()>(())), Err(CallError::CircuitOpen));

    // Wait for cooldown
    thread::sleep(Duration::from_millis(60));

    let probe_executions = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let cb_clone = cb.clone();
        let pe = Arc::clone(&probe_executions);
        handles.push(thread::spawn(move || {
            let _ = cb_clone.call(|| {
                pe.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(20));
                Ok("recovered")
            });
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        probe_executions.load(Ordering::SeqCst),
        1,
        "LockFreeCircuitBreaker must also guarantee single-winner probe!"
    );
}
