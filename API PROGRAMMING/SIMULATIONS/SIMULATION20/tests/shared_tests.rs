use simulation_020::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_on_tick_accumulates_mutating_state() {
    let service = make_ticker_service();
    let tick_count = Arc::new(AtomicUsize::new(0));

    let tc = Arc::clone(&tick_count);
    let mut local_accumulator = 0;
    service.on_tick(move || {
        local_accumulator += 1;
        tc.store(local_accumulator, Ordering::SeqCst);
    });

    service.tick();
    assert_eq!(tick_count.load(Ordering::SeqCst), 1);

    service.tick();
    assert_eq!(tick_count.load(Ordering::SeqCst), 2);

    service.tick();
    assert_eq!(tick_count.load(Ordering::SeqCst), 3);
}

#[test]
fn test_multiple_on_tick_registration_order() {
    let service = make_ticker_service();
    let execution_log = Arc::new(Mutex::new(Vec::new()));

    let l1 = Arc::clone(&execution_log);
    service.on_tick(move || {
        l1.lock().unwrap().push("first");
    });

    let l2 = Arc::clone(&execution_log);
    service.on_tick(move || {
        l2.lock().unwrap().push("second");
    });

    service.tick();

    assert_eq!(*execution_log.lock().unwrap(), vec!["first", "second"]);
}

#[test]
fn test_on_stop_invoked_exactly_once() {
    let service = make_ticker_service();
    let stop_count = Arc::new(AtomicUsize::new(0));

    let sc = Arc::clone(&stop_count);
    service.on_stop(move || {
        sc.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(stop_count.load(Ordering::SeqCst), 0);

    service.stop();
    assert_eq!(stop_count.load(Ordering::SeqCst), 1);

    // Repeated calls to stop() must NOT invoke callback again
    service.stop();
    service.stop();
    assert_eq!(stop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_tick_after_stop_is_no_op() {
    let service = make_ticker_service();
    let ticks = Arc::new(AtomicUsize::new(0));

    let t = Arc::clone(&ticks);
    service.on_tick(move || {
        t.fetch_add(1, Ordering::SeqCst);
    });

    service.tick();
    assert_eq!(ticks.load(Ordering::SeqCst), 1);

    service.stop();

    // After stop(), tick() must have no effect
    service.tick();
    service.tick();
    assert_eq!(ticks.load(Ordering::SeqCst), 1);
}

#[test]
fn test_cloned_services_share_state() {
    let s1 = make_ticker_service();
    let s2 = s1.clone();

    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);

    s1.on_tick(move || {
        c.fetch_add(1, Ordering::SeqCst);
    });

    s2.tick();
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    let stopped = Arc::new(AtomicBool::new(false));
    let st = Arc::clone(&stopped);
    s2.on_stop(move || {
        st.store(true, Ordering::SeqCst);
    });

    s1.stop();
    assert!(stopped.load(Ordering::SeqCst));
}

#[test]
fn test_concurrent_ticks_and_stops() {
    let service = make_ticker_service();
    let total_ticks = Arc::new(AtomicUsize::new(0));

    let tt = Arc::clone(&total_ticks);
    service.on_tick(move || {
        tt.fetch_add(1, Ordering::SeqCst);
    });

    let stop_called = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&stop_called);
    service.on_stop(move || {
        sc.fetch_add(1, Ordering::SeqCst);
    });

    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = service.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                s.tick();
            }
        }));
    }

    let s_stop = service.clone();
    let stop_handle = thread::spawn(move || {
        s_stop.stop();
    });

    for h in handles {
        h.join().unwrap();
    }
    stop_handle.join().unwrap();

    assert_eq!(stop_called.load(Ordering::SeqCst), 1);
}

#[test]
fn test_reentrant_callback_no_deadlock() {
    let service = make_ticker_service();
    let service_clone = service.clone();
    let dynamic_called = Arc::new(AtomicBool::new(false));

    let dc = Arc::clone(&dynamic_called);
    // All'interno di on_tick, si registra dinamicamente un'altra callback sul servizio
    // Se il lock globale fosse trattenuto, questo causerebbe un deadlock immediato!
    service.on_tick(move || {
        let dc2 = Arc::clone(&dc);
        service_clone.on_tick(move || {
            dc2.store(true, Ordering::SeqCst);
        });
    });

    service.tick();
    assert!(!dynamic_called.load(Ordering::SeqCst));

    // Al secondo tick, la callback registrata dinamicamente viene eseguita
    service.tick();
    assert!(dynamic_called.load(Ordering::SeqCst));
}

#[test]
fn test_send_sync_bounds() {
    let service = make_ticker_service();
    assert_send(&service);
    assert_sync(&service);
}
