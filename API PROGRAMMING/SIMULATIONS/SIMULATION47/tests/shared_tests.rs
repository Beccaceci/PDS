use simulation_047::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let sig = make_signal::<i32>();
    assert_send(&sig);
    assert_sync(&sig);
}

#[test]
fn test_basic_connect_and_emit() {
    let sig = make_signal::<String>();
    let received = Arc::new(Mutex::new(Vec::new()));

    let r1 = Arc::clone(&received);
    let _c1 = sig.connect(move |val| {
        r1.lock().unwrap().push(format!("slot1:{}", val));
    });

    let r2 = Arc::clone(&received);
    let _c2 = sig.connect(move |val| {
        r2.lock().unwrap().push(format!("slot2:{}", val));
    });

    sig.emit("hello".to_string());

    let res = received.lock().unwrap().clone();
    assert_eq!(res, vec!["slot1:hello", "slot2:hello"]);
}

#[test]
fn test_disconnect_prevents_future_invocations() {
    let sig = make_signal::<i32>();
    let counter = Arc::new(AtomicUsize::new(0));

    let c_clone = Arc::clone(&counter);
    let conn = sig.connect(move |_| {
        c_clone.fetch_add(1, Ordering::SeqCst);
    });

    sig.emit(10);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    conn.disconnect();

    sig.emit(20);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Multiple disconnects have no effect
    conn.disconnect();
    sig.emit(30);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn test_self_disconnect_during_emit() {
    let sig = Arc::new(make_signal::<usize>());
    let call_count = Arc::new(AtomicUsize::new(0));

    let conn_cell: Arc<Mutex<Option<Box<dyn Connection>>>> = Arc::new(Mutex::new(None));
    let conn_cell_clone = Arc::clone(&conn_cell);
    let cc = Arc::clone(&call_count);

    let conn = sig.connect(move |_val| {
        cc.fetch_add(1, Ordering::SeqCst);
        // Self-disconnect inside the callback!
        if let Some(ref c) = *conn_cell_clone.lock().unwrap() {
            c.disconnect();
        }
    });

    *conn_cell.lock().unwrap() = Some(Box::new(conn));

    sig.emit(1);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second emit: this slot should not be invoked again
    sig.emit(2);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_cross_disconnect_during_emit() {
    let sig = make_signal::<usize>();
    let order = Arc::new(Mutex::new(Vec::new()));

    let conn2_cell: Arc<Mutex<Option<Box<dyn Connection>>>> = Arc::new(Mutex::new(None));
    let c2_cell = Arc::clone(&conn2_cell);
    let ord1 = Arc::clone(&order);

    // Slot 1 will disconnect Slot 2 before emit reaches Slot 2
    let _c1 = sig.connect(move |_| {
        ord1.lock().unwrap().push(1);
        if let Some(ref c2) = *c2_cell.lock().unwrap() {
            c2.disconnect();
        }
    });

    let ord2 = Arc::clone(&order);
    let c2 = sig.connect(move |_| {
        ord2.lock().unwrap().push(2);
    });

    *conn2_cell.lock().unwrap() = Some(Box::new(c2));

    sig.emit(100);

    // Slot 2 must have been cancelled before emit called it!
    let res = order.lock().unwrap().clone();
    assert_eq!(res, vec![1]);
}

#[test]
fn test_connect_during_emit_is_not_invoked_in_same_emit() {
    let sig = Arc::new(make_signal::<i32>());
    let second_slot_called = Arc::new(AtomicBool::new(false));

    let sig_clone = Arc::clone(&sig);
    let s2_called = Arc::clone(&second_slot_called);

    let _c1 = sig.connect(move |_| {
        let s2_flag = Arc::clone(&s2_called);
        // Connect a new slot while emit is running
        sig_clone.connect(move |_| {
            s2_flag.store(true, Ordering::SeqCst);
        });
    });

    sig.emit(42);
    // The newly connected slot must NOT be invoked in the currently executing emit()
    assert!(!second_slot_called.load(Ordering::SeqCst));

    // But it must be invoked in the subsequent emit()
    sig.emit(43);
    assert!(second_slot_called.load(Ordering::SeqCst));
}

#[test]
fn test_concurrent_emit_and_disconnect_stress() {
    let sig = Arc::new(make_signal::<usize>());
    let mut handles = Vec::new();

    for _ in 0..8 {
        let s = Arc::clone(&sig);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let conn = s.connect(|_| {});
                if i % 2 == 0 {
                    conn.disconnect();
                }
                s.emit(i);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}
