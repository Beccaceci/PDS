use simulation_032::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_initial_phase_is_zero() {
    let phaser = make_phaser();
    assert_eq!(phaser.phase(), 0);
}

#[test]
fn test_basic_phaser_single_phase() {
    let phaser = make_phaser();

    let p1 = phaser.register();
    let p2 = phaser.register();
    let p3 = phaser.register();

    let unblocked = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for p in [p1, p2, p3] {
        let ub = Arc::clone(&unblocked);
        handles.push(thread::spawn(move || {
            p.arrive_and_wait();
            ub.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(unblocked.load(Ordering::SeqCst), 3);
    assert_eq!(phaser.phase(), 1);
}

#[test]
fn test_multi_phase_cyclic_reuse() {
    let phaser = make_phaser();

    let p1 = phaser.register();
    let p2 = phaser.register();

    let h1 = thread::spawn(move || {
        for _ in 0..3 {
            p1.arrive_and_wait();
        }
    });

    let h2 = thread::spawn(move || {
        for _ in 0..3 {
            p2.arrive_and_wait();
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    assert_eq!(phaser.phase(), 3);
}

#[test]
fn test_deregister_advances_phase_if_last() {
    let phaser = make_phaser();

    let p1 = phaser.register();
    let p2 = phaser.register();
    let p3 = phaser.register();

    let p1_unblocked = Arc::new(AtomicBool::new(false));
    let p2_unblocked = Arc::new(AtomicBool::new(false));

    let u1 = Arc::clone(&p1_unblocked);
    let h1 = thread::spawn(move || {
        p1.arrive_and_wait();
        u1.store(true, Ordering::SeqCst);
    });

    let u2 = Arc::clone(&p2_unblocked);
    let h2 = thread::spawn(move || {
        p2.arrive_and_wait();
        u2.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!p1_unblocked.load(Ordering::SeqCst));
    assert!(!p2_unblocked.load(Ordering::SeqCst));
    assert_eq!(phaser.phase(), 0);

    // p3 deregisters instead of arriving -> this satisfies the phase!
    p3.deregister();

    h1.join().unwrap();
    h2.join().unwrap();

    assert!(p1_unblocked.load(Ordering::SeqCst));
    assert!(p2_unblocked.load(Ordering::SeqCst));
    assert_eq!(phaser.phase(), 1);
}

#[test]
fn test_register_during_wait_requires_new_arrival() {
    let phaser = make_phaser();

    let p1 = phaser.register();
    let p2 = phaser.register();

    let unblocked = Arc::new(AtomicBool::new(false));
    let ub = Arc::clone(&unblocked);

    let h1 = thread::spawn(move || {
        p1.arrive_and_wait();
        ub.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(20));

    // Register a 3rd party while p1 is waiting and p2 hasn't arrived
    let p3 = phaser.register();

    // Now p2 arrives
    let h2 = thread::spawn(move || {
        p2.arrive_and_wait();
    });

    thread::sleep(Duration::from_millis(40));
    // p1 and p2 must STILL be blocked because p3 was registered and hasn't arrived!
    assert!(!unblocked.load(Ordering::SeqCst));
    assert_eq!(phaser.phase(), 0);

    // Now p3 arrives -> phase completes!
    p3.arrive_and_wait();

    h1.join().unwrap();
    h2.join().unwrap();

    assert!(unblocked.load(Ordering::SeqCst));
    assert_eq!(phaser.phase(), 1);
}

#[test]
fn test_high_concurrency_stress() {
    let phaser = make_phaser();
    let mut handles = Vec::new();

    for _ in 0..8 {
        let p = phaser.register();
        handles.push(thread::spawn(move || {
            for i in 0..5 {
                if i == 4 {
                    p.deregister();
                    break;
                } else {
                    p.arrive_and_wait();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(phaser.phase(), 4);
}

#[test]
fn test_send_sync_bounds() {
    let phaser = make_phaser();
    assert_send(&phaser);
    assert_sync(&phaser);
}
