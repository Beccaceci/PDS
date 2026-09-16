use simulation_018::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_lock_and_mutate() {
    let lock = make_ticket_lock(10);

    {
        let mut guard = lock.lock();
        assert_eq!(*guard.get(), 10);
        *guard.get_mut() = 20;
    }

    {
        let guard = lock.lock();
        assert_eq!(*guard.get(), 20);
    }
}

#[test]
fn test_exclusive_mutual_exclusion() {
    let lock = Arc::new(make_ticket_lock(0));
    let in_critical_section = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for _ in 0..5 {
        let l = Arc::clone(&lock);
        let in_cs = Arc::clone(&in_critical_section);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                let mut guard = l.lock();
                assert!(
                    !in_cs.swap(true, Ordering::SeqCst),
                    "Violated mutual exclusion: multiple threads inside critical section!"
                );

                *guard.get_mut() += 1;
                thread::sleep(Duration::from_millis(1));

                assert!(
                    in_cs.swap(false, Ordering::SeqCst),
                    "Inconsistent critical section state"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let guard = lock.lock();
    assert_eq!(*guard.get(), 250);
}

#[test]
fn test_strict_fifo_ticket_ordering() {
    let lock = Arc::new(make_ticket_lock(Vec::<usize>::new()));

    // Hold the lock in main thread first
    let main_guard = lock.lock();

    let ready_flags = Arc::new((0..5).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
    let mut handles = Vec::new();

    // Spawn 5 threads sequentially, each calling lock() in order
    for id in 0..5 {
        let l = Arc::clone(&lock);
        let rf = Arc::clone(&ready_flags);
        handles.push(thread::spawn(move || {
            rf[id].store(true, Ordering::SeqCst);
            let mut guard = l.lock();
            guard.get_mut().push(id);
        }));

        // Wait until this specific thread has started before spawning next to ensure ticket acquisition order
        while !ready_flags[id].load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(20));
    }

    // Now release the main lock -> threads must execute strictly in ticket order: 0, 1, 2, 3, 4
    drop(main_guard);

    for h in handles {
        h.join().unwrap();
    }

    let final_guard = lock.lock();
    assert_eq!(
        *final_guard.get(),
        vec![0, 1, 2, 3, 4],
        "Tickets must be served in exact FIFO sequential order!"
    );
}

#[test]
fn test_send_sync_bounds() {
    let lock = make_ticket_lock(42);
    assert_send(&lock);
    assert_sync(&lock);
}
