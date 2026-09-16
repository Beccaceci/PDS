use simulation_017::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_participants_count() {
    let tm = make_turn_manager(5);
    assert_eq!(tm.participants(), 5);
}

#[test]
fn test_sequential_turns_in_order() {
    let tm = make_turn_manager(3);

    // Turn 0
    {
        let t0 = tm.wait_for_turn(0);
        assert_eq!(t0.participant(), 0);
    }

    // Turn 1
    {
        let t1 = tm.wait_for_turn(1);
        assert_eq!(t1.participant(), 1);
    }

    // Turn 2
    {
        let t2 = tm.wait_for_turn(2);
        assert_eq!(t2.participant(), 2);
    }

    // Wraps back to Turn 0
    {
        let t0_again = tm.wait_for_turn(0);
        assert_eq!(t0_again.participant(), 0);
    }
}

#[test]
fn test_out_of_order_callers_strictly_follow_turn_rotation() {
    let tm = Arc::new(make_turn_manager(3));
    let order_log = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    // Spawn participants in reverse order: 2, then 1, then 0
    for id in (0..3).rev() {
        let manager = Arc::clone(&tm);
        let log = Arc::clone(&order_log);
        handles.push(thread::spawn(move || {
            let turn = manager.wait_for_turn(id);
            log.lock().unwrap().push(turn.participant());
            // drop(turn) implicitly here
        }));
        thread::sleep(Duration::from_millis(15));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        *order_log.lock().unwrap(),
        vec![0, 1, 2],
        "Participants must obtain their turn strictly in rotation order, NOT arrival order!"
    );
}

#[test]
fn test_turn_held_until_raii_drop() {
    let tm = Arc::new(make_turn_manager(2));

    // Thread 0 acquires turn and holds it
    let turn_0 = tm.wait_for_turn(0);

    let tm_clone = Arc::clone(&tm);
    let thread_1_started = Arc::new(AtomicBool::new(false));
    let t1_s = Arc::clone(&thread_1_started);

    let start = Instant::now();
    let worker_1 = thread::spawn(move || {
        let t1 = tm_clone.wait_for_turn(1);
        t1_s.store(true, Ordering::SeqCst);
        t1.participant()
    });

    thread::sleep(Duration::from_millis(50));
    assert!(
        !thread_1_started.load(Ordering::SeqCst),
        "Participant 1 must remain blocked while participant 0 holds its Turn guard"
    );

    // Release turn 0
    drop(turn_0);

    let p = worker_1.join().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(p, 1);
    assert!(thread_1_started.load(Ordering::SeqCst));
    assert!(elapsed >= Duration::from_millis(45));
}

#[test]
fn test_cyclic_multi_round_rotation_stress() {
    const NUM_PARTICIPANTS: usize = 5;
    const ROUNDS: usize = 20;
    let tm = Arc::new(make_turn_manager(NUM_PARTICIPANTS));
    let execution_log = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for id in 0..NUM_PARTICIPANTS {
        let manager = Arc::clone(&tm);
        let log = Arc::clone(&execution_log);
        handles.push(thread::spawn(move || {
            for _ in 0..ROUNDS {
                let turn = manager.wait_for_turn(id);
                log.lock().unwrap().push(turn.participant());
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let log = execution_log.lock().unwrap();
    assert_eq!(log.len(), NUM_PARTICIPANTS * ROUNDS);

    // Verify exact round-robin sequence: 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, ...
    for (idx, &val) in log.iter().enumerate() {
        let expected = idx % NUM_PARTICIPANTS;
        assert_eq!(
            val, expected,
            "At step {}, expected turn {} but got {}",
            idx, expected, val
        );
    }
}

#[test]
fn test_send_sync_bounds() {
    let tm = make_turn_manager(4);
    assert_send(&tm);
    assert_sync(&tm);
}
