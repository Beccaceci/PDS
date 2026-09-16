use simulation_039::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let barrier = make_poisonable_barrier::<i32>(2);
    assert_send_sync(&barrier);
}

#[test]
fn test_single_participant_normal_completion() {
    let barrier = make_poisonable_barrier::<i32>(1);
    let res = barrier.join_with(42);
    assert_eq!(res, ArriveResult::Complete(vec![42]));
}

#[test]
fn test_single_participant_poison() {
    let barrier = make_poisonable_barrier::<i32>(1);
    barrier.poison();
    // After poison, round resets clean:
    let res = barrier.join_with(100);
    assert_eq!(res, ArriveResult::Complete(vec![100]));
}

#[test]
fn test_multi_participant_normal_barrier() {
    let participants = 4;
    let barrier = Arc::new(make_poisonable_barrier::<usize>(participants));
    let mut handles = Vec::new();

    for id in 0..participants {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || b.join_with(id)));
    }

    for h in handles {
        let res = h.join().unwrap();
        match res {
            ArriveResult::Complete(vals) => {
                assert_eq!(vals.len(), participants);
                for i in 0..participants {
                    assert!(vals.contains(&i));
                }
            }
            ArriveResult::Poisoned => panic!("Expected Complete, got Poisoned"),
        }
    }
}

#[test]
fn test_multi_round_cyclic_reuse() {
    let participants = 3;
    let rounds = 5;
    let barrier = Arc::new(make_poisonable_barrier::<usize>(participants));
    let mut handles = Vec::new();

    for id in 0..participants {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            for round in 0..rounds {
                let res = b.join_with(id * 100 + round);
                match res {
                    ArriveResult::Complete(vals) => assert_eq!(vals.len(), participants),
                    ArriveResult::Poisoned => panic!("Unexpected poison in round {}", round),
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_poison_unblocks_all_waiting_threads() {
    let participants = 3;
    let barrier = Arc::new(make_poisonable_barrier::<String>(participants));
    let mut handles = Vec::new();

    // Spawn 2 out of 3 participants, leaving them blocked waiting for the 3rd
    for id in 0..2 {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || b.join_with(format!("val_{}", id))));
    }

    thread::sleep(Duration::from_millis(40));

    // Poison the current round
    barrier.poison();

    // All blocked threads should immediately wake up with Poisoned
    for h in handles {
        let res = h.join().unwrap();
        assert_eq!(res, ArriveResult::Poisoned);
    }
}

#[test]
fn test_poison_followed_by_clean_round() {
    let participants = 2;
    let barrier = Arc::new(make_poisonable_barrier::<i32>(participants));

    // Thread 0 arrives, gets blocked
    let b_clone = Arc::clone(&barrier);
    let h0 = thread::spawn(move || b_clone.join_with(1));

    thread::sleep(Duration::from_millis(30));
    barrier.poison();

    assert_eq!(h0.join().unwrap(), ArriveResult::Poisoned);

    // Round 2 must start fresh and succeed normally
    let mut handles = Vec::new();
    for id in 0..participants {
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || b.join_with((id + 1) as i32 * 10)));
    }

    for h in handles {
        let res = h.join().unwrap();
        assert_eq!(res, ArriveResult::Complete(vec![10, 20]));
    }
}

#[test]
fn test_poison_on_empty_barrier_advances_phase() {
    let barrier = Arc::new(make_poisonable_barrier::<i32>(2));

    // Poison before any join
    barrier.poison();

    // Next round should work normally
    let b1 = Arc::clone(&barrier);
    let h1 = thread::spawn(move || b1.join_with(10));
    let b2 = Arc::clone(&barrier);
    let h2 = thread::spawn(move || b2.join_with(20));

    assert_eq!(h1.join().unwrap(), ArriveResult::Complete(vec![10, 20]));
    assert_eq!(h2.join().unwrap(), ArriveResult::Complete(vec![10, 20]));
}

#[test]
fn test_high_concurrency_stress_interleaved_poison() {
    let participants = 4;
    let iterations = 100;
    let barrier = Arc::new(make_poisonable_barrier::<usize>(participants));

    for round in 0..iterations {
        let mut handles = Vec::new();
        let should_poison = round % 3 == 0;

        let num_arrivals = if should_poison { participants - 1 } else { participants };

        for id in 0..num_arrivals {
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || b.join_with(id)));
        }

        if should_poison {
            thread::sleep(Duration::from_micros(300));
            barrier.poison();
            for h in handles {
                assert_eq!(h.join().unwrap(), ArriveResult::Poisoned);
            }
        } else {
            for h in handles {
                match h.join().unwrap() {
                    ArriveResult::Complete(vals) => assert_eq!(vals.len(), participants),
                    ArriveResult::Poisoned => panic!("Unexpected poison in iteration {}", round),
                }
            }
        }
    }
}