use simulation_049::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        5,
        Duration::from_secs(1),
    );
    assert_send(&supervisor);
    assert_sync(&supervisor);

    let (_id, actor) = supervisor.spawn_actor(10);
    assert_send(&actor);
    assert_sync(&actor);
}

#[test]
fn test_single_actor_tell_and_ask_success() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        5,
        Duration::from_secs(1),
    );
    let (_id, actor) = supervisor.spawn_actor(10);
    assert_eq!(supervisor.active_actor_count(), 1);
    assert_eq!(actor.status(), ActorStatus::Running);

    // Test fire-and-forget tell
    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = Arc::clone(&flag);
    assert!(actor.tell(move || {
        flag_clone.store(true, Ordering::SeqCst);
        true
    }));

    // Test synchronous ask
    let res = actor.ask(|| true);
    assert_eq!(res, Ok(true));
    assert!(flag.load(Ordering::SeqCst));
    assert_eq!(supervisor.total_restarts(), 0);
}

#[test]
fn test_one_for_one_actor_crash_and_restart_isolated() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        5,
        Duration::from_secs(2),
    );
    let (_id1, actor1) = supervisor.spawn_actor(10);
    let (_id2, actor2) = supervisor.spawn_actor(10);

    // Actor 1 executes a failing job -> crashes
    let res1 = actor1.ask(|| false);
    assert_eq!(res1, Err(ActorError::ActorCrashed));

    // Actor 2 must remain unaffected and healthy
    assert_eq!(actor2.status(), ActorStatus::Running);
    assert_eq!(actor2.ask(|| true), Ok(true));

    // Wait a brief moment for supervisor to complete restart of Actor 1
    thread::sleep(Duration::from_millis(50));
    assert_eq!(supervisor.total_restarts(), 1);
    assert_eq!(actor1.status(), ActorStatus::Running);

    // Actor 1 can now process new messages normally
    assert_eq!(actor1.ask(|| true), Ok(true));
}

#[test]
fn test_all_for_one_sibling_crash_terminates_and_restarts_group() {
    let supervisor = make_supervisor(
        RestartStrategy::AllForOne,
        5,
        Duration::from_secs(2),
    );
    let (_id1, actor1) = supervisor.spawn_actor(10);
    let (_id2, actor2) = supervisor.spawn_actor(10);

    let barrier = Arc::new(Barrier::new(2));
    let b_clone = Arc::clone(&barrier);

    // Actor 2 is executing a long/pending job
    let a2_clone = Arc::new(actor2);
    let a2_thread = Arc::clone(&a2_clone);
    let h2 = thread::spawn(move || {
        a2_thread.ask(move || {
            b_clone.wait();
            thread::sleep(Duration::from_millis(100));
            true
        })
    });

    barrier.wait(); // Both threads are synchronized

    // Actor 1 fails and crashes
    let res1 = actor1.ask(|| false);
    assert_eq!(res1, Err(ActorError::ActorCrashed));

    // Under AllForOne, Actor 2 must be aborted by supervisor!
    let res2 = h2.join().unwrap();
    assert!(
        res2 == Err(ActorError::SupervisorTerminated) || res2 == Err(ActorError::ActorCrashed),
        "Sibling actor should be aborted due to AllForOne cascade"
    );

    // Wait for group restart to finish
    thread::sleep(Duration::from_millis(60));
    assert!(supervisor.total_restarts() >= 1);

    // Both actors should be revived and functional
    assert_eq!(actor1.ask(|| true), Ok(true));
    assert_eq!(a2_clone.ask(|| true), Ok(true));
}

#[test]
fn test_flapping_rate_limit_exceeded_triggers_permanent_termination() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        2, // At most 2 restarts allowed
        Duration::from_millis(300),
    );
    let (_id, actor) = supervisor.spawn_actor(10);

    // Crash 1: restarts ok
    let _ = actor.ask(|| false);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(actor.status(), ActorStatus::Running);

    // Crash 2: restarts ok
    let _ = actor.ask(|| false);
    thread::sleep(Duration::from_millis(20));
    assert_eq!(actor.status(), ActorStatus::Running);

    // Crash 3: exceeds max_restarts (3 > 2 in 300ms window) -> permanent termination!
    let _ = actor.ask(|| false);
    thread::sleep(Duration::from_millis(40));

    assert_eq!(actor.status(), ActorStatus::Terminated);
    assert_eq!(supervisor.active_actor_count(), 0);

    // Future requests should be rejected
    assert_eq!(actor.ask(|| true), Err(ActorError::MailboxClosed));
    assert!(!actor.tell(|| true));
}

#[test]
fn test_dead_letter_queue_accounting() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        1,
        Duration::from_secs(1),
    );
    let (_id, actor) = supervisor.spawn_actor(20);
    let initial_dead_letters = supervisor.dead_letter_count();

    // Fill mailbox with multiple asynchronous messages
    let barrier = Arc::new(Barrier::new(2));
    let b_clone = Arc::clone(&barrier);

    // First message blocks the worker thread
    actor.tell(move || {
        b_clone.wait();
        false // First message fails and crashes the actor!
    });

    // Queue subsequent messages that will be discarded upon crash
    for _ in 0..4 {
        actor.tell(|| true);
    }

    barrier.wait(); // Unblock first message to trigger the crash
    thread::sleep(Duration::from_millis(60));

    // The discarded messages from the crashed mailbox must be counted as dead letters
    assert!(
        supervisor.dead_letter_count() > initial_dead_letters,
        "Discarded messages must increment dead_letter_count"
    );
}

#[test]
fn test_actor_graceful_stop() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        5,
        Duration::from_secs(1),
    );
    let (_id, actor) = supervisor.spawn_actor(10);
    assert_eq!(actor.status(), ActorStatus::Running);

    assert!(actor.stop());
    // Second stop should return false
    assert!(!actor.stop());
    assert_eq!(actor.status(), ActorStatus::Terminated);

    // Further tells and asks must fail
    assert!(!actor.tell(|| true));
    assert_eq!(actor.ask(|| true), Err(ActorError::MailboxClosed));
}

#[test]
fn test_stress_concurrent_producers_and_supervision() {
    let supervisor = make_supervisor(
        RestartStrategy::OneForOne,
        50, // generous restart budget for stress testing
        Duration::from_secs(5),
    );

    let actor_count = 3;
    let mut actors = Vec::new();
    for _ in 0..actor_count {
        let (_id, a) = supervisor.spawn_actor(20);
        actors.push(Arc::new(a));
    }

    let thread_count = 8;
    let iterations = 20;
    let success_counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for t_idx in 0..thread_count {
        let actors_clone = actors.clone();
        let sc = Arc::clone(&success_counter);
        handles.push(thread::spawn(move || {
            for i in 0..iterations {
                let target_actor = &actors_clone[(t_idx + i) % actors_clone.len()];
                // Mostly successes, occasional deliberate crashes
                let should_fail = (t_idx + i) % 7 == 0;
                let outcome = target_actor.ask(move || !should_fail);
                if outcome == Ok(true) {
                    sc.fetch_add(1, Ordering::SeqCst);
                }
                thread::sleep(Duration::from_millis(2));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(success_counter.load(Ordering::SeqCst) > 0);
}
