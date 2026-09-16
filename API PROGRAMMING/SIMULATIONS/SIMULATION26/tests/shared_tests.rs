use simulation_026::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

struct MockParticipant {
    prepare_delay: Duration,
    prepare_result: bool,
    committed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
}

impl MockParticipant {
    fn new(delay_ms: u64, result: bool) -> (Self, Arc<AtomicBool>, Arc<AtomicBool>) {
        let committed = Arc::new(AtomicBool::new(false));
        let aborted = Arc::new(AtomicBool::new(false));
        (
            Self {
                prepare_delay: Duration::from_millis(delay_ms),
                prepare_result: result,
                committed: Arc::clone(&committed),
                aborted: Arc::clone(&aborted),
            },
            committed,
            aborted,
        )
    }
}

impl Participant for MockParticipant {
    fn prepare(&self) -> bool {
        if self.prepare_delay > Duration::ZERO {
            thread::sleep(self.prepare_delay);
        }
        self.prepare_result
    }

    fn commit(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    fn abort(&self) {
        self.aborted.store(true, Ordering::SeqCst);
    }
}

#[test]
fn test_successful_all_commit() {
    let coordinator = make_coordinator();

    let (p1, c1, a1) = MockParticipant::new(10, true);
    let (p2, c2, a2) = MockParticipant::new(20, true);
    let (p3, c3, a3) = MockParticipant::new(15, true);

    let participants: Vec<Box<dyn Participant>> = vec![Box::new(p1), Box::new(p2), Box::new(p3)];

    let result = coordinator.run_transaction(participants, Duration::from_millis(100));
    assert!(result, "All participants prepared successfully -> must commit");

    assert!(c1.load(Ordering::SeqCst));
    assert!(c2.load(Ordering::SeqCst));
    assert!(c3.load(Ordering::SeqCst));

    assert!(!a1.load(Ordering::SeqCst));
    assert!(!a2.load(Ordering::SeqCst));
    assert!(!a3.load(Ordering::SeqCst));
}

#[test]
fn test_single_participant_fails_prepare_triggers_aborts() {
    let coordinator = make_coordinator();

    let (p1, c1, a1) = MockParticipant::new(10, true);
    let (p2, c2, a2) = MockParticipant::new(15, false); // Fails prepare!
    let (p3, c3, a3) = MockParticipant::new(20, true);

    let participants: Vec<Box<dyn Participant>> = vec![Box::new(p1), Box::new(p2), Box::new(p3)];

    let result = coordinator.run_transaction(participants, Duration::from_millis(100));
    assert!(!result, "One participant failed prepare -> transaction must abort");

    // p1 and p3 were ready -> must be aborted
    assert!(a1.load(Ordering::SeqCst));
    assert!(a3.load(Ordering::SeqCst));

    // p2 failed prepare -> must NOT be aborted
    assert!(!a2.load(Ordering::SeqCst));

    // None should be committed
    assert!(!c1.load(Ordering::SeqCst));
    assert!(!c2.load(Ordering::SeqCst));
    assert!(!c3.load(Ordering::SeqCst));
}

#[test]
fn test_prepare_timeout_triggers_abort_on_ready_participants() {
    let coordinator = make_coordinator();

    let (p1, c1, a1) = MockParticipant::new(10, true);
    let (p2, c2, a2) = MockParticipant::new(120, true); // Exceeds timeout of 50ms!

    let participants: Vec<Box<dyn Participant>> = vec![Box::new(p1), Box::new(p2)];

    let result = coordinator.run_transaction(participants, Duration::from_millis(50));
    assert!(!result, "Timeout exceeded -> transaction must abort");

    // p1 prepared in time -> must be aborted
    assert!(a1.load(Ordering::SeqCst));
    assert!(!c1.load(Ordering::SeqCst));

    // p2 was not ready at decision time -> must NOT be committed or aborted
    assert!(!c2.load(Ordering::SeqCst));
    assert!(!a2.load(Ordering::SeqCst));
}

#[test]
fn test_concurrent_independent_transactions() {
    let coordinator = make_coordinator();
    let success_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..6 {
        let coord = coordinator.clone();
        let sc = Arc::clone(&success_count);
        handles.push(thread::spawn(move || {
            let should_succeed = i % 2 == 0;
            let (p1, _, _) = MockParticipant::new(10, true);
            let (p2, _, _) = MockParticipant::new(15, should_succeed);
            let participants: Vec<Box<dyn Participant>> = vec![Box::new(p1), Box::new(p2)];

            if coord.run_transaction(participants, Duration::from_millis(80)) {
                sc.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(success_count.load(Ordering::SeqCst), 3);
}

#[test]
fn test_send_sync_bounds() {
    let coordinator = make_coordinator();
    assert_send(&coordinator);
    assert_sync(&coordinator);
}
