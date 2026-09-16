use simulation_027::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

struct MockStep {
    id: usize,
    delay: Duration,
    result: bool,
    log: Arc<Mutex<Vec<String>>>,
    executed: Arc<AtomicBool>,
    compensated: Arc<AtomicBool>,
}

impl MockStep {
    fn new(
        id: usize,
        delay_ms: u64,
        result: bool,
        log: Arc<Mutex<Vec<String>>>,
    ) -> (Self, Arc<AtomicBool>, Arc<AtomicBool>) {
        let executed = Arc::new(AtomicBool::new(false));
        let compensated = Arc::new(AtomicBool::new(false));
        (
            Self {
                id,
                delay: Duration::from_millis(delay_ms),
                result,
                log,
                executed: Arc::clone(&executed),
                compensated: Arc::clone(&compensated),
            },
            executed,
            compensated,
        )
    }
}

impl Step for MockStep {
    fn execute(&self) -> bool {
        if self.delay > Duration::ZERO {
            thread::sleep(self.delay);
        }
        self.executed.store(true, Ordering::SeqCst);
        self.log
            .lock()
            .unwrap()
            .push(format!("execute_{}", self.id));
        self.result
    }

    fn compensate(&self) {
        self.compensated.store(true, Ordering::SeqCst);
        self.log
            .lock()
            .unwrap()
            .push(format!("compensate_{}", self.id));
    }
}

#[test]
fn test_successful_saga_all_steps() {
    let orchestrator = make_saga_orchestrator();
    let log = Arc::new(Mutex::new(Vec::new()));

    let (s1, e1, c1) = MockStep::new(1, 10, true, Arc::clone(&log));
    let (s2, e2, c2) = MockStep::new(2, 15, true, Arc::clone(&log));
    let (s3, e3, c3) = MockStep::new(3, 10, true, Arc::clone(&log));

    let steps: Vec<Box<dyn Step>> = vec![Box::new(s1), Box::new(s2), Box::new(s3)];

    let result = orchestrator.run_saga(steps, Duration::from_millis(100));
    assert!(result);

    assert!(e1.load(Ordering::SeqCst));
    assert!(e2.load(Ordering::SeqCst));
    assert!(e3.load(Ordering::SeqCst));

    assert!(!c1.load(Ordering::SeqCst));
    assert!(!c2.load(Ordering::SeqCst));
    assert!(!c3.load(Ordering::SeqCst));

    assert_eq!(
        *log.lock().unwrap(),
        vec!["execute_1", "execute_2", "execute_3"]
    );
}

#[test]
fn test_step_failure_triggers_reverse_compensation() {
    let orchestrator = make_saga_orchestrator();
    let log = Arc::new(Mutex::new(Vec::new()));

    let (s1, e1, c1) = MockStep::new(1, 10, true, Arc::clone(&log));
    let (s2, e2, c2) = MockStep::new(2, 10, true, Arc::clone(&log));
    let (s3, e3, c3) = MockStep::new(3, 10, false, Arc::clone(&log)); // Step 3 fails!
    let (s4, e4, c4) = MockStep::new(4, 10, true, Arc::clone(&log));

    let steps: Vec<Box<dyn Step>> = vec![
        Box::new(s1),
        Box::new(s2),
        Box::new(s3),
        Box::new(s4),
    ];

    let result = orchestrator.run_saga(steps, Duration::from_millis(100));
    assert!(!result);

    assert!(e1.load(Ordering::SeqCst));
    assert!(e2.load(Ordering::SeqCst));
    assert!(e3.load(Ordering::SeqCst));
    assert!(!e4.load(Ordering::SeqCst), "Step 4 must never be executed!");

    // Compensation must run in strictly REVERSE order for successful steps (2 then 1)
    assert!(c1.load(Ordering::SeqCst));
    assert!(c2.load(Ordering::SeqCst));
    assert!(!c3.load(Ordering::SeqCst), "Failed step must not be compensated");
    assert!(!c4.load(Ordering::SeqCst));

    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "execute_1",
            "execute_2",
            "execute_3",
            "compensate_2",
            "compensate_1"
        ]
    );
}

#[test]
fn test_step_timeout_triggers_compensation() {
    let orchestrator = make_saga_orchestrator();
    let log = Arc::new(Mutex::new(Vec::new()));

    let (s1, e1, c1) = MockStep::new(1, 10, true, Arc::clone(&log));
    let (s2, _, c2) = MockStep::new(2, 120, true, Arc::clone(&log)); // Exceeds 50ms timeout!

    let steps: Vec<Box<dyn Step>> = vec![Box::new(s1), Box::new(s2)];

    let result = orchestrator.run_saga(steps, Duration::from_millis(50));
    assert!(!result);

    assert!(e1.load(Ordering::SeqCst));
    assert!(c1.load(Ordering::SeqCst));
    assert!(!c2.load(Ordering::SeqCst));
}

#[test]
fn test_first_step_failure_zero_compensations() {
    let orchestrator = make_saga_orchestrator();
    let log = Arc::new(Mutex::new(Vec::new()));

    let (s1, e1, c1) = MockStep::new(1, 10, false, Arc::clone(&log));
    let steps: Vec<Box<dyn Step>> = vec![Box::new(s1)];

    let result = orchestrator.run_saga(steps, Duration::from_millis(50));
    assert!(!result);

    assert!(e1.load(Ordering::SeqCst));
    assert!(!c1.load(Ordering::SeqCst));
}

#[test]
fn test_concurrent_independent_sagas() {
    let orchestrator = make_saga_orchestrator();
    let success_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..6 {
        let orch = orchestrator.clone();
        let sc = Arc::clone(&success_count);
        handles.push(thread::spawn(move || {
            let log = Arc::new(Mutex::new(Vec::new()));
            let should_succeed = i % 2 == 0;
            let (s1, _, _) = MockStep::new(1, 5, true, Arc::clone(&log));
            let (s2, _, _) = MockStep::new(2, 5, should_succeed, Arc::clone(&log));
            let steps: Vec<Box<dyn Step>> = vec![Box::new(s1), Box::new(s2)];

            if orch.run_saga(steps, Duration::from_millis(50)) {
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
    let orchestrator = make_saga_orchestrator();
    assert_send(&orchestrator);
    assert_sync(&orchestrator);
}
