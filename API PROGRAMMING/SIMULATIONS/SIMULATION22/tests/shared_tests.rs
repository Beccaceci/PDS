use simulation_022::single_cvar_scheduler::SingleCvarScheduler;
use simulation_022::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_single_job_execution_and_join() {
    let scheduler = make_job_scheduler(2);
    let executed = Arc::new(AtomicBool::new(false));

    let ex = Arc::clone(&executed);
    let handle = scheduler.submit(10, &[], move || {
        thread::sleep(Duration::from_millis(30));
        ex.store(true, Ordering::SeqCst);
    });

    handle.join();
    assert!(executed.load(Ordering::SeqCst));

    scheduler.close();
    scheduler.join_all();
}

#[test]
fn test_conflicting_resources_mutual_exclusion() {
    let scheduler = make_job_scheduler(4);
    let in_critical_section = Arc::new(AtomicBool::new(false));
    let conflict_detected = Arc::new(AtomicBool::new(false));

    let res = vec!["gpu_0".to_string(), "shared_db".to_string()];

    let mut handles = Vec::new();
    for _ in 0..5 {
        let in_cs = Arc::clone(&in_critical_section);
        let conf = Arc::clone(&conflict_detected);
        handles.push(scheduler.submit(10, &res, move || {
            if in_cs.swap(true, Ordering::SeqCst) {
                conf.store(true, Ordering::SeqCst);
            }
            thread::sleep(Duration::from_millis(25));
            in_cs.store(false, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.join();
    }

    assert!(
        !conflict_detected.load(Ordering::SeqCst),
        "Jobs with overlapping resources must NEVER run in parallel!"
    );

    scheduler.close();
    scheduler.join_all();
}

#[test]
fn test_disjoint_resources_run_in_parallel() {
    let scheduler = make_job_scheduler(3);
    let concurrent_running = Arc::new(AtomicUsize::new(0));
    let max_concurrency = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..3 {
        let res = vec![format!("independent_res_{}", i)];
        let cr = Arc::clone(&concurrent_running);
        let mc = Arc::clone(&max_concurrency);

        handles.push(scheduler.submit(10, &res, move || {
            let active = cr.fetch_add(1, Ordering::SeqCst) + 1;
            mc.fetch_max(active, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(60));
            cr.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.join();
    }

    assert!(
        max_concurrency.load(Ordering::SeqCst) >= 2,
        "Jobs with disjoint resources must execute in parallel across multiple worker threads"
    );

    scheduler.close();
    scheduler.join_all();
}

#[test]
fn test_cancel_pending_job() {
    let scheduler = make_job_scheduler(1);
    let res = vec!["exclusive".to_string()];

    // 1. Submit long blocking job holding resource
    let _h1 = scheduler.submit(100, &res, || {
        thread::sleep(Duration::from_millis(100));
    });

    // 2. Submit second job that waits for same resource
    let job2_executed = Arc::new(AtomicBool::new(false));
    let j2 = Arc::clone(&job2_executed);
    let h2 = scheduler.submit(50, &res, move || {
        j2.store(true, Ordering::SeqCst);
    });

    // Cancel while pending
    assert!(h2.cancel(), "Cancelling pending job must return true");

    // Once cancelled, second cancel returns false
    assert!(!h2.cancel());

    // Join h2 -> returns immediately without error
    h2.join();
    assert!(!job2_executed.load(Ordering::SeqCst), "Cancelled job must never be executed");

    scheduler.close();
    scheduler.join_all();
}

#[test]
fn test_aging_promotes_older_job() {
    let scheduler = make_job_scheduler(1);
    let execution_order = Arc::new(Mutex::new(Vec::new()));

    let res = vec!["single_worker_resource".to_string()];

    // 1. Bloccante
    let _h_block = scheduler.submit(100, &res, || {
        thread::sleep(Duration::from_millis(80));
    });

    // 2. Job a bassa priorità (10) inviato subito
    let order1 = Arc::clone(&execution_order);
    let h_low = scheduler.submit(10, &res, move || {
        order1.lock().unwrap().push("low_priority_aged");
    });

    // Aspetta 60ms: il job a bassa priorità guadagna ~60 punti di aging -> priorità effettiva ~70
    thread::sleep(Duration::from_millis(60));

    // 3. Job ad alta priorità base (30) appena arrivato -> priorità effettiva ~30
    let order2 = Arc::clone(&execution_order);
    let h_high = scheduler.submit(30, &res, move || {
        order2.lock().unwrap().push("high_priority_fresh");
    });

    h_low.join();
    h_high.join();

    scheduler.close();
    scheduler.join_all();

    let logs = execution_order.lock().unwrap().clone();
    assert_eq!(
        logs,
        vec!["low_priority_aged", "high_priority_fresh"],
        "Older job with lower base priority should win due to aging"
    );
}

#[test]
fn test_join_all_waits_for_all_jobs_to_complete() {
    let scheduler = make_job_scheduler(3);
    let counter = Arc::new(AtomicUsize::new(0));

    for i in 0..6 {
        let c = Arc::clone(&counter);
        scheduler.submit(10, &[], move || {
            thread::sleep(Duration::from_millis(20 * (i % 2 + 1)));
            c.fetch_add(1, Ordering::SeqCst);
        });
    }

    scheduler.close();
    scheduler.join_all();

    assert_eq!(
        counter.load(Ordering::SeqCst),
        6,
        "join_all() must block until all jobs have fully finished"
    );
}

#[test]
fn test_single_cvar_scheduler_variant() {
    let scheduler = SingleCvarScheduler::new(3);
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..6 {
        let c = Arc::clone(&counter);
        handles.push(scheduler.submit(10, &["shared".to_string()], move || {
            thread::sleep(Duration::from_millis(15));
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.join();
    }

    scheduler.close();
    scheduler.join_all();

    assert_eq!(counter.load(Ordering::SeqCst), 6);
}

#[test]
fn test_send_sync_bounds() {
    let scheduler = make_job_scheduler(2);
    assert_send(&scheduler);
    assert_sync(&scheduler);
    scheduler.close();
    scheduler.join_all();
}
