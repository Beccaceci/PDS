use simulation_009::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_wait_on_empty_wait_group_returns_immediately() {
    let wg = make_wait_group();
    let start = Instant::now();
    wg.wait();
    assert!(start.elapsed() < Duration::from_millis(50));
}

#[test]
fn test_basic_add_done_wait() {
    let wg = make_wait_group();
    let token = wg.add();

    let finished = Arc::new(AtomicBool::new(false));
    let f_clone = Arc::clone(&finished);

    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(60));
        f_clone.store(true, Ordering::SeqCst);
        token.done();
    });

    wg.wait();
    assert!(finished.load(Ordering::SeqCst));
    handle.join().unwrap();
}

#[test]
fn test_token_auto_drop_decrements_counter() {
    let wg = make_wait_group();

    let token = wg.add();
    let dropped_flag = Arc::new(AtomicBool::new(false));
    let df_clone = Arc::clone(&dropped_flag);

    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(60));
        df_clone.store(true, Ordering::SeqCst);
        // Explicitly drop token without calling done()
        drop(token);
    });

    wg.wait();
    assert!(dropped_flag.load(Ordering::SeqCst));
    handle.join().unwrap();
}

#[test]
fn test_done_does_not_double_decrement_on_drop() {
    let wg = make_wait_group();

    let token1 = wg.add();
    let token2 = wg.add();

    // Calling done(token1) consumes token1 and triggers its Drop.
    // It must NOT decrement the internal counter twice!
    token1.done();

    let start = Instant::now();
    // Since token2 is still alive, wait_timeout should fail
    let ok = wg.wait_timeout(Duration::from_millis(60));
    assert!(!ok, "WaitGroup should still be waiting for token2");
    assert!(start.elapsed() >= Duration::from_millis(50));

    token2.done();
    wg.wait();
}

#[test]
fn test_multiple_worker_threads() {
    let wg = make_wait_group();
    const NUM_WORKERS: usize = 12;

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..NUM_WORKERS {
        let token = wg.add();
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            c.fetch_add(1, Ordering::SeqCst);
            token.done();
        }));
    }

    wg.wait();
    assert_eq!(
        counter.load(Ordering::SeqCst),
        NUM_WORKERS,
        "All workers must have completed when wait() returns"
    );

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_wait_timeout_success_and_failure() {
    let wg = make_wait_group();

    let token1 = wg.add();
    let start = Instant::now();
    let res_fail = wg.wait_timeout(Duration::from_millis(80));
    let elapsed = start.elapsed();

    assert!(!res_fail, "wait_timeout should return false when task is not finished");
    assert!(elapsed >= Duration::from_millis(60));

    // Finish token1
    token1.done();

    // Now wait_timeout should immediately succeed
    let res_success = wg.wait_timeout(Duration::from_millis(80));
    assert!(res_success, "wait_timeout should return true when counter is 0");
}

#[test]
fn test_reusability_across_sequential_rounds() {
    let wg = make_wait_group();

    for round in 1..=4 {
        let mut tokens = vec![];
        for _ in 0..round {
            tokens.push(wg.add());
        }

        let wg_clone = wg.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(40));
            for t in tokens {
                t.done();
            }
        });

        wg_clone.wait();
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_add_and_wait_stress() {
    let wg = Arc::new(make_wait_group());
    const THREADS: usize = 16;
    const TASKS_PER_THREAD: usize = 20;

    let completed_tasks = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..THREADS {
        let wg_c = Arc::clone(&wg);
        let count_c = Arc::clone(&completed_tasks);
        handles.push(thread::spawn(move || {
            for i in 0..TASKS_PER_THREAD {
                let token = wg_c.add();
                if i % 3 == 0 {
                    // Test auto-drop
                    count_c.fetch_add(1, Ordering::SeqCst);
                    drop(token);
                } else {
                    // Test done()
                    count_c.fetch_add(1, Ordering::SeqCst);
                    token.done();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    wg.wait();
    assert_eq!(
        completed_tasks.load(Ordering::SeqCst),
        THREADS * TASKS_PER_THREAD
    );
}
