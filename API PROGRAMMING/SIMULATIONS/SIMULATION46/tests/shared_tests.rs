use simulation_046::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let graph = make_task_graph(2);
    assert_send(&graph);
    assert_sync(&graph);
}

#[test]
fn test_single_successful_task() {
    let graph = make_task_graph(2);
    let (_id, handle) = graph.submit(&[], || true);
    assert!(handle.join());
}

#[test]
fn test_single_failed_task() {
    let graph = make_task_graph(2);
    let (_id, handle) = graph.submit(&[], || false);
    assert!(!handle.join());
}

#[test]
fn test_linear_dependency_chain_success() {
    let graph = make_task_graph(2);
    let counter = Arc::new(AtomicUsize::new(0));

    let c1 = Arc::clone(&counter);
    let (id1, h1) = graph.submit(&[], move || {
        thread::sleep(Duration::from_millis(20));
        c1.fetch_add(1, Ordering::SeqCst);
        true
    });

    let c2 = Arc::clone(&counter);
    let (id2, h2) = graph.submit(&[id1], move || {
        assert_eq!(c2.load(Ordering::SeqCst), 1);
        c2.fetch_add(1, Ordering::SeqCst);
        true
    });

    let c3 = Arc::clone(&counter);
    let (_id3, h3) = graph.submit(&[id2], move || {
        assert_eq!(c3.load(Ordering::SeqCst), 2);
        c3.fetch_add(1, Ordering::SeqCst);
        true
    });

    assert!(h1.join());
    assert!(h2.join());
    assert!(h3.join());
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

#[test]
fn test_failure_cascades_transitively() {
    let graph = make_task_graph(2);
    let executed_child = Arc::new(AtomicBool::new(false));
    let ec = Arc::clone(&executed_child);

    // Root task fails
    let (id_root, h_root) = graph.submit(&[], || false);

    // Dependent task 1
    let (id_dep1, h_dep1) = graph.submit(&[id_root], || true);

    // Transitive dependent task 2
    let (_id_dep2, h_dep2) = graph.submit(&[id_dep1], move || {
        ec.store(true, Ordering::SeqCst);
        true
    });

    assert!(!h_root.join());
    assert!(!h_dep1.join());
    assert!(!h_dep2.join());

    // Ensure dependent task was never executed
    assert!(!executed_child.load(Ordering::SeqCst));
}

#[test]
fn test_cancel_pending_task_and_cascade() {
    let graph = make_task_graph(1);
    let executed_cancelled = Arc::new(AtomicBool::new(false));
    let executed_child = Arc::new(AtomicBool::new(false));

    let ec_clone = Arc::clone(&executed_cancelled);
    let child_clone = Arc::clone(&executed_child);

    // Task 1 blocks the single worker thread for a while
    let (_id1, h1) = graph.submit(&[], || {
        thread::sleep(Duration::from_millis(60));
        true
    });

    // Task 2 is waiting in queue
    let (id2, h2) = graph.submit(&[], move || {
        ec_clone.store(true, Ordering::SeqCst);
        true
    });

    // Task 3 depends on Task 2
    let (_id3, h3) = graph.submit(&[id2], move || {
        child_clone.store(true, Ordering::SeqCst);
        true
    });

    // Cancel Task 2 while it's still waiting
    assert!(h2.cancel(), "cancel on pending task should return true");
    // Second cancel should return false
    assert!(!h2.cancel(), "already cancelled task cancel should return false");

    assert!(h1.join());
    assert!(!h2.join(), "cancelled task join should return false");
    assert!(!h3.join(), "task depending on cancelled task should join as false");

    assert!(!executed_cancelled.load(Ordering::SeqCst));
    assert!(!executed_child.load(Ordering::SeqCst));
}

#[test]
fn test_submit_with_already_completed_dependencies() {
    let graph = make_task_graph(2);

    let (id_success, h_succ) = graph.submit(&[], || true);
    assert!(h_succ.join());

    // Submit task depending on already-succeeded task: should run and succeed
    let (_id2, h2) = graph.submit(&[id_success], || true);
    assert!(h2.join());

    let (id_failed, h_fail) = graph.submit(&[], || false);
    assert!(!h_fail.join());

    let executed = Arc::new(AtomicBool::new(false));
    let exec_clone = Arc::clone(&executed);
    // Submit task depending on already-failed task: should be skipped immediately
    let (_id4, h4) = graph.submit(&[id_failed], move || {
        exec_clone.store(true, Ordering::SeqCst);
        true
    });
    assert!(!h4.join());
    assert!(!executed.load(Ordering::SeqCst));
}

#[test]
fn test_multi_dependency_diamond_graph() {
    let graph = make_task_graph(4);
    let counter = Arc::new(AtomicUsize::new(0));

    // Root
    let (id_root, h_root) = graph.submit(&[], || true);

    // Left and Right branches
    let c_left = Arc::clone(&counter);
    let (id_left, h_left) = graph.submit(&[id_root], move || {
        c_left.fetch_add(1, Ordering::SeqCst);
        true
    });

    let c_right = Arc::clone(&counter);
    let (id_right, h_right) = graph.submit(&[id_root], move || {
        c_right.fetch_add(1, Ordering::SeqCst);
        true
    });

    // Join at bottom
    let c_bottom = Arc::clone(&counter);
    let (_id_bottom, h_bottom) = graph.submit(&[id_left, id_right], move || {
        assert_eq!(c_bottom.load(Ordering::SeqCst), 2);
        true
    });

    assert!(h_root.join());
    assert!(h_left.join());
    assert!(h_right.join());
    assert!(h_bottom.join());
}
