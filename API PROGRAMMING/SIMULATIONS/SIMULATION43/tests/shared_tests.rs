use simulation_043::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let root = make_root_scope();
    assert_send_sync(&root);
}

#[test]
fn test_root_initial_state_not_cancelled() {
    let root = make_root_scope();
    assert!(!root.is_cancelled());
}

#[test]
fn test_cancel_root_cancels_root() {
    let root = make_root_scope();
    root.cancel();
    assert!(root.is_cancelled());
}

#[test]
fn test_cancellation_propagates_to_descendants() {
    let root = make_root_scope();
    let child1 = root.child();
    let child2 = root.child();
    let grandchild = child1.child();

    assert!(!root.is_cancelled());
    assert!(!child1.is_cancelled());
    assert!(!child2.is_cancelled());
    assert!(!grandchild.is_cancelled());

    // Cancel root -> all descendants must be cancelled
    root.cancel();

    assert!(root.is_cancelled());
    assert!(child1.is_cancelled());
    assert!(child2.is_cancelled());
    assert!(grandchild.is_cancelled());
}

#[test]
fn test_cancelling_child_does_not_cancel_parent_or_sibling() {
    let root = make_root_scope();
    let child1 = root.child();
    let child2 = root.child();

    child1.cancel();

    assert!(child1.is_cancelled());
    assert!(!root.is_cancelled());
    assert!(!child2.is_cancelled());
}

#[test]
fn test_child_created_from_cancelled_parent_is_born_cancelled() {
    let root = make_root_scope();
    root.cancel();

    let child = root.child();
    let grandchild = child.child();

    assert!(child.is_cancelled());
    assert!(grandchild.is_cancelled());
}

#[test]
fn test_dropped_children_do_not_prevent_parent_cancellation() {
    let root = make_root_scope();

    {
        let _child = root.child();
        let _grandchild = _child.child();
        // Children dropped here
    }

    let surviving_child = root.child();

    // Cancelling root should gracefully skip dropped Weak references
    root.cancel();

    assert!(root.is_cancelled());
    assert!(surviving_child.is_cancelled());
}

#[test]
fn test_clone_shares_the_same_scope() {
    let root = make_root_scope();
    let root_clone = root.clone();

    assert!(!root.is_cancelled());
    assert!(!root_clone.is_cancelled());

    root_clone.cancel();

    assert!(root.is_cancelled());
    assert!(root_clone.is_cancelled());
}

#[test]
fn test_high_concurrency_multi_level_tree() {
    let root = Arc::new(make_root_scope());
    let mut handles = Vec::new();
    let cancelled_observed = Arc::new(AtomicBool::new(false));

    for _ in 0..4 {
        let r = Arc::clone(&root);
        let co = Arc::clone(&cancelled_observed);
        handles.push(thread::spawn(move || {
            let child = r.child();
            let subchild = child.child();

            while !subchild.is_cancelled() {
                thread::sleep(Duration::from_millis(5));
            }
            co.store(true, Ordering::SeqCst);
        }));
    }

    thread::sleep(Duration::from_millis(30));
    assert!(!cancelled_observed.load(Ordering::SeqCst));

    root.cancel();

    for h in handles {
        h.join().unwrap();
    }

    assert!(cancelled_observed.load(Ordering::SeqCst));
    assert!(root.is_cancelled());
}