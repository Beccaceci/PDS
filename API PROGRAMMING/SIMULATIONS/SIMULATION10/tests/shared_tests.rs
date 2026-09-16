use simulation_010::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::LocalSet;
use tokio::time::sleep;

#[tokio::test]
async fn test_capacity() {
    let pool = make_async_resource_pool(vec![10, 20, 30]);
    assert_eq!(pool.capacity(), 3);

    let empty_pool: _ = make_async_resource_pool::<i32>(vec![]);
    assert_eq!(empty_pool.capacity(), 0);
}

#[tokio::test]
async fn test_basic_acquire_and_get() {
    let pool = make_async_resource_pool(vec!["resource_1".to_string()]);
    let res = pool.acquire().await;
    assert_eq!(res.get(), "resource_1");
}

#[tokio::test]
async fn test_raii_return_on_drop() {
    let pool = make_async_resource_pool(vec![42]);

    {
        let res = pool.acquire().await;
        assert_eq!(*res.get(), 42);
        // While held, acquire_timeout with 20ms should expire to None
        let attempt = pool.acquire_timeout(Duration::from_millis(20)).await;
        assert!(attempt.is_none());
    } // res is dropped here and should be returned synchronously to the pool

    // Pool should now have the resource available again
    let res2 = pool.acquire_timeout(Duration::from_millis(50)).await;
    assert!(res2.is_some());
    assert_eq!(*res2.unwrap().get(), 42);
}

#[tokio::test]
async fn test_exclusive_borrow_and_capacity_limits() {
    let pool = make_async_resource_pool(vec![1, 2]);
    let res1 = pool.acquire().await;
    let res2 = pool.acquire().await;

    // Both items are acquired
    let v1 = *res1.get();
    let v2 = *res2.get();
    assert!((v1 == 1 && v2 == 2) || (v1 == 2 && v2 == 1));

    // A 3rd acquire must timeout because capacity is 2
    let start = Instant::now();
    let res3 = pool.acquire_timeout(Duration::from_millis(60)).await;
    assert!(res3.is_none());
    assert!(start.elapsed() >= Duration::from_millis(50));

    // Drop one resource
    drop(res1);

    // Now acquiring 3rd should succeed
    let res3 = pool.acquire_timeout(Duration::from_millis(60)).await;
    assert!(res3.is_some());
    assert_eq!(*res3.unwrap().get(), v1);
}

#[tokio::test]
async fn test_acquire_timeout_expiration() {
    let pool = make_async_resource_pool(vec![100]);
    let _held = pool.acquire().await;

    let start = Instant::now();
    let res = pool.acquire_timeout(Duration::from_millis(80)).await;
    let elapsed = start.elapsed();

    assert!(res.is_none());
    assert!(elapsed >= Duration::from_millis(70));
}

#[tokio::test]
async fn test_acquire_timeout_success_before_expiry() {
    let local = LocalSet::new();
    let pool = Arc::new(make_async_resource_pool(vec![999]));
    let pool_clone = Arc::clone(&pool);

    local
        .run_until(async move {
            // Task 1: acquires immediately, holds for 40ms, then drops
            tokio::task::spawn_local(async move {
                let res = pool_clone.acquire().await;
                sleep(Duration::from_millis(40)).await;
                drop(res);
            });

            // Brief yield so task 1 acquires first
            sleep(Duration::from_millis(10)).await;

            let start = Instant::now();
            let res = pool.acquire_timeout(Duration::from_millis(150)).await;
            let elapsed = start.elapsed();

            assert!(res.is_some());
            assert_eq!(*res.unwrap().get(), 999);
            assert!(elapsed < Duration::from_millis(120));
        })
        .await;
}

#[tokio::test]
async fn test_empty_pool_acquire_timeout() {
    let empty_pool: _ = make_async_resource_pool::<String>(vec![]);
    assert_eq!(empty_pool.capacity(), 0);

    let start = Instant::now();
    let res = empty_pool.acquire_timeout(Duration::from_millis(30)).await;
    assert!(res.is_none());
    assert!(start.elapsed() >= Duration::from_millis(25));
}

#[tokio::test]
async fn test_multiple_rounds_sequential_reuse() {
    let pool = make_async_resource_pool(vec![10, 20, 30]);

    for round in 0..5 {
        let r1 = pool.acquire().await;
        let r2 = pool.acquire().await;
        let r3 = pool.acquire().await;

        let mut values = vec![*r1.get(), *r2.get(), *r3.get()];
        values.sort();
        assert_eq!(values, vec![10, 20, 30], "Round {} failed", round);

        drop(r1);
        drop(r2);
        drop(r3);
    }
}

#[tokio::test]
async fn test_concurrent_multi_task_stress() {
    const NUM_RESOURCES: usize = 5;
    const NUM_TASKS: usize = 30;
    const ITERATIONS_PER_TASK: usize = 10;

    let items: Vec<usize> = (0..NUM_RESOURCES).collect();
    let pool = Arc::new(make_async_resource_pool(items));
    let completed_ops = Arc::new(AtomicUsize::new(0));

    let local = LocalSet::new();

    local
        .run_until(async move {
            let mut handles = Vec::new();

            for _ in 0..NUM_TASKS {
                let pool_c = Arc::clone(&pool);
                let completed_c = Arc::clone(&completed_ops);

                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..ITERATIONS_PER_TASK {
                        let res = pool_c.acquire().await;
                        let val = *res.get();
                        assert!(val < NUM_RESOURCES);
                        sleep(Duration::from_millis(2)).await;
                        completed_c.fetch_add(1, Ordering::SeqCst);
                        drop(res);
                    }
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(
                completed_ops.load(Ordering::SeqCst),
                NUM_TASKS * ITERATIONS_PER_TASK
            );
        })
        .await;
}

#[tokio::test]
async fn test_high_contention_and_spurious_wakeups() {
    const NUM_RESOURCES: usize = 3;
    const NUM_TASKS: usize = 20;

    let items: Vec<usize> = (1..=NUM_RESOURCES).collect();
    let pool = Arc::new(make_async_resource_pool(items));
    let success_count = Arc::new(AtomicUsize::new(0));

    let local = LocalSet::new();

    local
        .run_until(async move {
            let mut handles = Vec::new();

            for _ in 0..NUM_TASKS {
                let pool_c = Arc::clone(&pool);
                let success_c = Arc::clone(&success_count);

                handles.push(tokio::task::spawn_local(async move {
                    for _ in 0..15 {
                        // Mix of acquire and acquire_timeout
                        if let Some(res) = pool_c.acquire_timeout(Duration::from_millis(25)).await {
                            let val = *res.get();
                            assert!(val >= 1 && val <= NUM_RESOURCES);
                            sleep(Duration::from_millis(1)).await;
                            success_c.fetch_add(1, Ordering::SeqCst);
                            drop(res);
                        }
                    }
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }

            // After all tasks finish, all resources must be back in the pool
            let mut acquired = Vec::new();
            for _ in 0..NUM_RESOURCES {
                let res = pool.acquire_timeout(Duration::from_millis(100)).await;
                assert!(res.is_some(), "All resources should be returned to the pool");
                acquired.push(res.unwrap());
            }
            assert_eq!(acquired.len(), NUM_RESOURCES);
        })
        .await;
}
