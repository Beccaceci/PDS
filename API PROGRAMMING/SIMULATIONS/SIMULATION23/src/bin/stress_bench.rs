use simulation_023::lock_free_circuit_breaker::LockFreeCircuitBreaker;
use simulation_023::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn benchmark_closed_throughput<CB: CircuitBreaker<u64> + 'static>(
    name: &str,
    cb: CB,
    threads_count: usize,
    operations_per_thread: usize,
) {
    let successful_calls = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for thread_id in 0..threads_count {
        let cb_clone = cb.clone();
        let sc = Arc::clone(&successful_calls);

        handles.push(thread::spawn(move || {
            for i in 0..operations_per_thread {
                let res = cb_clone.call(|| Ok((thread_id * operations_per_thread + i) as u64));
                if res.is_ok() {
                    sc.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = threads_count * operations_per_thread;
    let ops_per_sec = (total_ops as f64) / elapsed.as_secs_f64();
    let avg_latency_ns = (elapsed.as_nanos() as f64) / (total_ops as f64);

    println!(
        "  {:<32} | {:>10} ops | {:>10.3?} | {:>12.2} ops/s | {:>8.1} ns/op",
        name, total_ops, elapsed, ops_per_sec, avg_latency_ns
    );
}

fn benchmark_contention_and_failures<CB: CircuitBreaker<u64> + 'static>(
    name: &str,
    cb: CB,
    threads_count: usize,
    iterations: usize,
) {
    let start = Instant::now();
    let total_calls = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for thread_id in 0..threads_count {
        let cb_clone = cb.clone();
        let tc = Arc::clone(&total_calls);

        handles.push(thread::spawn(move || {
            for i in 0..iterations {
                tc.fetch_add(1, Ordering::Relaxed);
                // Inietta un 5% di fallimenti per forzare transizioni di stato e contesa
                let should_fail = (thread_id + i) % 20 == 0;
                let _ = cb_clone.call(|| {
                    if should_fail {
                        Err(())
                    } else {
                        Ok(42)
                    }
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = threads_count * iterations;
    let ops_per_sec = (total_ops as f64) / elapsed.as_secs_f64();

    println!(
        "  {:<32} | {:>10} ops | {:>10.3?} | {:>12.2} ops/s",
        name, total_ops, elapsed, ops_per_sec
    );
}

fn main() {
    println!("\n==========================================================================================");
    println!("  🚀 BENCHMARK COMPARATIVO: Mutex CircuitBreaker VS Lock-Free Atomic CAS CircuitBreaker");
    println!("==========================================================================================\n");

    let num_threads = 16;
    let ops_per_thread = 100_000;

    println!("📊 1. TEST DI THROUGHPUT PURO A CIRCUITO CHIUSO (16 Thread, 1.600.000 Chiamate)");
    println!("------------------------------------------------------------------------------------------");
    println!("  {:<32} | {:>10}     | {:>10}     | {:>14} | {:>10}", "Implementazione", "Operazioni", "Tempo", "Throughput", "Latenza");
    println!("------------------------------------------------------------------------------------------");

    // 1. Mutex-based
    let mutex_cb = make_circuit_breaker::<u64>(5, Duration::from_millis(50));
    benchmark_closed_throughput("Standard (Mutex)", mutex_cb, num_threads, ops_per_thread);

    // 2. Lock-Free
    let lock_free_cb = LockFreeCircuitBreaker::new(5, Duration::from_millis(50));
    benchmark_closed_throughput("Lock-Free (AtomicU64 CAS)", lock_free_cb, num_threads, ops_per_thread);

    println!("------------------------------------------------------------------------------------------\n");

    println!("📊 2. TEST DI ALTA CONTESA CON INIEZIONE DI FALLIMENTI (16 Thread, 800.000 Chiamate)");
    println!("------------------------------------------------------------------------------------------");
    println!("  {:<32} | {:>10}     | {:>10}     | {:>14}", "Implementazione", "Operazioni", "Tempo", "Throughput");
    println!("------------------------------------------------------------------------------------------");

    let mutex_cb_stress = make_circuit_breaker::<u64>(10, Duration::from_micros(200));
    benchmark_contention_and_failures("Standard (Mutex)", mutex_cb_stress, num_threads, 50_000);

    let lock_free_cb_stress = LockFreeCircuitBreaker::new(10, Duration::from_micros(200));
    benchmark_contention_and_failures("Lock-Free (AtomicU64 CAS)", lock_free_cb_stress, num_threads, 50_000);

    println!("==========================================================================================\n");
}
