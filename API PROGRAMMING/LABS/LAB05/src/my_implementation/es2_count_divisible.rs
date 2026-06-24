#![allow(dead_code)]
//! Esercizio 2 - Conteggio parallelo con stato condiviso
//!
//! Implementare la funzione `count_divisible` che conta quanti elementi di
//! `data` sono divisibili per `k`, usando `n_threads` thread che cooperano
//! aggiornando un contatore condiviso `Arc<Mutex<usize>>`.
//!
//! Vedi il file `Lab5.md` per la specifica completa.

use std::slice::Iter;
/// Restituisce il numero di elementi di `data` divisibili per `k`,
/// usando `n_threads` thread.

use std::thread;
use std::sync::{Arc, Mutex};

pub fn count_divisible(_data: Vec<u32>, _k: u32, _n_threads: usize) -> usize {
    // Input validation
    if _n_threads == 0 {
        panic!("Parallel sum of _n_threads cannot be 0");
    }

    if _data.is_empty() {
        return 0;
    }

    // Shared state initialization
    // Arc is required to share ownership across threads
    // Mutex ensures safe concurrent mutation
    let shared_counter: Arc<Mutex<usize>> = Arc::new(Mutex::new(0usize));

    // If threads > data length → assign at least 1 element per thread
    // Otherwise → use ceiling division to distribute work evenly
    let len: usize = if _n_threads > _data.len() {
        1
    } else {
        (_data.len() + _n_threads - 1) / _n_threads
    };

    // Keeps track of thread handles for later synchronization
    let mut threads = Vec::new();

    // Each iteration defines the work assigned to a thread
    for chunk in _data.chunks(len) {
        // Convert slice into owned Vec
        // Necessary because threads require 'static ownership
        let chunk_owned = chunk.to_vec();

        // Local variable declared outside closure for clarity,
        // but used only inside the thread (moved into closure).
        let mut local_counter = 0usize;

        // Clone Arc BEFORE spawning thread
        // Each thread needs its own reference to shared data.
        let shared_data = shared_counter.clone();

        threads.push(thread::spawn(move || {
            // Local computation without locking
            local_counter = chunk_owned
                .iter()
                .filter(|&x| x % _k == 0)
                .count();

            // Lock is acquired only to update shared state
            let mut v = shared_data.lock().unwrap();
            *v += local_counter;
        }));
    }

    // Ensures all computations are completed before reading the result
    for t in threads {
        match t.join() {
            Ok(_) => (),
            // Fail if any thread panics
            Err(_) => panic!("Thread panicked"),
        }
    }

    // Return the final result only after all threads have joined
    // Lock is required to safely read the shared value
    shared_counter.lock().unwrap().clone()
}


/// Restituisce il numero di elementi di `data` divisibili per `k`,
/// usando `n_threads` thread che aggiornano un contatore atomico condiviso.

use std::sync::atomic::{AtomicUsize, Ordering};

pub fn count_divisible_atomic(_data: Vec<u32>, _k: u32, _n_threads: usize) -> usize {
    // Input validation
    if _n_threads == 0 {
        panic!("Parallel sum of _n_threads cannot be 0");
    }

    if _data.is_empty() {
        return 0;
    }

    // Shared state initialization
    // Arc is required to share ownership across threads
    // AtomicUsize ensures correct concurrent computations without explicity locking shared resources
    let shared_counter = Arc::new(AtomicUsize::new(0usize));

    // If threads > data length → assign at least 1 element per thread
    // Otherwise → use ceiling division to distribute work evenly
    let len: usize = if _n_threads > _data.len() {
        1
    } else {
        (_data.len() + _n_threads - 1) / _n_threads
    };

    // Keeps track of thread handles for later synchronization
    let mut threads = Vec::new();

    // Each iteration defines the work assigned to a thread
    for chunk in _data.chunks(len) {
        // Convert slice into owned Vec
        // Necessary because threads require 'static ownership
        let chunk_owned = chunk.to_vec();

        // Local variable declared outside closure for clarity,
        // but used only inside the thread (moved into closure).
        let mut local_counter = 0usize;

        // Clone Arc BEFORE spawning thread
        // Each thread needs its own reference to shared data.
        let shared_counter_clone = Arc::clone(&shared_counter);


        threads.push(thread::spawn(move || {
            // Local computation without locking
            local_counter = chunk_owned
                .iter()
                .filter(|&x| x % _k == 0)
                .count();

            // fetch_add ensures the atomicity of the updates at hardware level
            shared_counter_clone.fetch_add(local_counter, Ordering::Relaxed);
        }));
    }

    // Ensures all computations are completed before reading the result
    for t in threads {
        match t.join() {
            Ok(_) => (),
            // Fail if any thread panics
            Err(_) => panic!("Thread panicked"),
        }
    }

    // Return the final result only after all threads have joined
    // Lock is not required to safely read the shared value beacause load ensures it at hardware level
    shared_counter.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nessun_divisibile() {
        let data = vec![1, 3, 5, 7, 9];
        assert_eq!(count_divisible(data, 2, 4), 0);
    }

    #[test]
    fn tutti_divisibili() {
        let data = vec![6, 12, 18, 24];
        assert_eq!(count_divisible(data, 3, 2), 4);
    }

    #[test]
    fn divisibili_per_uno() {
        let data: Vec<u32> = (1..=100).collect();
        assert_eq!(count_divisible(data, 1, 4), 100);
    }

    #[test]
    fn vettore_grande() {
        let data: Vec<u32> = (1..=10_000).collect();
        let atteso = data.iter().filter(|&&x| x % 7 == 0).count();
        assert_eq!(count_divisible(data, 7, 5), atteso);
    }

    #[test]
    fn vettore_vuoto() {
        let data: Vec<u32> = vec![];
        assert_eq!(count_divisible(data, 3, 4), 0);
    }

    #[test]
    fn singolo_thread() {
        let data: Vec<u32> = (1..=100).collect();
        let atteso = data.iter().filter(|&&x| x % 5 == 0).count();
        assert_eq!(count_divisible(data, 5, 1), atteso);
    }

    #[test]
    fn piu_thread_che_elementi() {
        let data = vec![2, 4, 7];
        // 7 non è divisibile per 2; 2 e 4 sì.
        assert_eq!(count_divisible(data, 2, 16), 2);
    }

    // ----- Test per la versione atomica -----

    #[test]
    fn atomic_nessun_divisibile() {
        let data = vec![1, 3, 5, 7, 9];
        assert_eq!(count_divisible_atomic(data, 2, 4), 0);
    }

    #[test]
    fn atomic_tutti_divisibili() {
        let data = vec![6, 12, 18, 24];
        assert_eq!(count_divisible_atomic(data, 3, 2), 4);
    }

    #[test]
    fn atomic_vettore_vuoto() {
        let data: Vec<u32> = vec![];
        assert_eq!(count_divisible_atomic(data, 3, 4), 0);
    }

    #[test]
    fn atomic_vettore_grande() {
        let data: Vec<u32> = (1..=10_000).collect();
        let atteso = data.iter().filter(|&&x| x % 7 == 0).count();
        assert_eq!(count_divisible_atomic(data, 7, 5), atteso);
    }

    #[test]
    fn coerenza_mutex_atomic() {
        // Le due implementazioni devono restituire esattamente lo stesso
        // risultato per qualunque combinazione di input.
        let data: Vec<u32> = (0..20_000).map(|i| (i * 13 + 7) % 1000).collect();
        for k in [1u32, 2, 3, 5, 7, 11, 100] {
            for nt in [1usize, 2, 4, 8] {
                let a = count_divisible(data.clone(), k, nt);
                let b = count_divisible_atomic(data.clone(), k, nt);
                assert_eq!(a, b, "divergenza per k={}, n_threads={}", k, nt);
            }
        }
    }

    #[test]
    fn nessuna_perdita_di_aggiornamenti() {
        // Stress test: molti thread, vettore grande, k = 1 (tutti divisibili).
        // Il conteggio finale deve essere esattamente data.len() in entrambe
        // le implementazioni: una eventuale race condition farebbe perdere
        // aggiornamenti.
        let data: Vec<u32> = (1..=50_000).collect();
        assert_eq!(count_divisible(data.clone(), 1, 16), 50_000);
        assert_eq!(count_divisible_atomic(data, 1, 16), 50_000);
    }
}
