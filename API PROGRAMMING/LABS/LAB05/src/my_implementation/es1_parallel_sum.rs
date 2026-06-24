#![allow(dead_code)]
/// Exercise 1 - Calculates the sum of a vector in parallel by dividing it into chunks.
///
/// # Arguments
/// * `_data` - A vector of 64-bit integers to be summed.
/// * `_n_threads` - The number of threads to spawn for the calculation.
///
/// # Returns
/// The total sum of all elements in the vector.
///
/// # Panics
/// This function will panic if `_n_threads` is 0.
///
use std::thread;
pub fn parallel_sum(_data: Vec<i64>, _n_threads: usize) -> i64 {
    // Handle the case where n_threads is zero
    if _n_threads == 0 {
        panic!("Parallel sum of _n_threads cannot be 0");
    }

    // An empty vector has a sum of zero.
    if _data.is_empty() {
        return 0;
    }

    // Divide the vector into n_threads portions of equal size
    // Use ceiling division (a + b - 1) / b to ensure that we don't exceed n_threads
    // If n_threads > data length, set len to 1 to use as many threads as possible
    let len: usize = if _n_threads > _data.len() {
        1
    }
    else {
        (_data.len() + _n_threads - 1) / _n_threads
    };

    let mut threads = Vec::new();

    // Create a thread for each portion and calculate partial sums
    for chunk in _data.chunks(len) {
        // Move a new vector into each thread
        let chunk_owned = chunk.to_vec();

        threads.push(thread::spawn(move || {
            // The thread returns its partial sum through the closure
            chunk_owned.iter().sum::<i64>()
        }))
    }

    // Combine results by retrieving values through join()
    let mut sum: i64 = 0;
    for t in threads {
        match t.join() {
            // Add each thread's partial result to the total
            Ok(result) => sum += result,
            // Panic if a worker thread fails unexpectedly
            Err(_) => panic!("Thread panicked")
        }
    }

    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn somma_vettore_vuoto() {
        let data: Vec<i64> = vec![];
        assert_eq!(parallel_sum(data, 4), 0);
    }

    #[test]
    fn somma_vettore_piccolo() {
        let data = vec![1, 2, 3, 4, 5];
        assert_eq!(parallel_sum(data, 2), 15);
    }

    #[test]
    fn somma_lunghezza_non_multipla() {
        // 10 elementi distribuiti su 3 thread: nessuno deve essere perso o duplicato.
        let data: Vec<i64> = (1..=10).collect();
        assert_eq!(parallel_sum(data, 3), 55);
    }

    #[test]
    fn somma_piu_thread_che_elementi() {
        let data = vec![10, 20, 30];
        assert_eq!(parallel_sum(data, 8), 60);
    }

    #[test]
    fn somma_vettore_grande() {
        let data: Vec<i64> = (1..=10_000).collect();
        let atteso: i64 = data.iter().sum();
        assert_eq!(parallel_sum(data, 7), atteso);
    }

    #[test]
    fn somma_con_valori_negativi() {
        let data = vec![-5, 10, -3, 8, -1];
        assert_eq!(parallel_sum(data, 4), 9);
    }

    #[test]
    fn somma_singolo_thread() {
        let data: Vec<i64> = (1..=100).collect();
        let atteso: i64 = data.iter().sum();
        assert_eq!(parallel_sum(data, 1), atteso);
    }

    #[test]
    fn somma_singolo_elemento() {
        let data = vec![42];
        assert_eq!(parallel_sum(data, 4), 42);
    }

    #[test]
    fn somma_lunghezza_multipla_esatta() {
        // 12 elementi su 4 thread: ogni chunk di dimensione 3 esatta.
        let data: Vec<i64> = (1..=12).collect();
        assert_eq!(parallel_sum(data, 4), 78);
    }

    #[test]
    #[should_panic]
    fn n_threads_zero_e_errore() {
        // Il lab richiede che n_threads == 0 sia gestito come errore.
        let data = vec![1, 2, 3];
        let _ = parallel_sum(data, 0);
    }

    #[test]
    fn somma_consistente_per_diversi_n_threads() {
        // Confronta il risultato di parallel_sum con la somma sequenziale
        // per diversi valori di n_threads: nessun elemento deve essere
        // perso o duplicato.
        let data: Vec<i64> = (-500..=500).collect();
        let atteso: i64 = data.iter().sum();
        for nt in [1usize, 2, 3, 5, 8, 16, 32, 100] {
            assert_eq!(
                parallel_sum(data.clone(), nt),
                atteso,
                "fallito con n_threads = {}",
                nt
            );
        }
    }

    #[test]
    fn somma_grande_con_overflow_evitato() {
        // Numeri grandi ma somma ancora rappresentabile in i64.
        let data: Vec<i64> = vec![1_000_000_000; 1_000];
        assert_eq!(parallel_sum(data, 8), 1_000_000_000_000);
    }
}