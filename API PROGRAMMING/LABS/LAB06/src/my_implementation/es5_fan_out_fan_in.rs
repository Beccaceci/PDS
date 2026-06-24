#![allow(dead_code)]
//! Esercizio 5 - Fan-out / fan-in con `crossbeam::channel`
//!
//! Calcola in parallelo, su una grande lista di numeri, una funzione costosa
//! (simulata da una somma di cifre con un piccolo `thread::sleep`), usando
//! canali MPMC di `crossbeam_channel` per distribuire i task ai worker e
//! raccogliere i risultati in un thread aggregator.
//!
//! Vedi il file `Lab6.md` per la specifica completa.

use crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;

/// Each worker thread consumes from the same 'rx' (Fan-out) and sends results to the same 'tx' (Fan-in)
fn worker(rx: Receiver<i64>, tx: Sender<(i64, u32)>) {
    for value in rx {
        let result = somma_cifre(value);
        if tx.send((value, result)).is_err() {
            break; // Aggregator stopped listening
        }
    }
}

pub fn fan_out_fan_in(input: Vec<i64>, n_workers: usize) -> Vec<(i64, u32)> {
    // Stage 1: Create MPMC channels
    let (tx_work, rx_work) = unbounded::<i64>();
    let (tx_res, rx_res) = unbounded::<(i64, u32)>();

    // Stage 2: Spawn the worker pool (Fan-out)
    let mut worker_handles = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let rx = rx_work.clone();
        let tx = tx_res.clone();
        worker_handles.push(thread::spawn(move || worker(rx, tx)));
    }

    // Stage 3: Dispatch tasks
    // We do this in a separate thread so we can start collecting results immediately
    thread::spawn(move || {
        for val in input {
            let _ = tx_work.send(val);
        }
        // When this thread ends, tx_work is dropped
        // Workers will see an empty channel and exit their loops.
    });

    // Stage 4: Aggregate results (Fan-in)
    // We must drop our local reference to tx_res so the aggregator knows
    // when ALL workers are done.
    drop(tx_res);

    let mut results = Vec::new();
    // This loop continues until all clones of tx_res (in all workers) are dropped.
    while let Ok(res) = rx_res.recv() {
        results.push(res);
    }

    // Wait for workers to clean up
    for h in worker_handles {
        let _ = h.join();
    }

    results
}

pub fn somma_cifre(x: i64) -> u32 {
    let mut n = x.unsigned_abs();
    let mut s: u32 = 0;
    while n > 0 {
        s += (n % 10) as u32;
        n /= 10;
    }
    // Simulate expensive calculation
    std::thread::sleep(std::time::Duration::from_micros(100));
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn input_vuoto() {
        let v = fan_out_fan_in(vec![], 4);
        assert!(v.is_empty());
    }

    #[test]
    fn risultati_corretti_pochi_elementi() {
        let input = vec![0, 5, 12, 99, 123];
        let v = fan_out_fan_in(input.clone(), 2);
        assert_eq!(v.len(), input.len());
        let map: HashMap<i64, u32> = v.into_iter().collect();
        assert_eq!(map[&0], 0);
        assert_eq!(map[&5], 5);
        assert_eq!(map[&12], 3);
        assert_eq!(map[&99], 18);
        assert_eq!(map[&123], 6);
    }

    #[test]
    fn risultati_corretti_molti_elementi() {
        let input: Vec<i64> = (0..500).collect();
        let v = fan_out_fan_in(input.clone(), 8);
        assert_eq!(v.len(), input.len());

        let map: HashMap<i64, u32> = v.into_iter().collect();
        for x in &input {
            let mut atteso: u32 = 0;
            let mut n = x.unsigned_abs();
            while n > 0 {
                atteso += (n % 10) as u32;
                n /= 10;
            }
            assert_eq!(map.get(x).copied(), Some(atteso), "x = {x}");
        }
    }

    #[test]
    fn nessun_elemento_duplicato_o_perso() {
        // Anche con molti worker, ogni input deve essere processato
        // esattamente una volta.
        let input: Vec<i64> = (1..=1000).collect();
        let v = fan_out_fan_in(input.clone(), 16);
        assert_eq!(v.len(), input.len());

        let mut chiavi: Vec<i64> = v.iter().map(|(k, _)| *k).collect();
        chiavi.sort();
        assert_eq!(chiavi, input);
    }
}
