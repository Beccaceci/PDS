#![allow(dead_code)]
//! Esercizio 3 - Pipeline a stadi con canali `mpsc`
//!
//! Realizzare una pipeline a tre stadi (Generator -> Transformer -> Collector)
//! che comunicano tra loro tramite canali `std::sync::mpsc`.
//!
//! Vedi il file `Lab6.md` per la specifica completa.

use std::sync::mpsc;
use std::thread;
use crossbeam_channel::{Receiver, Sender};
use crate::es2_mpsc::channel;

/// Stage 1: Generator - Produces numbers from 1 to n and sends them
fn stage_one(tx: mpsc::Sender<i64>, _n: i64) {
    for i in 1..=_n as usize {
        tx.send(i as i64).unwrap();
    }
}

/// Stage 2: Transformer - Receives x and sends x*x
fn stage_two(rx: mpsc::Receiver<i64>, tx: mpsc::Sender<i64>) {
    while let Ok(value) = rx.recv() {
        tx.send(value*value).unwrap();
    }
}

/// Stage 3: Collector - Accumulates results into a Vec
fn stage_three(rx: mpsc::Receiver<i64>) -> Vec<i64> {
    let mut results = Vec::new();
    while let Ok(value) = rx.recv() {
        results.push(value);
    }
    results
}
/// Esegue la pipeline producendo i numeri da 1 a `n`, calcolandone il quadrato
/// nel secondo stadio e raccogliendoli nel terzo stadio.
///
/// L'ordine degli elementi nel vettore restituito deve coincidere con quello
/// di produzione, ovvero `[1, 4, 9, 16, ..., n*n]`.
pub fn run_pipeline(_n: i64) -> Vec<i64> {
    // Channel between Generator and Transformer
    let (tx1, rx1) = mpsc::channel();
    // Channel between Transformer and Collector
    let (tx2, rx2) = mpsc::channel();

    // Spawn the three distinct threads
    thread::spawn(move || stage_one(tx1, _n));
    thread::spawn(move || stage_two(rx1, tx2));

    // The Collector runs in the third thread; we keep the handle to get the Vec
    let collector_handle = thread::spawn(move || stage_three(rx2));

    // Join the collector thread and return the final vector[span_21](end_span)
    collector_handle.join().expect("Collector thread panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_zero_elementi() {
        let v = run_pipeline(0);
        assert!(v.is_empty());
    }

    #[test]
    fn pipeline_pochi_elementi() {
        let v = run_pipeline(5);
        assert_eq!(v, vec![1, 4, 9, 16, 25]);
    }

    #[test]
    fn pipeline_molti_elementi() {
        let n = 1000;
        let v = run_pipeline(n);
        assert_eq!(v.len(), n as usize);
        for (i, x) in v.iter().enumerate() {
            let k = (i + 1) as i64;
            assert_eq!(*x, k * k);
        }
    }
}
