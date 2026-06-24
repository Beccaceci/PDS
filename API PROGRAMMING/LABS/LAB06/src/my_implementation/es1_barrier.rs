#![allow(dead_code)]
//! Esercizio 1 - Barriera di sincronizzazione
//!
//! Implementare una barriera di sincronizzazione riusabile. Il metodo `wait`
//! deve bloccare il thread chiamante finché tutti gli `n` partecipanti non
//! hanno raggiunto la barriera. L'attesa deve essere passiva (nessun polling).
//!
//! Vedi il file `Lab6.md` per la specifica completa.

use std::arch::aarch64::int32x2_t;
use std::sync::{Condvar, Mutex};

/// Barriera di sincronizzazione riusabile per `n` thread.
pub struct MyBarrier {
    queue: Mutex<BarrierState>,
    cv: Condvar,
    capacity: usize,
}

struct BarrierState {
    n_threads: usize,
    current_run: usize
}

impl BarrierState {
    pub fn new() -> Self {
        Self {
            n_threads: 0,
            current_run: 0
        }
    }
}

impl MyBarrier {
    /// Crea una nuova barriera per `n` partecipanti.
    pub fn new(_n: usize) -> Self {
        Self {
            queue: Mutex::new(BarrierState::new()),
            cv: Condvar::new(),
            capacity: _n
        }

    }

    /// Blocca il thread chiamante finché tutti gli `n` partecipanti
    /// non hanno chiamato a loro volta `wait()`.
    pub fn wait(&self) {
        let mut queue = self.queue.lock().unwrap();

        // increment the number of threads that have reached the barrier
        queue.n_threads += 1;

        let actual_num_threads:usize = queue.n_threads;
        let actual_run:usize = queue.current_run;

        if actual_num_threads == self.capacity && actual_run == queue.current_run {
            // it should signal to the others one that N threads have reached the barrier and a new run will be executed
            queue.n_threads = 0;
            queue.current_run = actual_run + 1;

            drop(queue); // release the lock before notifying all the threads
            self.cv.notify_all();
        }
        else {
            // wait while until N threads have reached the barrier
            queue = self.cv.wait_while(queue, |q| {
                q.n_threads < self.capacity && actual_run == q.current_run
            }).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn barriera_singolo_thread() {
        // n = 1 => wait() ritorna subito.
        let b = MyBarrier::new(1);
        b.wait();
    }

    #[test]
    fn barriera_sblocca_tutti() {
        let n = 4;
        let b = Arc::new(MyBarrier::new(n));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        for _ in 0..n {
            let b = Arc::clone(&b);
            let c = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                // Tutti incrementano prima della barriera, attendono, poi controllano.
                c.fetch_add(1, Ordering::SeqCst);
                b.wait();
                // Dopo la barriera tutti devono vedere n incrementi.
                assert_eq!(c.load(Ordering::SeqCst), n);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn barriera_blocca_finche_non_arrivano_tutti() {
        let n = 3;
        let b = Arc::new(MyBarrier::new(n));
        let arrivati_dopo = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        // Due thread arrivano subito.
        for _ in 0..n - 1 {
            let b = Arc::clone(&b);
            let a = Arc::clone(&arrivati_dopo);
            handles.push(thread::spawn(move || {
                b.wait();
                a.fetch_add(1, Ordering::SeqCst);
            }));
        }
        // Diamo tempo ai due thread di mettersi in attesa.
        thread::sleep(Duration::from_millis(100));
        assert_eq!(arrivati_dopo.load(Ordering::SeqCst), 0);

        // Il terzo thread sblocca tutti.
        let b2 = Arc::clone(&b);
        let a2 = Arc::clone(&arrivati_dopo);
        handles.push(thread::spawn(move || {
            b2.wait();
            a2.fetch_add(1, Ordering::SeqCst);
        }));

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(arrivati_dopo.load(Ordering::SeqCst), n);
    }

    #[test]
fn barriera_riusabile() {
        let (tx, rx) = std::sync::mpsc::channel::<usize>();
        let n = 3;
        let giri = 5;
        let b = Arc::new(MyBarrier::new(n));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..n {
            let b = Arc::clone(&b);
            let c = Arc::clone(&counter);
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for g in 1..giri+1 {
                    c.fetch_add(1, Ordering::SeqCst);
                    b.wait();
                    tx.send(g).unwrap();
                }
            }));
        }
        drop(tx);
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), n * giri);
        let mut giro = 0 as usize;
        while let Ok(g) = rx.recv() {
            assert!(g >= giro, "Giro ricevuto {g} ma ci aspettavamo un numero non maggiore di {giro}");
            giro = g;
        }
    }
}
