#![allow(dead_code)]
//! Esercizio 2 - WaitGroup
//!
//! Primitiva che permette ad uno o più thread di attendere il completamento
//! di un insieme di operazioni il cui numero non è necessariamente noto al
//! momento della costruzione e può crescere dinamicamente.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::sync::WaitTimeoutResult;
use std::time::Duration;

pub struct WaitGroup {}

impl WaitGroup {
    pub fn new() -> Self {
        todo!("Implementare WaitGroup::new")
    }

    /// Incrementa di `delta` il numero di operazioni pendenti. Può essere
    /// chiamato in qualunque momento, anche da thread già in attesa.
    pub fn increment(&self, _delta: usize) {
        todo!("Implementare WaitGroup::increment")
    }

    /// Decrementa di uno il numero di operazioni pendenti. Quando il contatore
    /// raggiunge zero, tutti i thread in attesa vengono sbloccati. Decrementare
    /// un contatore già a zero deve causare panico.
    pub fn decrement(&self) {
        todo!("Implementare WaitGroup::decrement")
    }

    /// Blocca il chiamante senza consumare cicli di CPU finché il contatore
    /// non raggiunge zero.
    pub fn wait(&self) {
        todo!("Implementare WaitGroup::wait")
    }

    /// Analogo a `wait`, ma con un timeout massimo.
    pub fn wait_timeout(&self, _d: Duration) -> WaitTimeoutResult {
        todo!("Implementare WaitGroup::wait_timeout")
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    fn run_with_timeout<F>(timeout: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = thread::spawn(f);
        let start = Instant::now();
        while start.elapsed() < timeout {
            if handle.is_finished() {
                handle.join().unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("test non terminato entro {:?} (probabile deadlock)", timeout);
    }

    #[test]
    fn wait_su_gruppo_vuoto_e_immediato() {
        run_with_timeout(Duration::from_secs(2), || {
            let wg = WaitGroup::new();
            let t1 = Instant::now();
            wg.wait();
            assert!(t1.elapsed() < Duration::from_millis(100));
        });
    }

    #[test]
    fn increment_e_decrement_sbloccano_wait() {
        run_with_timeout(Duration::from_secs(5), || {
            let wg = Arc::new(WaitGroup::new());
            wg.increment(3);

            let n_completati = Arc::new(AtomicUsize::new(0));
            for _ in 0..3 {
                let wg = Arc::clone(&wg);
                let n = Arc::clone(&n_completati);
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(50));
                    n.fetch_add(1, Ordering::SeqCst);
                    wg.decrement();
                });
            }

            wg.wait();
            assert_eq!(n_completati.load(Ordering::SeqCst), 3);
        });
    }

    #[test]
    fn piu_thread_in_wait_vengono_tutti_sbloccati() {
        run_with_timeout(Duration::from_secs(5), || {
            let wg = Arc::new(WaitGroup::new());
            wg.increment(1);

            let sbloccati = Arc::new(AtomicUsize::new(0));
            let mut waiters = vec![];
            for _ in 0..5 {
                let wg = Arc::clone(&wg);
                let sb = Arc::clone(&sbloccati);
                waiters.push(thread::spawn(move || {
                    wg.wait();
                    sb.fetch_add(1, Ordering::SeqCst);
                }));
            }

            thread::sleep(Duration::from_millis(100));
            assert_eq!(sbloccati.load(Ordering::SeqCst), 0);

            wg.decrement();
            for w in waiters {
                w.join().unwrap();
            }
            assert_eq!(sbloccati.load(Ordering::SeqCst), 5);
        });
    }

    #[test]
    #[should_panic]
    fn decrement_su_contatore_zero_causa_panic() {
        let wg = WaitGroup::new();
        wg.decrement();
    }

    #[test]
    fn wait_timeout_scade_se_nessun_decrement() {
        run_with_timeout(Duration::from_secs(3), || {
            let wg = WaitGroup::new();
            wg.increment(1);
            let t1 = Instant::now();
            let r = wg.wait_timeout(Duration::from_millis(200));
            let el = t1.elapsed();
            assert!(r.timed_out(), "wait_timeout doveva scadere");
            assert!(
                el >= Duration::from_millis(150),
                "wait_timeout doveva attendere il timeout"
            );
        });
    }

    #[test]
    fn wait_timeout_ritorna_subito_se_contatore_zero() {
        run_with_timeout(Duration::from_secs(2), || {
            let wg = WaitGroup::new();
            let t1 = Instant::now();
            let r = wg.wait_timeout(Duration::from_secs(10));
            assert!(!r.timed_out());
            assert!(t1.elapsed() < Duration::from_millis(100));
        });
    }

    #[test]
    fn wait_timeout_riesce_se_decrement_concorrente() {
        // Un waiter entra in wait_timeout con timeout lungo; un altro thread
        // porta il contatore a zero mentre il waiter e' gia' bloccato. Il
        // wait_timeout deve avere SUCCESSO (non timeout) e ritornare appena
        // avviene il decrement, non allo scadere del timeout.
        run_with_timeout(Duration::from_secs(5), || {
            let wg = Arc::new(WaitGroup::new());
            wg.increment(1);

            let wg2 = Arc::clone(&wg);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(100));
                wg2.decrement();
            });

            let t1 = Instant::now();
            let r = wg.wait_timeout(Duration::from_secs(10));
            let el = t1.elapsed();
            assert!(
                !r.timed_out(),
                "wait_timeout doveva avere successo grazie al decrement concorrente"
            );
            assert!(
                el >= Duration::from_millis(80),
                "non doveva sbloccarsi prima del decrement: {:?}",
                el
            );
            assert!(
                el < Duration::from_secs(2),
                "doveva sbloccarsi al decrement, non allo scadere del timeout: {:?}",
                el
            );
        });
    }

    #[test]
    fn increment_dinamico_da_thread_gia_in_attesa() {
        // Un thread chiama increment(1) -> decrement -> increment(1) -> ...
        // mentre un waiter è bloccato su wait(). Verifichiamo che wait
        // completi solo quando il contatore torna a zero alla fine.
        run_with_timeout(Duration::from_secs(10), || {
            let wg = Arc::new(WaitGroup::new());
            let n_ops = 50;
            wg.increment(1);

            let wg_p = Arc::clone(&wg);
            let producer = thread::spawn(move || {
                for _ in 0..n_ops {
                    wg_p.increment(1);
                    thread::sleep(Duration::from_millis(5));
                    wg_p.decrement();
                }
                wg_p.decrement();
            });

            wg.wait();
            producer.join().unwrap();
        });
    }

    #[test]
    fn contatore_puo_riscendere_a_zero_e_risalire() {
        // Verifichiamo che dopo un ciclo completo (counter va a 0 e sblocca
        // i waiter), una nuova increment() rimetta il gruppo in stato bloccante.
        run_with_timeout(Duration::from_secs(5), || {
            let wg = Arc::new(WaitGroup::new());
            wg.increment(1);
            wg.decrement(); // primo ciclo, ora 0
            // wg.wait() ritorna subito perche' counter == 0

            wg.wait();

            wg.increment(1);
            let wg2 = Arc::clone(&wg);
            let sbloccato = Arc::new(AtomicUsize::new(0));
            let sb = Arc::clone(&sbloccato);
            let w = thread::spawn(move || {
                wg2.wait();
                sb.fetch_add(1, Ordering::SeqCst);
            });
            thread::sleep(Duration::from_millis(100));
            assert_eq!(sbloccato.load(Ordering::SeqCst), 0);
            wg.decrement();
            w.join().unwrap();
            assert_eq!(sbloccato.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn increment_zero_e_no_op() {
        // increment(0) non deve modificare il contatore ne' sbloccare/bloccare
        // alcun thread. Sequenza: nuovo WaitGroup -> wait() torna subito;
        // increment(0) -> il contatore resta 0; wait() torna ancora subito.
        // Una implementazione difettosa che chiamasse notify_all su ogni
        // increment, o che incrementasse di 1 a prescindere, fallirebbe qui.
        run_with_timeout(Duration::from_secs(2), || {
            let wg = WaitGroup::new();
            wg.increment(0);
            let t1 = Instant::now();
            wg.wait();
            assert!(t1.elapsed() < Duration::from_millis(100));
            // Sequenza increment/decrement attorno a un increment(0) "neutro".
            wg.increment(1);
            wg.increment(0);
            wg.decrement();
            let t2 = Instant::now();
            wg.wait();
            assert!(t2.elapsed() < Duration::from_millis(100));
        });
    }

    #[test]
    fn stress_concorrente_n_thread() {
        run_with_timeout(Duration::from_secs(30), || {
            let wg = Arc::new(WaitGroup::new());
            let n = 100;
            let lavoro_per_thread = 100;
            wg.increment(n);

            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..n {
                let wg = Arc::clone(&wg);
                let c = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..lavoro_per_thread {
                        c.fetch_add(1, Ordering::SeqCst);
                    }
                    wg.decrement();
                });
            }

            wg.wait();
            assert_eq!(
                counter.load(Ordering::SeqCst),
                n * lavoro_per_thread
            );
        });
    }
}
