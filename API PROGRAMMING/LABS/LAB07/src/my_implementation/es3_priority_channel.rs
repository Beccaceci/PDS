#![allow(dead_code)]
//! Esercizio 3 - PriorityChannel
//!
//! Canale di comunicazione concorrente in cui i messaggi vengono consegnati
//! in ordine di priorità decrescente, con FIFO a parità di priorità.
//! Capacità limitata e chiusura esplicita.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

pub struct PriorityChannel<T: Send + Ord> {
    _marker: PhantomData<T>,
}

impl<T: Send + Ord> PriorityChannel<T> {
    /// Crea un canale con capacità massima `cap` ([std::num::NonZeroUsize]: una capacità
    /// nulla bloccherebbe per sempre ogni `send`).
    pub fn new(_cap: NonZeroUsize) -> Self {
        todo!("Implementare PriorityChannel::new")
    }

    /// Inserisce nel canale il messaggio `t`. La priorità è data dall'`Ord` di
    /// `T` stesso. Se la coda è piena, attende senza CPU che si liberi spazio.
    /// Restituisce `Some(())` in caso di successo, `None` se nel frattempo il
    /// canale è stato chiuso.
    pub fn send(&self, _t: T) -> Option<()> {
        todo!("Implementare PriorityChannel::send")
    }

    /// Estrae dal canale il messaggio a priorità più alta. A parità di
    /// priorità (cioè per messaggi Ord-equivalenti), l'ordine di estrazione è
    /// quello di inserimento (stabile). Se la coda è vuota, attende senza CPU
    /// l'arrivo di un nuovo messaggio. Restituisce `None` se il canale è
    /// chiuso e non sono più presenti messaggi.
    pub fn recv(&self) -> Option<T> {
        todo!("Implementare PriorityChannel::recv")
    }

    /// Chiude il canale, impedendo ulteriori invii. I messaggi già presenti
    /// restano comunque consegnabili tramite `recv`.
    pub fn close(&self) {
        todo!("Implementare PriorityChannel::close")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering as CmpOrdering;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

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

    /// Messaggio etichettato usato per verificare la stabilita' FIFO a parita'
    /// di priorita': `Ord` compara solo il campo `priority`, ma `PartialEq`
    /// guarda anche `tag` cosi' che il test possa distinguere i messaggi.
    /// Si tratta di una violazione consapevole del contratto canonico tra
    /// `Ord` ed `Eq`, utile solo a livello di test.
    #[derive(Debug, Clone, Copy)]
    struct Tagged {
        priority: u32,
        tag: char,
    }
    impl PartialEq for Tagged {
        fn eq(&self, other: &Self) -> bool {
            self.priority == other.priority && self.tag == other.tag
        }
    }
    impl Eq for Tagged {}
    impl Ord for Tagged {
        fn cmp(&self, other: &Self) -> CmpOrdering {
            self.priority.cmp(&other.priority)
        }
    }
    impl PartialOrd for Tagged {
        fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
            Some(self.cmp(other))
        }
    }

    #[test]
    fn send_e_recv_singolo_elemento() {
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<i32>::new(nz(4));
            assert_eq!(ch.send(5), Some(()));
            assert_eq!(ch.recv(), Some(5));
        });
    }

    #[test]
    fn ordine_priorita_decrescente() {
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<i32>::new(nz(10));
            ch.send(1).unwrap();
            ch.send(10).unwrap();
            ch.send(5).unwrap();

            assert_eq!(ch.recv(), Some(10));
            assert_eq!(ch.recv(), Some(5));
            assert_eq!(ch.recv(), Some(1));
        });
    }

    #[test]
    fn fifo_a_parita_di_priorita() {
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<Tagged>::new(nz(10));
            for t in ['a', 'b', 'c', 'd', 'e'] {
                ch.send(Tagged { priority: 7, tag: t }).unwrap();
            }
            for t in ['a', 'b', 'c', 'd', 'e'] {
                assert_eq!(ch.recv(), Some(Tagged { priority: 7, tag: t }));
            }
        });
    }

    #[test]
    fn send_blocca_se_pieno_e_si_sblocca_dopo_recv() {
        run_with_timeout(Duration::from_secs(5), || {
            let ch = Arc::new(PriorityChannel::<i32>::new(nz(2)));
            ch.send(1).unwrap();
            ch.send(2).unwrap();

            let ch_p = Arc::clone(&ch);
            let h = thread::spawn(move || ch_p.send(3));
            thread::sleep(Duration::from_millis(150));
            assert!(!h.is_finished(), "send doveva bloccarsi");

            assert_eq!(ch.recv(), Some(2));
            assert_eq!(h.join().unwrap(), Some(()));
            // Sotto: i due rimasti devono uscire in ordine di priorità.
            let a = ch.recv().unwrap();
            let b = ch.recv().unwrap();
            assert!(a >= b, "priorità rispettata: {} >= {}", a, b);
        });
    }

    #[test]
    fn recv_blocca_se_vuoto_e_si_sblocca_dopo_send() {
        run_with_timeout(Duration::from_secs(5), || {
            let ch = Arc::new(PriorityChannel::<i32>::new(nz(2)));
            let ch_c = Arc::clone(&ch);
            let h = thread::spawn(move || ch_c.recv());
            thread::sleep(Duration::from_millis(150));
            assert!(!h.is_finished(), "recv doveva bloccarsi");
            ch.send(42).unwrap();
            assert_eq!(h.join().unwrap(), Some(42));
        });
    }

    #[test]
    fn close_drena_prima_di_ritornare_none() {
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<i32>::new(nz(8));
            ch.send(1).unwrap();
            ch.send(2).unwrap();
            ch.close();
            assert_eq!(ch.recv(), Some(2));
            assert_eq!(ch.recv(), Some(1));
            assert_eq!(ch.recv(), None);
        });
    }

    #[test]
    fn close_su_canale_vuoto_fa_uscire_recv() {
        run_with_timeout(Duration::from_secs(5), || {
            let ch = Arc::new(PriorityChannel::<i32>::new(nz(2)));
            let ch_c = Arc::clone(&ch);
            let h = thread::spawn(move || ch_c.recv());
            thread::sleep(Duration::from_millis(150));
            ch.close();
            assert_eq!(h.join().unwrap(), None);
        });
    }

    #[test]
    fn close_sblocca_send_in_attesa() {
        run_with_timeout(Duration::from_secs(5), || {
            let ch = Arc::new(PriorityChannel::<i32>::new(nz(1)));
            ch.send(1).unwrap();

            let ch_p = Arc::clone(&ch);
            let h = thread::spawn(move || ch_p.send(2));
            thread::sleep(Duration::from_millis(150));
            assert!(!h.is_finished());
            ch.close();
            assert_eq!(h.join().unwrap(), None);
        });
    }

    #[test]
    fn send_su_canale_chiuso_restituisce_none() {
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<i32>::new(nz(4));
            ch.close();
            assert_eq!(ch.send(1), None);
        });
    }

    #[test]
    fn close_idempotente() {
        // Calls ripetuti a close() devono essere innocui: nessun panic,
        // nessun cambio di comportamento osservabile.
        run_with_timeout(Duration::from_secs(2), || {
            let ch = PriorityChannel::<i32>::new(nz(2));
            ch.close();
            ch.close();
            ch.close();
            assert_eq!(ch.send(1), None);
            assert_eq!(ch.recv(), None);
        });
    }

    #[test]
    fn close_sblocca_tutti_i_sender_bloccati() {
        // Saturiamo il canale e mettiamo in attesa N sender. La chiusura
        // deve sbloccarli TUTTI (con None), non solo uno.
        run_with_timeout(Duration::from_secs(5), || {
            let n_blocked = 4;
            let ch = Arc::new(PriorityChannel::<i32>::new(nz(1)));
            ch.send(0).unwrap(); // saturazione

            let mut handles = vec![];
            for i in 0..n_blocked {
                let ch = Arc::clone(&ch);
                handles.push(thread::spawn(move || ch.send(i as i32 + 1)));
            }
            thread::sleep(Duration::from_millis(200));
            for h in &handles {
                assert!(
                    !h.is_finished(),
                    "tutti i sender dovevano essere bloccati su pieno"
                );
            }
            ch.close();
            for h in handles {
                assert_eq!(
                    h.join().unwrap(),
                    None,
                    "ogni sender bloccato deve risvegliarsi con None alla chiusura"
                );
            }
        });
    }

    #[test]
    fn multi_produttori_singolo_consumatore() {
        run_with_timeout(Duration::from_secs(30), || {
            let ch = Arc::new(PriorityChannel::<u64>::new(nz(16)));
            let n_prod = 4;
            let per_prod = 500u64;

            let mut producers = vec![];
            for p in 0..n_prod {
                let ch = Arc::clone(&ch);
                producers.push(thread::spawn(move || {
                    for i in 0..per_prod {
                        ch.send(p as u64 * per_prod + i).unwrap();
                    }
                }));
            }

            let ch_c = Arc::clone(&ch);
            let count = Arc::new(AtomicUsize::new(0));
            let c2 = Arc::clone(&count);
            let consumer = thread::spawn(move || {
                while ch_c.recv().is_some() {
                    c2.fetch_add(1, Ordering::SeqCst);
                }
            });

            for h in producers {
                h.join().unwrap();
            }
            ch.close();
            consumer.join().unwrap();
            assert_eq!(
                count.load(Ordering::SeqCst),
                (n_prod as u64 * per_prod) as usize
            );
        });
    }
}
