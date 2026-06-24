#![allow(dead_code)]
//! Esercizio 4 - Watchdog
//!
//! Timer "cane da guardia" che invoca una callback se non viene "feedato"
//! entro un certo tempo. Il watchdog incapsula un thread interno che attende
//! lo scadere del tempo senza consumare CPU.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::time::Duration;

pub struct Watchdog {}

impl Watchdog {
    /// Crea un watchdog con scadenza `timeout` e callback `on_timeout`.
    /// La callback viene invocata al massimo una volta.
    pub fn new<F>(_timeout: Duration, _on_timeout: F) -> Self
    where
        F: Fn() + Send + 'static,
    {
        todo!("Implementare Watchdog::new")
    }

    /// Rinvia la scadenza del watchdog a `now + timeout`. Se la scadenza è
    /// già trascorsa (callback già invocata), la chiamata non ha effetto e
    /// restituisce `false`; altrimenti restituisce `true`.
    pub fn feed(&self) -> bool {
        todo!("Implementare Watchdog::feed")
    }

    /// Indica se la callback è già stata invocata e non ancora re-feedata.
    pub fn is_expired(&self) -> bool {
        todo!("Implementare Watchdog::is_expired")
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        todo!(
            "Implementare Drop per Watchdog: cancella la callback se non è
             ancora stata invocata e attende la terminazione del thread interno"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn watchdog_scade_e_invoca_callback() {
        let flag = Arc::new(AtomicBool::new(false));
        let fc = Arc::clone(&flag);
        let wd = Watchdog::new(Duration::from_millis(100), move || {
            fc.store(true, Ordering::SeqCst);
        });

        assert!(!wd.is_expired());
        thread::sleep(Duration::from_millis(250));
        assert!(flag.load(Ordering::SeqCst), "la callback doveva essere invocata");
        assert!(wd.is_expired());
    }

    #[test]
    fn feed_rinvia_la_scadenza() {
        let flag = Arc::new(AtomicBool::new(false));
        let fc = Arc::clone(&flag);
        let wd = Watchdog::new(Duration::from_millis(150), move || {
            fc.store(true, Ordering::SeqCst);
        });

        // Per ~300ms feediamo ogni 50ms: la callback non deve mai partire.
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(300) {
            thread::sleep(Duration::from_millis(50));
            assert!(wd.feed());
        }
        assert!(!flag.load(Ordering::SeqCst));
        assert!(!wd.is_expired());

        // Smettiamo di feedare: dopo ~200ms la callback deve essere partita.
        thread::sleep(Duration::from_millis(300));
        assert!(flag.load(Ordering::SeqCst));
        assert!(wd.is_expired());
    }

    #[test]
    fn feed_dopo_scadenza_restituisce_false() {
        let wd = Watchdog::new(Duration::from_millis(50), || {});
        thread::sleep(Duration::from_millis(200));
        assert!(wd.is_expired());
        assert!(!wd.feed());
    }

    #[test]
    fn callback_invocata_al_massimo_una_volta() {
        let n = Arc::new(AtomicUsize::new(0));
        let nc = Arc::clone(&n);
        let wd = Watchdog::new(Duration::from_millis(50), move || {
            nc.fetch_add(1, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(300));
        assert!(wd.is_expired());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_prima_della_scadenza_cancella_callback() {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let fc = Arc::clone(&flag);
            let _wd = Watchdog::new(Duration::from_millis(500), move || {
                fc.store(true, Ordering::SeqCst);
            });
            thread::sleep(Duration::from_millis(50));
        }
        thread::sleep(Duration::from_millis(700));
        assert!(
            !flag.load(Ordering::SeqCst),
            "la callback NON doveva essere invocata dopo il drop"
        );
    }

    #[test]
    fn drop_termina_in_tempo_breve_anche_con_timeout_lungo() {
        let t1 = Instant::now();
        {
            let _wd = Watchdog::new(Duration::from_secs(60), || {});
            thread::sleep(Duration::from_millis(20));
        }
        let el = t1.elapsed();
        assert!(
            el < Duration::from_secs(1),
            "Drop ha impiegato troppo: {:?}",
            el
        );
    }

    #[test]
    fn drop_dopo_scadenza_non_panica() {
        let _wd = Watchdog::new(Duration::from_millis(20), || {});
        thread::sleep(Duration::from_millis(150));
    }

    #[test]
    fn watchdog_non_si_riarma_dopo_la_scadenza() {
        // Una volta scaduto (callback invocata) il watchdog NON e' riutilizzabile:
        // feed() ritorna false e non riavvia il timer; is_expired resta true e la
        // callback non viene mai invocata una seconda volta. Questo test fissa la
        // non-riusabilita' implicata da feed() (nessun effetto dopo la scadenza).
        let n = Arc::new(AtomicUsize::new(0));
        let nc = Arc::clone(&n);
        let wd = Watchdog::new(Duration::from_millis(50), move || {
            nc.fetch_add(1, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(150));
        assert!(wd.is_expired());
        assert_eq!(n.load(Ordering::SeqCst), 1);

        // Tentativo di "riuso": feed deve fallire e non riarmare nulla.
        assert!(!wd.feed());
        assert!(
            wd.is_expired(),
            "il watchdog non deve tornare attivo dopo un feed post-scadenza"
        );

        // Diamo tempo a un eventuale (errato) ri-armo di far ripartire la callback.
        thread::sleep(Duration::from_millis(150));
        assert!(!wd.feed());
        assert_eq!(
            n.load(Ordering::SeqCst),
            1,
            "la callback non deve mai essere invocata una seconda volta"
        );
    }

    #[test]
    fn feed_concorrente_da_piu_thread() {
        // Piu' thread chiamano feed() in parallelo per ~300ms (timeout=150ms).
        // Una implementazione non thread-safe potrebbe corrompere lo stato
        // interno o lasciar partire la callback per via di una race; una
        // implementazione corretta mantiene il watchdog vivo per tutto il
        // periodo e lo fa scadere solo DOPO che i feed cessano.
        let flag = Arc::new(AtomicBool::new(false));
        let fc = Arc::clone(&flag);
        let wd = Arc::new(Watchdog::new(Duration::from_millis(150), move || {
            fc.store(true, Ordering::SeqCst);
        }));

        let mut handles = vec![];
        for _ in 0..4 {
            let wd = Arc::clone(&wd);
            handles.push(thread::spawn(move || {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(300) {
                    assert!(wd.feed());
                    thread::sleep(Duration::from_millis(10));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(!flag.load(Ordering::SeqCst));
        assert!(!wd.is_expired());

        // Cessati i feed, la callback deve partire entro ~150ms.
        thread::sleep(Duration::from_millis(300));
        assert!(flag.load(Ordering::SeqCst));
        assert!(wd.is_expired());
    }

    #[test]
    fn watchdog_e_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Watchdog>();
        assert_sync::<Watchdog>();
    }
}
