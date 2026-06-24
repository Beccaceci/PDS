#![allow(dead_code)]
//! Esercizio 6 - Throttle
//!
//! Limita la frequenza globale di invocazione di una funzione: al massimo
//! `max_calls` invocazioni per ogni intervallo di durata `window`. Garantisce
//! equità FIFO tra i chiamanti.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::time::Duration;

pub struct Throttle<F, R>
where
    F: Fn() -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    _marker: PhantomData<(F, R)>,
}

impl<F, R> Throttle<F, R>
where
    F: Fn() -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    /// Crea un throttle che permetterà al massimo `max_calls` invocazioni di
    /// `f` per ogni intervallo di durata `window`. `max_calls` è un
    /// [std::num::NonZeroUsize]: un limite nullo non permetterebbe mai alcuna chiamata.
    pub fn new(_f: F, _max_calls: NonZeroUsize, _window: Duration) -> Self {
        todo!("Implementare Throttle::new")
    }

    /// Invoca la funzione incapsulata e ne restituisce il risultato. Se
    /// nell'ultimo `window` sono già avvenute `max_calls` invocazioni,
    /// attende senza CPU finché una di esse non esce dalla finestra.
    pub fn call(&self) -> R {
        todo!("Implementare Throttle::call")
    }

    /// Variante non bloccante: se l'invocazione è immediatamente possibile,
    /// la esegue e restituisce `Some(r)`; altrimenti `None`.
    pub fn try_call(&self) -> Option<R> {
        todo!("Implementare Throttle::try_call")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::thread;
    use std::time::{Duration, Instant};

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    #[test]
    fn prime_n_chiamate_immediate() {
        let n = Arc::new(AtomicUsize::new(0));
        let nc = Arc::clone(&n);
        let t = Throttle::<_, ()>::new(
            move || {
                nc.fetch_add(1, Ordering::SeqCst);
            },
            nz(3),
            Duration::from_secs(60),
        );
        let t1 = Instant::now();
        t.call();
        t.call();
        t.call();
        let el = t1.elapsed();
        assert!(el < Duration::from_millis(100), "le prime N devono essere immediate");
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn n_piu_una_attende_che_la_prima_esca_dalla_finestra() {
        let t = Throttle::<_, ()>::new(|| {}, nz(2), Duration::from_millis(200));
        let t1 = Instant::now();
        t.call();
        t.call();
        t.call();
        let el = t1.elapsed();
        assert!(
            el >= Duration::from_millis(180),
            "la terza chiamata doveva attendere ~200ms: {:?}",
            el
        );
    }

    #[test]
    fn try_call_restituisce_some_se_possibile() {
        let t = Throttle::<_, u32>::new(|| 7, nz(2), Duration::from_secs(60));
        assert_eq!(t.try_call(), Some(7));
        assert_eq!(t.try_call(), Some(7));
        assert_eq!(t.try_call(), None);
    }

    #[test]
    fn try_call_torna_disponibile_dopo_la_finestra() {
        let t = Throttle::<_, ()>::new(|| {}, nz(1), Duration::from_millis(150));
        assert_eq!(t.try_call(), Some(()));
        assert_eq!(t.try_call(), None);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(t.try_call(), Some(()));
    }

    #[test]
    fn risultato_della_funzione_viene_restituito() {
        let t = Throttle::<_, String>::new(|| "ciao".to_string(), nz(5), Duration::from_secs(60));
        assert_eq!(t.call(), "ciao");
    }

    #[test]
    fn rate_globale_rispettato_con_piu_thread() {
        let n_chiamate = 20;
        let max = 3;
        let window = Duration::from_millis(200);
        let t = Arc::new(Throttle::<_, ()>::new(|| {}, nz(max), window));

        let t1 = Instant::now();
        let mut handles = vec![];
        for _ in 0..n_chiamate {
            let t = Arc::clone(&t);
            handles.push(thread::spawn(move || t.call()));
        }
        for h in handles {
            h.join().unwrap();
        }
        let el = t1.elapsed();

        // Con max=3 ogni 200ms, 20 chiamate richiedono circa (20-3)/3 * 200ms = ~1.13s
        // Tolleranza: lower bound conservativo.
        let min_atteso = Duration::from_millis(800);
        assert!(
            el >= min_atteso,
            "rate globale violato: {} chiamate completate in {:?}",
            n_chiamate,
            el
        );
    }

    #[test]
    fn rate_non_eccessivamente_restrittivo() {
        // Lo spec richiede AL MASSIMO max_calls per finestra. Un'implementazione
        // troppo restrittiva (ad esempio "una sola chiamata per finestra anche
        // se max=5") rispetterebbe il limite superiore ma violerebbe l'aspettativa
        // pratica: il bound inferiore in `rate_globale_rispettato` non basta
        // a beccarla. Qui imponiamo un BOUND SUPERIORE stretto.
        //
        // Con max=5, window=100ms, 10 chiamate dovrebbero richiedere ~100ms
        // (le prime 5 subito, le altre 5 dopo la finestra). Un'impl che ne
        // ammette solo 1 per finestra ne impiegherebbe ~1000ms.
        let t = Arc::new(Throttle::<_, ()>::new(|| {}, nz(5), Duration::from_millis(100)));
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 0..10 {
            let t = Arc::clone(&t);
            handles.push(thread::spawn(move || t.call()));
        }
        for h in handles {
            h.join().unwrap();
        }
        let el = start.elapsed();
        assert!(
            el < Duration::from_millis(400),
            "throttle troppo restrittivo: 10 chiamate (max=5/100ms) in {:?}",
            el
        );
    }

    #[test]
    fn fairness_fifo_tra_chiamanti_in_attesa() {
        // Saturiamo il throttle, poi sblocchiamo gradualmente. I thread che
        // sono arrivati prima devono uscire prima.
        let t = Arc::new(Throttle::<_, ()>::new(|| {}, nz(1), Duration::from_millis(200)));
        // Saturazione.
        t.call();

        let exit_order = Arc::new(Mutex::new(Vec::<usize>::new()));
        let mut handles = vec![];
        for i in 0..5 {
            let t = Arc::clone(&t);
            let eo = Arc::clone(&exit_order);
            handles.push(thread::spawn(move || {
                // Distanziamo gli arrivi per stabilire un ordine ben definito.
                thread::sleep(Duration::from_millis(20 * (i + 1) as u64));
                t.call();
                eo.lock().unwrap().push(i);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let order = exit_order.lock().unwrap().clone();
        assert_eq!(
            order,
            (0..5).collect::<Vec<_>>(),
            "ordine di uscita non FIFO: {:?}",
            order
        );
    }

    #[test]
    fn window_misurata_dall_inizio_della_chiamata_non_dalla_fine() {
        // window=200ms, max=1, f() dura 300ms. Due thread chiamano in
        // contemporanea. Il primo entra a t=0 e termina a t=300. Il secondo
        // viene sbloccato non appena la finestra di 200ms dall'INIZIO della
        // prima chiamata e' trascorsa (cioe' a t=200, prima che la prima
        // chiamata finisca), e termina a t~=500. Il tempo totale deve essere
        // sensibilmente inferiore alle due chiamate sequenziali (600ms).
        let t = Arc::new(Throttle::<_, ()>::new(
            || thread::sleep(Duration::from_millis(300)),
            nz(1),
            Duration::from_millis(200),
        ));
        let start = Instant::now();
        let t1 = {
            let t = Arc::clone(&t);
            thread::spawn(move || t.call())
        };
        // Diamo al primo thread il tempo di entrare nella sezione critica.
        thread::sleep(Duration::from_millis(20));
        let t2 = {
            let t = Arc::clone(&t);
            thread::spawn(move || t.call())
        };
        t1.join().unwrap();
        t2.join().unwrap();
        let el = start.elapsed();
        assert!(
            el < Duration::from_millis(560),
            "le due chiamate dovevano sovrapporsi (la finestra parte dall'inizio): {:?}",
            el
        );
        // Lower bound: il secondo non puo' essere iniziato prima di t=200ms,
        // quindi non puo' essere finito prima di t=500ms.
        assert!(
            el >= Duration::from_millis(480),
            "il secondo thread non doveva partire prima di 200ms: {:?}",
            el
        );
    }

    #[test]
    fn try_call_non_consuma_uno_slot_se_negato() {
        let n = Arc::new(AtomicUsize::new(0));
        let nc = Arc::clone(&n);
        let t = Throttle::<_, ()>::new(
            move || {
                nc.fetch_add(1, Ordering::SeqCst);
            },
            nz(1),
            Duration::from_millis(150),
        );
        assert_eq!(t.try_call(), Some(()));
        // Negato: non deve aver incrementato di nuovo.
        assert_eq!(t.try_call(), None);
        assert_eq!(n.load(Ordering::SeqCst), 1);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(t.try_call(), Some(()));
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }
}
