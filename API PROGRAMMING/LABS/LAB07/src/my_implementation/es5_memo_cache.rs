#![allow(dead_code)]
//! Esercizio 5 - MemoCache
//!
//! Memoizzazione concorrente per chiave: la funzione di calcolo viene eseguita
//! una sola volta per ciascuna chiave distinta, anche se più thread la
//! richiedono concorrentemente. Chiavi diverse non si bloccano a vicenda.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct MemoCache<K, V> {
    _marker: PhantomData<(K, V)>,
}

impl<K, V> MemoCache<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> Self {
        todo!("Implementare MemoCache::new")
    }

    /// Restituisce un puntatore al valore associato a `key`. Se non esiste
    /// ancora, invoca `compute` per produrlo e lo memorizza permanentemente.
    /// Se nel frattempo un altro thread sta già calcolando per la stessa
    /// chiave, attende senza CPU il risultato di quel calcolo, senza invocare
    /// nuovamente `compute`. Chiavi distinte non si bloccano a vicenda.
    pub fn get_or_compute<F>(&self, _key: K, _compute: F) -> Arc<V>
    where
        F: FnOnce() -> V,
    {
        todo!("Implementare MemoCache::get_or_compute")
    }
}

impl<K, V> Default for MemoCache<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn primo_accesso_invoca_compute() {
        let cache = MemoCache::<String, u32>::new();
        let n = Arc::new(AtomicUsize::new(0));
        let nc = Arc::clone(&n);
        let v = cache.get_or_compute("a".to_string(), move || {
            nc.fetch_add(1, Ordering::SeqCst);
            42
        });
        assert_eq!(*v, 42);
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn accessi_successivi_non_invocano_compute() {
        let cache = MemoCache::<String, u32>::new();
        let n = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let nc = Arc::clone(&n);
            let v = cache.get_or_compute("k".to_string(), move || {
                nc.fetch_add(1, Ordering::SeqCst);
                7
            });
            assert_eq!(*v, 7);
        }
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn chiavi_diverse_vengono_calcolate_indipendentemente() {
        let cache = MemoCache::<&'static str, i32>::new();
        let va = cache.get_or_compute("a", || 1);
        let vb = cache.get_or_compute("b", || 2);
        let vc = cache.get_or_compute("c", || 3);
        assert_eq!(*va, 1);
        assert_eq!(*vb, 2);
        assert_eq!(*vc, 3);
    }

    #[test]
    fn richieste_concorrenti_stessa_chiave_calcolano_una_volta_sola() {
        let cache = Arc::new(MemoCache::<String, u32>::new());
        let n = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(10));

        let mut handles = vec![];
        for _ in 0..10 {
            let cache = Arc::clone(&cache);
            let n = Arc::clone(&n);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let nc = Arc::clone(&n);
                let v = cache.get_or_compute("hot".to_string(), move || {
                    nc.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(100));
                    123
                });
                assert_eq!(*v, 123);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            n.load(Ordering::SeqCst),
            1,
            "compute doveva essere invocata una sola volta"
        );
    }

    #[test]
    fn richieste_concorrenti_condividono_lo_stesso_arc() {
        // Oltre a invocare compute una sola volta, tutti i thread che
        // richiedono concorrentemente la stessa chiave devono ricevere
        // ESATTAMENTE lo stesso Arc (stesso puntatore), non copie distinte.
        let cache = Arc::new(MemoCache::<String, u32>::new());
        let barrier = Arc::new(Barrier::new(8));
        let results = Arc::new(Mutex::new(Vec::<Arc<u32>>::new()));

        let mut handles = vec![];
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let v = cache.get_or_compute("shared".to_string(), || {
                    thread::sleep(Duration::from_millis(50));
                    7
                });
                results.lock().unwrap().push(v);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let results = results.lock().unwrap();
        assert_eq!(results.len(), 8);
        let first = &results[0];
        for v in results.iter() {
            assert!(
                Arc::ptr_eq(first, v),
                "tutti i thread devono condividere lo stesso Arc"
            );
            assert_eq!(**v, 7);
        }
    }

    #[test]
    fn ritorna_lo_stesso_arc() {
        let cache = MemoCache::<String, u32>::new();
        let v1 = cache.get_or_compute("k".to_string(), || 1);
        let v2 = cache.get_or_compute("k".to_string(), || 999);
        assert!(Arc::ptr_eq(&v1, &v2));
    }

    #[test]
    fn chiavi_diverse_non_si_bloccano_a_vicenda() {
        // Mentre la chiave "slow" sta calcolando (sleep 300ms), la chiave
        // "fast" deve completare senza attendere.
        let cache = Arc::new(MemoCache::<&'static str, u32>::new());

        let cache_slow = Arc::clone(&cache);
        let slow = thread::spawn(move || {
            cache_slow.get_or_compute("slow", || {
                thread::sleep(Duration::from_millis(300));
                1
            })
        });

        thread::sleep(Duration::from_millis(50));
        let t1 = Instant::now();
        let v = cache.get_or_compute("fast", || 2);
        let el = t1.elapsed();
        assert_eq!(*v, 2);
        assert!(
            el < Duration::from_millis(150),
            "la chiave 'fast' non doveva attendere 'slow': {:?}",
            el
        );
        slow.join().unwrap();
    }

    #[test]
    fn stress_concorrenza_su_molte_chiavi() {
        let cache = Arc::new(MemoCache::<u32, u32>::new());
        let n_threads = 8;
        let n_keys = 50;
        let mut handles = vec![];
        for _ in 0..n_threads {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for k in 0..n_keys {
                    let v = cache.get_or_compute(k, || k * 2);
                    assert_eq!(*v, k * 2);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
