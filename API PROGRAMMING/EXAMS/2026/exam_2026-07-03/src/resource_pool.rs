// =================================================================================
// ESAME PROGRAMMAZIONE DI SISTEMA - 2026-07-03
// DOMANDA DI PROGRAMMAZIONE: ResourcePool
// =================================================================================
//
// Molti servizi reali non aprono una nuova connessione ogni volta che ne hanno bisogno:
// l'apertura è lenta e onerosa. Mantengono invece un insieme fisso di connessioni già
// pronte in un pool; chi deve lavorare ne preleva una, la usa e la restituisce, affinché
// torni disponibile per qualcun altro. È lo schema dei connection pool usati in molti
// contesti, come i database o i servizi web.
//
// Si scriva in Rust una struttura che implementi il tratto generico ResourcePool<T: Send>,
// che gestisce un insieme fisso di elementi riutilizzabili di tipo T e li concede in
// prestito esclusivo ai thread che ne fanno richiesta. Questi elementi devono essere
// racchiusi da un tipo che implementi il tratto generico Resource<T: Send>, che permette
// di accedere al valore dell'elemento concesso in prestito e che li restituisce al pool
// quando il valore esce dallo scope utilizzando il concetto di RAII (tramite il tratto Drop).
//
// Requisiti:
// - La struttura deve essere thread-safe e condivisibile tra più thread.
// - Ogni elemento deve essere concesso ad al più un thread alla volta: quando l'oggetto
//   che implementa il tratto Resource<T> viene distrutto, il valore di tipo T (con il
//   proprio stato) presente al suo interno torna nel pool, diventando disponibile per
//   un'ulteriore richiesta.
// - Nessuna attesa attiva (niente busy-waiting).
// =================================================================================

use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}, time::{Duration, Instant}};

// ---------------------------------------------------------------------------------
// INTERFACCE FORNITE DAL PROFESSORE (NON MODIFICARE)
// ---------------------------------------------------------------------------------

pub trait Resource<T: Send> {
    fn get(&self) -> &T;
}

pub trait ResourcePool<T: Send> {
    /// Numero totale di elementi gestiti dal pool.
    fn capacity(&self) -> usize;

    /// Preleva un elemento dal pool e lo consegna al chiamante. 
    /// Se nessun elemento è disponibile, blocca il chiamante senza consumare cicli di CPU finché una risorsa non viene rilasciata.
    fn acquire(&self) -> impl Resource<T>;

    /// Variante con attesa limitata: come acquire, ma se non ottiene un elemento entro timeout rinuncia e restituisce None.
    /// L'attesa non deve consumare CPU.
    fn acquire_timeout(&self, timeout: Duration) -> Option<impl Resource<T>>;
}

// ---------------------------------------------------------------------------------
// SCHELETRO DELLE STRUTTURE (DA IMPLEMENTARE)
// --------------------------------------------------------------------------------- 

pub struct MyHandle<T: Send> {
    value: Option<T>,
    shared_pool: MyResourcePool<T>
}

impl<T: Send> Resource<T> for MyHandle<T> {
    fn get(&self) -> &T {
        self.value.as_ref().take().unwrap()
    }
}

impl<T: Send> Drop for MyHandle<T> {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_pool.inner;
        let mut guard = mutex.lock().unwrap();
        guard.push(self.value.take().unwrap());
        drop(guard);
        cvar.notify_one();
    }
}

pub struct MyResourcePool<T: Send> {
    inner: Arc<(Mutex<Vec<T>>, Condvar)>,
    capacity: usize
}

impl<T: Send> MyResourcePool<T> {
    pub fn with_items (_items: Vec<T>) -> Self {
        Self {
            capacity: _items.len(),
            inner: Arc::new((Mutex::new(_items), Condvar::new()))
        }
    }
}

impl<T: Send> Clone for MyResourcePool<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            capacity: self.capacity
        }
    }
}

impl<T: Send> ResourcePool<T> for MyResourcePool<T> {
    fn capacity(&self) -> usize {
        self.capacity
    }

    fn acquire(&self) -> impl Resource<T> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard = cvar.wait_while(guard, |c| {
            c.is_empty()
        }).unwrap();

        let value = guard.pop().unwrap();
        MyHandle {
            value: Some(value),
            shared_pool: self.clone()
        }
    }

    fn acquire_timeout(&self, timeout: Duration) -> Option<impl Resource<T>> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        (guard, _) = cvar.wait_timeout_while(guard, timeout, |c| {
            c.is_empty()
        }).unwrap();

        if let Some(value) = guard.pop() {
            Some(MyHandle {
                value: Some(value),
                shared_pool: self.clone()
            })
        }
        else {
            None
        }
    }
}

// ---------------------------------------------------------------------------------
// COSTRUTTORE PRINCIPALE
// ---------------------------------------------------------------------------------

pub fn make_resource_pool<T: Send>(_items: Vec<T>) -> impl ResourcePool<T> {
    MyResourcePool::with_items(_items)
}

// ---------------------------------------------------------------------------------
// CAMPAGNA DI TEST COMPLETA (TEST SUITE AD ALTA COPERTURA)
// ---------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_capacity() {
        let items = vec![1, 2, 3, 4, 5];
        let pool = make_resource_pool(items);
        assert_eq!(pool.capacity(), 5, "La capacità del pool deve corrispondere al numero di elementi iniziali");
    }

    #[test]
    fn test_acquire_and_get() {
        let items = vec!["Resource_A", "Resource_B"];
        let pool = make_resource_pool(items);
        
        let res1 = pool.acquire();
        let val = res1.get();
        assert!(*val == "Resource_A" || *val == "Resource_B");
    }

    #[test]
    fn test_raii_return_to_pool() {
        let items = vec![42];
        let pool = make_resource_pool(items);

        // Preleva la risorsa
        {
            let res = pool.acquire();
            assert_eq!(*res.get(), 42);
            // `res` viene distrutta qui (Drop) e la risorsa DEVE tornare nel pool
        }

        // Se la risorsa è tornata nel pool, questa acquire non deve bloccarsi!
        let res_again = pool.acquire();
        assert_eq!(*res_again.get(), 42);
    }

    #[test]
    fn test_state_preservation() {
        // Verifica che lo stato interno dell'elemento sia preservato quando torna nel pool
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct MutableConn {
            counter: AtomicUsize,
        }

        let items = vec![MutableConn { counter: AtomicUsize::new(0) }];
        let pool = make_resource_pool(items);

        {
            let res = pool.acquire();
            res.get().counter.fetch_add(10, Ordering::SeqCst);
        }

        // Ri-acquisiamo la stessa connessione dal pool
        let res2 = pool.acquire();
        assert_eq!(res2.get().counter.load(Ordering::SeqCst), 10, "Lo stato interno della risorsa deve essere preservato");
    }

    #[test]
    fn test_exclusive_borrow_and_blocking() {
        let items = vec![100];
        let pool = Arc::new(make_resource_pool(items));
        let barrier = Arc::new(Barrier::new(2));

        let pool_clone = Arc::clone(&pool);
        let barrier_clone = Arc::clone(&barrier);

        // Thread 1 acquisisce l'unica risorsa disponibile e attende la barriera prima di rilasciarla
        let handle1 = thread::spawn(move || {
            let res = pool_clone.acquire();
            assert_eq!(*res.get(), 100);
            barrier_clone.wait(); // Sincronizza con Thread 2
            thread::sleep(Duration::from_millis(100)); // Trattiene la risorsa per 100ms
            // `res` viene distrutta alla fine di questa closure
        });

        // Thread 2 aspetta che Thread 1 abbia acquisito la risorsa, poi chiama `acquire()`.
        // `acquire()` deve bloccarsi finché Thread 1 non rilascia la risorsa.
        let handle2 = thread::spawn(move || {
            barrier.wait(); // Si assicura che Thread 1 abbia già fatto acquire
            let start = std::time::Instant::now();
            let res = pool.acquire();
            assert_eq!(*res.get(), 100);
            assert!(start.elapsed() >= Duration::from_millis(50), "Thread 2 avrebbe dovuto bloccarsi in attesa del rilascio da parte di Thread 1");
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_acquire_timeout_success() {
        let items = vec![999];
        let pool = make_resource_pool(items);

        let res = pool.acquire_timeout(Duration::from_millis(200));
        assert!(res.is_some(), "acquire_timeout deve restituire Some se una risorsa è immediatamente disponibile");
        assert_eq!(*res.unwrap().get(), 999);
    }

    #[test]
    fn test_acquire_timeout_expiry() {
        let items = vec![777];
        let pool = Arc::new(make_resource_pool(items));

        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            let _res = pool_clone.acquire();
            thread::sleep(Duration::from_millis(200)); // Trattiene la risorsa per 200ms
        });

        // Diamo tempo a `handle` di acquisire la risorsa
        thread::sleep(Duration::from_millis(20));

        // Proviamo a fare acquire_timeout con 50ms di timeout (minore di 200ms)
        let res = pool.acquire_timeout(Duration::from_millis(50));
        assert!(res.is_none(), "acquire_timeout deve restituire None se la risorsa non torna disponibile entro il timeout");

        handle.join().unwrap();
    }

    #[test]
    fn test_concurrent_stress() {
        // Stress test concorrente con 10 thread e 3 risorse nel pool
        let num_threads = 10;
        let num_ops_per_thread = 50;
        let items = vec![1, 2, 3];
        let pool = Arc::new(make_resource_pool(items));

        let mut handles = vec![];
        for i in 0..num_threads {
            let pool_clone = Arc::clone(&pool);
            handles.push(thread::spawn(move || {
                for j in 0..num_ops_per_thread {
                    if (i + j) % 2 == 0 {
                        let res = pool_clone.acquire();
                        assert!(*res.get() >= 1 && *res.get() <= 3);
                    } else {
                        if let Some(res) = pool_clone.acquire_timeout(Duration::from_millis(10)) {
                            assert!(*res.get() >= 1 && *res.get() <= 3);
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.capacity(), 3);
    }
}