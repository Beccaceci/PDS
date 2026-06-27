use std::{sync::{Condvar, Mutex, WaitTimeoutResult}, time::Duration};



/// Una struttura dati di sincronizzazione (CountDownLock) che permette a uno o più
/// thread di bloccarsi in attesa finché un contatore interno non scende a zero.
///
/// Nota Architetturale: L'Arc non è incluso all'interno della struttura.
/// Questa è una best practice in Rust (simile a come è progettato `Mutex`),
/// poiché delega all'utente la responsabilità di decidere come condividere 
/// la struttura (es. tramite `Arc` tra thread disconnessi, oppure tramite 
/// `&` se si usano gli scoped threads).
pub struct CountDownLock {
    // Il contatore è protetto da un Mutex per garantire l'accesso thread-safe.
    counter: Mutex<usize>,
    // Il Condvar è usato per mettere in pausa i thread senza consumare CPU.
    // Lavora sempre in tandem con il Mutex.
    cvar: Condvar
}

impl CountDownLock {
    /// Inizializza il CountDownLock con il valore di partenza `_n`.
    pub fn new (_n: usize) -> Self {
        if _n <= 0 {
            panic!("The counter must be greater than 0");
        }

        Self {
            counter: Mutex::new(_n),
            cvar: Condvar::new()
        }
    }

    /// Segnala che un'operazione è stata completata, decrementando il contatore.
    /// Se il contatore raggiunge lo zero, sveglia TUTTI i thread in attesa.
    pub fn count_down (&self) {
        // Acquisiamo il lock per proteggere la sezione critica.
        // Il MutexGuard (`c`) resterà attivo fino alla fine di questa funzione.
        let mut c = self.counter.lock().unwrap();

        if *c > 0 {
            // Modifichiamo direttamente il dato condiviso deferenziando il puntatore.
            *c -= 1;

            // Se l'operazione appena conclusa era l'ultima rimasta (il contatore
            // tocca quota 0), è il momento di avvisare tutti i thread addormentati!
            if *c == 0 {
                // notify_all() garantisce che *tutti* i thread sbloccati rivaluteranno
                // la condizione e, trovando 0, proseguiranno la loro esecuzione.
                let _ = self.cvar.notify_all();
            }
        }
    }

    /// Blocca (addormenta) il thread chiamante senza consumare cicli CPU
    /// finché il contatore non scende esattamente a 0.
    pub fn wait (&self) {
        // Acquisiamo il lock iniziale.
        let mut guard = self.counter.lock().unwrap();
        let cvar = &self.cvar;

        // La wait_while è un loop interno gestito direttamente da Rust che risolve
        // il problema degli "spurious wakeups" (svegliate casuali del SO).
        // Finché la condizione restituita dalla closure (*c > 0) è VERA, 
        // il Condvar rilascia il Mutex e mette il thread a dormire.
        // Quando il thread viene svegliato da `notify_all`, riacquisisce 
        // automaticamente il Mutex e rivaluta la condizione.
        let _guard = cvar.wait_while(guard, |c| {
            *c > 0
        }).unwrap();
    }

    /// Blocca il thread chiamante come la `wait`, ma con un limite di tempo massimo.
    /// Restituisce un `WaitTimeoutResult` che permette di capire se l'attesa è 
    /// terminata per successo o per lo scadere del timer.
    pub fn wait_timeout (&self, _d: Duration) -> WaitTimeoutResult {
        let guard = self.counter.lock().unwrap();
        let cvar = &self.cvar;

        // wait_timeout_while funziona esattamente come wait_while, ma prende
        // in input anche una Duration `_d`. Restituisce una tupla contenente:
        // 1. La guardia riacquisita alla fine del blocco
        // 2. Il risultato del timeout (che ci dice se è scaduto il tempo o no)
        let (_guard, timeout_result) = cvar.wait_timeout_while(guard, _d, |c| {
            *c > 0
        }).unwrap();

        timeout_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    // Test 1: Il metodo wait si sblocca correttamente quando il contatore arriva a zero
    #[test]
    fn test_wait_success() {
        let lock = Arc::new(CountDownLock::new(3));
        
        for i in 0..3 {
            let lock_clone = Arc::clone(&lock);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(50 * i)); // Sfalsiamo i thread
                lock_clone.count_down();
            });
        }

        // Questo thread si bloccherà fino a quando i 3 thread non chiamano count_down()
        lock.wait();
        
        // Se arriviamo qui, significa che wait() si è sbloccato correttamente
        assert_eq!(*lock.counter.lock().unwrap(), 0, "Il contatore dovrebbe essere 0");
    }

    // Test 2: Timeout scade prima che il contatore arrivi a zero
    #[test]
    fn test_wait_timeout_expires() {
        let lock = Arc::new(CountDownLock::new(2));
        
        let lock_clone = Arc::clone(&lock);
        thread::spawn(move || {
            // Un solo count_down! Il contatore rimarrà a 1
            lock_clone.count_down(); 
        });

        // Aspettiamo al massimo 100ms
        let result = lock.wait_timeout(Duration::from_millis(100));
        
        assert!(result.timed_out(), "Il timeout dovrebbe essere scaduto perché il contatore è fermo a 1");
        assert_eq!(*lock.counter.lock().unwrap(), 1, "Il contatore dovrebbe essere fermo a 1");
    }

    // Test 3: Timeout non scade perché il contatore arriva a zero in tempo
    #[test]
    fn test_wait_timeout_success() {
        let lock = Arc::new(CountDownLock::new(2));
        
        for _ in 0..2 {
            let lock_clone = Arc::clone(&lock);
            thread::spawn(move || {
                lock_clone.count_down();
            });
        }

        // Aspettiamo per ben 500ms (abbondante per assicurare il completamento)
        let result = lock.wait_timeout(Duration::from_millis(500));
        
        assert!(!result.timed_out(), "Il timeout NON dovrebbe essere scaduto");
        assert_eq!(*lock.counter.lock().unwrap(), 0, "Il contatore dovrebbe essere 0");
    }
}