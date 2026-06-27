use std::{sync::{Arc, Condvar, Mutex}, thread::{self, JoinHandle}, time::{Duration, Instant}};


struct SharedState {
    queue: Vec<(Box<dyn FnOnce() + Send + 'static>, Instant)>,
    closed: bool
}
impl SharedState {
    pub fn new () -> Self {
        Self {
            queue: Vec::new(),
            closed: false
        }
    }
}

pub struct DelayedExecutor {
    state: Arc<(Mutex<SharedState>, Condvar)>,
    handle: JoinHandle<()>
}
impl DelayedExecutor {
    pub fn new () -> Self {
        let reference = Arc::new((Mutex::new(SharedState::new()), Condvar::new()));
        let cloned_reference = Arc::clone(&reference);

        let handle = thread::spawn(move || {
            loop {
                let (lock, cvar) = &*cloned_reference;
                
                loop {
                    let mut state = lock.lock().unwrap();

                    if state.closed && state.queue.len() == 0 {
                        break;
                    }

                    if let Some(index) = state.queue.iter().position(|(f, time)| *time < Instant::now()) {
                        let (function, _) = state.queue.remove(index);
                        drop(state);
                        function();
                    }
                    else {
                        if let Some(delay) = state.queue.iter().map(|(f, t)| *t - Instant::now()).min() {
                            let len = state.queue.len();
                            (state, _) = cvar.wait_timeout_while(state, delay, |c| {
                                c.queue.len() == len && !c.closed
                            }).unwrap();
                        }
                        else {
                            break;
                        }
                    }
                }
            }
        });

        
        Self {
            state: reference,
            handle: handle
        }
    }

    pub fn execute<F: FnOnce() + Send + 'static> (&self, _f: F, _delay: Duration) -> bool {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        
        if state.closed {
            drop(state);
            cvar.notify_one();
            false
        }
        else {
            state.queue.push((Box::new(_f), Instant::now() + _delay));
            drop(state);
            cvar.notify_one();
            true
        }
    }

    pub fn close (&self, drop_pending_task: bool) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.closed = true;

        if drop_pending_task {
            for index in 0..state.queue.len() {
                let _ = state.queue.remove(index);
            }
        }

        drop(state);
        cvar.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use std::thread;

    // Test 1: Verifica che execute ritorni true se aperto e false se chiuso.
    #[test]
    fn test_execute_return_values() {
        let executor = DelayedExecutor::new();
        
        let res1 = executor.execute(|| {}, Duration::from_millis(10));
        assert!(res1, "execute deve ritornare true se l'executor è aperto");

        executor.close(true);

        let res2 = executor.execute(|| {}, Duration::from_millis(10));
        assert!(!res2, "execute deve ritornare false se l'executor è chiuso");
    }

    // Test 2: Verifica che i task vengano effettivamente eseguiti asincronamente
    #[test]
    fn test_execution_delay() {
        let executor = DelayedExecutor::new();
        let flag = Arc::new(Mutex::new(false));
        
        let flag_clone = Arc::clone(&flag);
        executor.execute(move || {
            *flag_clone.lock().unwrap() = true;
        }, Duration::from_millis(200));

        // Prima del delay il task non deve essere eseguito
        thread::sleep(Duration::from_millis(100));
        assert_eq!(*flag.lock().unwrap(), false, "Task eseguito troppo presto!");

        // Dopo il delay il task deve essere stato eseguito dal background thread
        thread::sleep(Duration::from_millis(150));
        assert_eq!(*flag.lock().unwrap(), true, "Task non eseguito!");
    }

    // Test 3: Verifica che l'ordine di esecuzione rispetti le scadenze temporali
    #[test]
    fn test_execution_order() {
        let executor = DelayedExecutor::new();
        let results = Arc::new(Mutex::new(Vec::new()));

        // Inserito per primo, ma con ritardo molto lungo (300ms)
        let res_clone1 = Arc::clone(&results);
        executor.execute(move || {
            res_clone1.lock().unwrap().push(1);
        }, Duration::from_millis(300));

        // Inserito per secondo, ma con ritardo breve (100ms)
        let res_clone2 = Arc::clone(&results);
        executor.execute(move || {
            res_clone2.lock().unwrap().push(2);
        }, Duration::from_millis(100));

        // Aspettiamo che entrambi finiscano
        thread::sleep(Duration::from_millis(400));
        
        let final_results = results.lock().unwrap();
        // Il Task 2 DEVE essere eseguito prima del Task 1!
        assert_eq!(*final_results, vec![2, 1], "Ordine di esecuzione errato!");
    }

    // Test 4: close(false) deve permettere ai task già in coda di terminare
    #[test]
    fn test_close_without_drop_pending() {
        let executor = DelayedExecutor::new();
        let flag = Arc::new(Mutex::new(false));
        
        let flag_clone = Arc::clone(&flag);
        executor.execute(move || {
            *flag_clone.lock().unwrap() = true;
        }, Duration::from_millis(100));

        // Chiudiamo l'executor chiedendo di NON eliminare i pendenti
        executor.close(false);
        
        // Aspettiamo la scadenza
        thread::sleep(Duration::from_millis(200));
        assert_eq!(*flag.lock().unwrap(), true, "close(false) ha scartato i task!");
    }

    // Test 5: close(true) deve distruggere i task in coda senza eseguirli
    #[test]
    fn test_close_with_drop_pending() {
        let executor = DelayedExecutor::new();
        let flag = Arc::new(Mutex::new(false));
        
        let flag_clone = Arc::clone(&flag);
        executor.execute(move || {
            *flag_clone.lock().unwrap() = true;
        }, Duration::from_millis(100));

        // Chiudiamo l'executor chiedendo di ELIMINARE i pendenti
        executor.close(true);
        
        thread::sleep(Duration::from_millis(200));
        assert_eq!(*flag.lock().unwrap(), false, "close(true) non ha eliminato i task!");
    }
}