use std::{sync::{Arc, Condvar, Mutex}, thread::{self, JoinHandle}};

/// This struct acts as the "mailbox" or "ticket rail" that is shared between
/// the main thread (which submits tasks) and the worker thread (which executes them).
struct SharedState {
    /// The queue of tasks. We use a Vec of Boxed closures because each closure
    /// has a unique anonymous type, so we need dynamic dispatch (dyn) to store them together.
    /// `Send` allows the closure to be transferred to the worker thread.
    /// `'static` ensures the closure doesn't contain references to short-lived data.
    tasks: Vec<Box<dyn FnOnce() + Send + 'static>>,
    
    /// A flag to indicate whether the executor is closed and should not accept new tasks.
    closed: bool,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            closed: false,
        }
    }
}

/// A ThreadPool containing a single worker thread.
pub struct SingleThreadExecutor {
    /// The shared state (Mutex) and the bell to wake the worker (Condvar),
    /// wrapped in an Arc so ownership can be shared with the worker thread.
    state: Arc<(Mutex<SharedState>, Condvar)>,
    
    /// The handle to the worker thread, stored in an Option so we can safely
    /// extract it using `.take()` when we need to join() the thread.
    worker: Option<JoinHandle<()>>,
}

impl SingleThreadExecutor {
    pub fn new() -> Self {
        // 1. Initialize the shared state and condvar, wrapping them in an Arc.
        let shared_state = Arc::new((Mutex::new(SharedState::new()), Condvar::new()));
        
        // 2. Clone the Arc so the worker thread has its own pointer to the shared state.
        let clone = Arc::clone(&shared_state);

        // 3. Spawn the worker thread.
        let thread_handle = thread::spawn(move || {
            loop {
                // ACQUIRE LOCK: At the start of every shift, grab the lock to check the queue.
                let mut state = clone.0.lock().unwrap();
                let cvar = &clone.1;

                if state.tasks.is_empty() && state.closed {
                    // SHUTDOWN: The queue is empty and no more tasks are coming. Shift is over!
                    return;
                } else if state.tasks.is_empty() { 
                    // SLEEP: The queue is empty, but the executor is still open. 
                    // We wait for the Condvar to ring. `wait_while` automatically unlocks the Mutex 
                    // while sleeping, and re-locks it when waking up.
                    state = cvar.wait_while(state, |c| {
                        c.tasks.is_empty() && !c.closed
                    }).unwrap();
                } else {
                    // WORK: There is at least one task in the queue!
                    // We remove the oldest task (FIFO). (Using remove(0) on a Vec is O(N); 
                    // a VecDeque pop_front() would be O(1), but this works perfectly for our logic).
                    let task = state.tasks.remove(0);
                    
                    // CRITICAL: We MUST drop the lock before executing the task!
                    // If the task itself tries to call `submit()`, it would need the lock.
                    // If we held it here, the whole program would deadlock.
                    drop(state);
                    
                    // Execute the closure!
                    task();
                }
            }
        });
        
        // 4. Return the executor holding the original Arc and the worker handle.
        Self {
            state: shared_state,
            worker: Some(thread_handle),
        }
    }

    /// Submits a new task to the executor.
    pub fn submit<F>(&self, task: F) -> Result<(), &str> 
    where 
        F: FnOnce() + Send + 'static 
    {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();

        // If the executor has been closed, reject the task.
        if state.closed {
            return Err("Queue closed");
        } else {
            // Box the closure and push it onto the queue.
            state.tasks.push(Box::new(task));
            
            // WAKE UP: Ring the bell in case the worker thread is sleeping!
            cvar.notify_one();
            Ok(())
        }
    }

    /// Closes the executor, preventing new tasks from being submitted.
    pub fn close(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        
        state.closed = true;
        
        // WAKE UP: Ring the bell! If the worker is sleeping because the queue
        // was empty, it needs to wake up, see that it's closed, and terminate.
        cvar.notify_one();
    } 

    /// Waits for the worker thread to finish executing all remaining tasks and terminate.
    pub fn join(mut self) {
        // By taking `self` by value, we consume the executor.
        // We use `.take()` to extract the JoinHandle from the Option.
        if let Some(t) = self.worker.take() {
            // Wait for the thread to finish, propagating any panics with unwrap().
            t.join().unwrap();
        }
    }
}

// -----------------------------------------------------------------------------
// TEST CAMPAIGN
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_basic_execution() {
        let executor = SingleThreadExecutor::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let c1 = Arc::clone(&counter);
        executor.submit(move || {
            c1.fetch_add(1, Ordering::SeqCst);
        }).unwrap();

        let c2 = Arc::clone(&counter);
        executor.submit(move || {
            c2.fetch_add(2, Ordering::SeqCst);
        }).unwrap();

        executor.close();
        executor.join();

        // Both tasks should have completed before join() returns
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_submit_after_close_fails() {
        let executor = SingleThreadExecutor::new();
        executor.close();

        let result = executor.submit(|| {
            println!("This should never run!");
        });

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Queue closed");
        
        executor.join();
    }

    #[test]
    fn test_recursive_submission_no_deadlock() {
        // STRESS TEST: Can a task submit another task without deadlocking?
        // This explicitly tests the `drop(state)` logic inside the worker loop!
        
        // We need an executor wrapped in an Arc so the task can hold a reference to it
        let executor = Arc::new(SingleThreadExecutor::new());
        let counter = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = std::sync::mpsc::channel();
        let executor_clone = Arc::clone(&executor);
        let c1 = Arc::clone(&counter);
        
        executor.submit(move || {
            c1.fetch_add(1, Ordering::SeqCst);
            
            // Recursively submit a new task from INSIDE the worker thread!
            let c2 = Arc::clone(&c1);
            let result = executor_clone.submit(move || {
                c2.fetch_add(10, Ordering::SeqCst);
            });
            assert!(result.is_ok());
            
            // And close it from inside the worker thread!
            executor_clone.close();
            
            // Explicitly drop the clone so try_unwrap below succeeds
            drop(executor_clone);
            
            // Signal the main test thread that we are done
            tx.send(()).unwrap();
        }).unwrap();

        // Wait for the task to finish executing
        rx.recv().unwrap();

        // We can't use standard join() here because `executor` is inside an Arc, 
        // but we can extract the inner executor if it's the last reference.
        let inner_executor = Arc::try_unwrap(executor)
            .unwrap_or_else(|_| panic!("Failed to unwrap Arc"));
        inner_executor.join();

        assert_eq!(counter.load(Ordering::SeqCst), 11);
    }

    #[test]
    fn test_massive_concurrency() {
        // STRESS TEST: Submit 10,000 tasks from multiple threads simultaneously.
        let executor = Arc::new(SingleThreadExecutor::new());
        let counter = Arc::new(AtomicUsize::new(0));
        
        let mut handles = vec![];
        
        for _ in 0..10 {
            let exec_clone = Arc::clone(&executor);
            let counter_clone = Arc::clone(&counter);
            
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let c = Arc::clone(&counter_clone);
                    exec_clone.submit(move || {
                        c.fetch_add(1, Ordering::SeqCst);
                    }).unwrap();
                }
            }));
        }
        
        // Wait for all submitter threads to finish
        for h in handles {
            h.join().unwrap();
        }
        
        // Close and wait for the executor to process all 10,000 tasks
        let inner_executor = Arc::try_unwrap(executor)
            .unwrap_or_else(|_| panic!("Failed to unwrap Arc"));
        inner_executor.close();
        inner_executor.join();
        
        assert_eq!(counter.load(Ordering::SeqCst), 10000);
    }
}