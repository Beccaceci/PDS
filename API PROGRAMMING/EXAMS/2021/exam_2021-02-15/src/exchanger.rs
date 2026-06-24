use std::sync::{Condvar, Mutex};

/// Represents the different states the Exchanger can be in.
enum State<T> {
    /// No threads have arrived yet. The Exchanger is ready for a new exchange.
    Empty,
    /// The first thread has arrived, left its data, and is waiting for a partner.
    Waiting(T),
    /// The second thread has arrived, taken the first thread's data, and left its own data.
    /// The first thread must wake up, take this data, and reset the state to Empty.
    Done(T),
}

impl<T> State<T> {
    pub fn new() -> Self {
        Self::Empty
    }
}

/// A synchronization primitive that allows two threads to exchange a value.
pub struct Exchanger<T> {
    inner: Mutex<State<T>>,
    condvar: Condvar,
}

impl<T> Exchanger<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(State::new()),
            condvar: Condvar::new(),
        }
    }

    /// Blocks the current thread until another thread calls `exchange`.
    /// When two threads have called it, they return each other's data.
    pub fn exchange(&self, data: T) -> Option<T> {
        // Lock the Mutex to safely inspect and mutate the state.
        // We use unwrap() because if the mutex is poisoned, we want to panic.
        let mut state = self.inner.lock().unwrap();

        // We wrap the logic in a loop. This is crucial for handling edge cases
        // like a third thread arriving while the Exchanger is still cleaning up (State::Done).
        loop {
            match *state {
                State::Empty => {
                    // I am the FIRST thread to arrive.
                    // 1. I leave my data in the state, transitioning it to Waiting.
                    *state = State::Waiting(data);

                    // 2. I go to sleep on the condition variable.
                    // The wait_while loop protects against spurious wakeups.
                    // I will stay asleep as long as the state is STILL Waiting.
                    state = self.condvar.wait_while(state, |s| {
                        matches!(*s, State::Waiting(_))
                    }).unwrap();

                    // 3. I have woken up, which means the state is no longer Waiting.
                    // It must be Done(other_data). 
                    // I use std::mem::replace to extract the Done state, and safely 
                    // leave an Empty state behind so the Exchanger can be reused!
                    let final_state = std::mem::replace(&mut *state, State::Empty);

                    // 4. Since the state is now Empty, any queued threads (like a 3rd thread)
                    // can now proceed. I must notify them to wake up.
                    self.condvar.notify_all();

                    // 5. I extract the data left by the second thread and return it.
                    if let State::Done(other_thread_data) = final_state {
                        return Some(other_thread_data);
                    } else {
                        unreachable!("State must be Done after waiting");
                    }
                }
                State::Waiting(_) => {
                    // I am the SECOND thread to arrive.
                    // 1. I use std::mem::replace to extract the first thread's data (Waiting)
                    // and simultaneously leave my own data behind in the Done state.
                    let final_state = std::mem::replace(&mut *state, State::Done(data));

                    // 2. I notify the condition variable to wake up the first thread,
                    // which is currently sleeping in the State::Empty branch.
                    self.condvar.notify_all();

                    // 3. I extract the data left by the first thread and return it.
                    if let State::Waiting(other_thread_data) = final_state {
                        return Some(other_thread_data);
                    } else {
                        unreachable!("State must be Waiting in this branch");
                    }
                }
                State::Done(_) => {
                    // I am a THIRD thread. I arrived while the first thread was waking up
                    // but before it had a chance to reset the state to Empty.
                    // I must wait until the state is no longer Done.
                    state = self.condvar.wait_while(state, |s| {
                        matches!(*s, State::Done(_))
                    }).unwrap();

                    // Once I wake up, the loop will restart and I will evaluate
                    // the state again (which will likely be Empty).
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_exchange() {
        let exchanger = Arc::new(Exchanger::new());
        let ex1 = Arc::clone(&exchanger);
        let ex2 = Arc::clone(&exchanger);

        let t1 = thread::spawn(move || {
            ex1.exchange(10)
        });

        let t2 = thread::spawn(move || {
            ex2.exchange(20)
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert_eq!(r1, Some(20));
        assert_eq!(r2, Some(10));
    }

    #[test]
    fn test_concurrent_multiple_pairs() {
        // Test that the exchanger can handle multiple pairs of threads concurrently.
        let exchanger = Arc::new(Exchanger::new());
        let mut handles = vec![];

        // 4 threads trying to exchange values. 
        // They will pair up randomly (e.g. 0 with 2, 1 with 3, etc.)
        for i in 0..4 {
            let ex = Arc::clone(&exchanger);
            handles.push(thread::spawn(move || {
                ex.exchange(i)
            }));
        }

        let mut results = vec![];
        for handle in handles {
            results.push(handle.join().unwrap().unwrap());
        }

        results.sort();
        // Since there are 4 threads exchanging values (0, 1, 2, 3), 
        // the results should contain the exact same set of values,
        // because each value sent by one thread is received by another.
        assert_eq!(results, vec![0, 1, 2, 3]);
    }
}