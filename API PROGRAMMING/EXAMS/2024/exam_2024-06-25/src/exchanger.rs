use std::{ops::{Deref, DerefMut}, sync::{Arc, Condvar, Mutex}, mem::replace};


pub enum State<T: Send> {
    Empty,
    Full(T),
    Taken(T)
}

struct SharedState<T: Send> {
    data: State<T>,
    closed: bool
}
impl<T: Send> SharedState<T> {
    pub fn new () -> Self {
        Self {
            data: State::Empty,
            closed: false
        }
    }
}

pub struct Exchanger<T: Send> {
    channel: Arc<(Mutex<SharedState<T>>, Condvar)>
}
impl<T: Send> Exchanger<T> {
    pub fn new () -> Self {
        Self {
            channel: Arc::new((Mutex::new(SharedState::new()), Condvar::new()))
        }
    }

    pub fn exchange (&self, _t: T) -> Option<T> {
        let (lock, cvar) = &*self.channel;
        let mut state = lock.lock().unwrap();
        
        if state.closed {
            return None;
        }

        if matches!(state.data, State::Empty) {
            state.data = State::Full(_t);
            state = cvar.wait_while(state, |c| {
                matches!(&c.data, State::Full(_t)) && !c.closed
            }).unwrap();

            if state.closed {
                return None;
            }
            
            let State::Taken(extracted_data) = replace(&mut state.data, State::Empty) else { return None };
            Some(extracted_data)
        }
        else {
            let State::Full(extracted_data) = replace(&mut state.data, State::Taken(_t)) else { return None };
            drop(state);
            cvar.notify_one();

            Some(extracted_data)
        } 
    }
}

impl<T: Send > Drop for Exchanger<T> {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.channel;
        let mut state = lock.lock().unwrap();
        state.closed = true;
        drop(state);
        cvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    // Test 1: Comportamento in condizioni nominali
    #[test]
    fn test_exchange_success() {
        let exchanger1 = Arc::new(Exchanger::new());
        let exchanger2 = Arc::clone(&exchanger1);
        let handle = thread::spawn(move || {
            exchanger2.exchange("Dato_dal_Thread_2")
        });
        // Lasciamo al thread il tempo di bloccarsi nella wait
        thread::sleep(Duration::from_millis(100));
        let res1 = exchanger1.exchange("Dato_dal_Thread_1");
        
        // Questo test rischierà di bloccarsi all'infinito se c'è un deadlock (lost wakeup)
        let res2 = handle.join().expect("Il thread secondario ha generato un panico");
        assert_eq!(res1, Some("Dato_dal_Thread_2"), "Il Thread 1 avrebbe dovuto ricevere il dato del Thread 2");
        assert_eq!(res2, Some("Dato_dal_Thread_1"), "Il Thread 2 avrebbe dovuto ricevere il dato del Thread 1");
    }
    // Test 2: Comportamento con chiusura/distruzione
    #[test]
    fn test_exchange_closed() {
        let exchanger = Exchanger::<i32>::new();
        // Simuliamo la chiusura manuale per vedere se abortisce correttamente
        exchanger.channel.0.lock().unwrap().closed = true;
        let res = exchanger.exchange(42);
        assert_eq!(res, None, "Se il canale è chiuso, dovrebbe restituire None");
    }
}