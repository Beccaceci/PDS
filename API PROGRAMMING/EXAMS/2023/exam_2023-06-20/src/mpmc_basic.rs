use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}};

struct SharedState<E: Send> {
    queue: VecDeque<E>,
    numElements: usize,
    closed: bool
}
impl<E: Send> SharedState<E> {
    pub fn new () -> Self {
        Self {
            queue: VecDeque::new(),
            numElements: 0,
            closed: false
        }
    } 
}

struct MpMcChannel<E: Send> {
    state: Arc<(Mutex<SharedState<E>>, Condvar, Condvar)>,
    capacity: usize
}
impl<E: Send> MpMcChannel<E> {
    pub fn new (_n: usize) -> Self {
        Self {
            state: Arc::new((Mutex::new(SharedState::new()), Condvar::new(), Condvar::new())),
            capacity: _n
        }
    }

    pub fn send (&self, _element: E) -> Option<()> {
        let (lock, cvar_full, cvar_empty) = &*self.state;
        let mut state = lock.lock().unwrap();

        state = cvar_full.wait_while(state, |c| {
            c.numElements == self.capacity && !c.closed
        }).unwrap();

        match state.closed {
            true =>  {
                None
            },
            false => {
                state.queue.push_back(_element);
                state.numElements += 1;
                drop(state);
                cvar_empty.notify_one();
                Some(())
            }
        }
    }

    pub fn recv (&self) -> Option<E> {
        let (lock, cvar_full, cvar_empty) = &*self.state;
        let mut state = lock.lock().unwrap();

        state = cvar_empty.wait_while(state, |c| {
            c.numElements == 0 && !c.closed
        }).unwrap();

        if state.numElements > 0 {
            let element = state.queue.pop_front();
            state.numElements -= 1;
            drop(state);
            cvar_full.notify_one();
            element
        }
        else {
            None
        }
    }

    pub fn shutdown (&self) -> Option<()> {
        let (lock, cvar_full, cvar_empty) = &*self.state;
        let mut state = lock.lock().unwrap();

        state.closed = true;
        drop(state);
        cvar_full.notify_all();
        cvar_empty.notify_all();
        Some(())
    }
}

crate::generate_channel_tests!(crate::mpmc_basic::MpMcChannel<i32>);