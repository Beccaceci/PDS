use std::sync::{Condvar, Mutex};

pub struct ExecutionLimiter {
    state: Mutex<usize>,
    condvar: Condvar,
    limit: usize
}
impl ExecutionLimiter {
    pub fn with_capacity (usize: N) -> Self {
        Self {
            state: Mutex::new(0),
            condvar: Condvar::new(),
            limit: N
        }
    }

    pub fn execute<R> (&self, fun: fn()) -> R {

        unimplemented!();
    }
}
