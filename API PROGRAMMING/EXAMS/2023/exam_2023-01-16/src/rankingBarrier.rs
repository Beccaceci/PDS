use std::sync::{Arc, Condvar, Mutex};

struct BarrierState {
    count: usize,
    generation: usize
}
impl BarrierState {
    pub fn new () -> Self {
        Self {
            count: 0,
            generation: 0
        }
    }
}

pub struct RankingBarrier {
    inner: Arc<(Mutex<BarrierState>, Condvar)>,
    capacity: usize
}
impl RankingBarrier {
    pub fn with_capacity (_n: usize) -> Self {
        if _n < 2 {
            panic!("The capacity of the RankingBarrier must be at least equal to 2");
        }

        Self {
            inner: Arc::new((Mutex::new(BarrierState::new()), Condvar::new())),
            capacity: _n
        }
    }

    pub fn wait (&self) -> usize {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap();

        state.count += 1;
        let current_position = state.count;
        let current_generation = state.generation;

        if current_position == self.capacity {
            state.generation += 1;
            state.count = 0;
            drop(state);
            cvar.notify_all();
        }
        else {
            state = cvar.wait_while(state, |c| {
                c.generation == current_generation
            }).unwrap();
            drop(state);
        }
        
        current_position
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_1_basic_sharing() {
        // Test 1: Verifichiamo se la barriera può essere condivisa tra i thread.
        let barrier = Arc::new(RankingBarrier::with_capacity(3));
        let mut handles = vec![];

        for _ in 0..3 {
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                // Ogni thread prova ad entrare nella barriera
                let rank = b.wait(); 
                assert!(rank >= 1 && rank <= 3);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_2_cyclic_generations() {
        // Test 2: Verifichiamo se la barriera è veramente ciclica e non mescola i thread
        // di cicli diversi!
        let barrier = Arc::new(RankingBarrier::with_capacity(2));
        
        let b1 = Arc::clone(&barrier);
        let h1 = thread::spawn(move || {
            // Ciclo 1 
            let rank1 = b1.wait();
            // Ciclo 2
            let rank2 = b1.wait();
            
            (rank1, rank2)
        });

        thread::sleep(Duration::from_millis(50));

        let b2 = Arc::clone(&barrier);
        let h2 = thread::spawn(move || {
            let rank1 = b2.wait();
            let rank2 = b2.wait();
            
            (rank1, rank2)
        });

        let (r1_1, r1_2) = h1.join().unwrap();
        let (r2_1, r2_2) = h2.join().unwrap();

        assert_eq!(r1_1 + r2_1, 3, "Il primo ciclo non ha distribuito correttamente i rank");
        assert_eq!(r1_2 + r2_2, 3, "Il secondo ciclo non ha distribuito correttamente i rank");
    }
}
