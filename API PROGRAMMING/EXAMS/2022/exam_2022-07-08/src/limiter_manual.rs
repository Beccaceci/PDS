use std::sync::{Condvar, Mutex};

pub struct ExecutionLimiter {
    state: Mutex<usize>,
    condvar: Condvar,
    limit: usize
}

impl ExecutionLimiter {
    pub fn with_capacity(limit: usize) -> Self {
        Self {
            state: Mutex::new(0),
            condvar: Condvar::new(),
            limit
        }
    }

    /// Il parametro `f` è di tipo `F`, vincolato dal trait `FnOnce() -> R`.
    /// Questo permette di accettare sia puntatori a funzione semplici `fn()`,
    /// sia closure che catturano l'ambiente `|| { ... }`.
    pub fn execute<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        // 1. Acquisizione del lock e verifica del limite
        let mut count = self.state.lock().unwrap();
        // Se abbiamo raggiunto il limite N, ci addormentiamo senza consumare CPU.
        count = self.condvar.wait_while(count, |c| 
            *c >= self.limit
        ).unwrap();

        // 2. Il limite non è raggiunto: incrementiamo il contatore
        *count += 1;

        // 3. Rilasciamo il lock PRIMA di eseguire `f()`. 
        // Se non lo facessimo, nessun altro thread potrebbe mai chiamare `execute`,
        // perdendo totalmente il parallelismo!
        drop(count);

        // 4. Creiamo il Guardia RAII per la gestione del Decremento (anche in caso di Panic)
        // Definiamo una struttura "dummy" interna che ha come unico scopo 
        // l'implementazione del trait Drop.
        struct CountGuard<'a> {
            limiter: &'a ExecutionLimiter,
        }

        impl<'a> Drop for CountGuard<'a> {
            fn drop(&mut self) {
                // Quando la guardia viene distrutta (alla fine della funzione o durante un Panic),
                // ri-acquisisce il lock, decrementa il contatore e sveglia un thread in attesa!
                let mut count = self.limiter.state.lock().unwrap();
                *count -= 1;
                self.limiter.condvar.notify_one();
            }
        }

        // Instanziamo la Guardia sullo stack *prima* di lanciare la funzione
        let _guard = CountGuard { limiter: self };

        // 5. Eseguiamo la funzione. Se fa panic, inizia l'unwinding dello stack, 
        // distruggendo `_guard` e chiamando il suo `Drop`!
        f()

        // 6. Al termine della funzione (se non fa panic), la funzione ritorna R, 
        // lo scope finisce, e `_guard` viene droppata normalmente.
    }
}

// Invochiamo la macro per i test!
crate::generate_limiter_tests!(crate::limiter_manual::ExecutionLimiter);
