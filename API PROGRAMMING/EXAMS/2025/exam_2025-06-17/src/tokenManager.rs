use std::{sync::{Condvar, Mutex}, time::Instant};

type TokenAcquirer = dyn Fn () -> Result <(String , Instant), String > + Sync + Send;

enum SharedState {
    Empty,
    Pending,
    Valid((String, Instant))
}

pub struct TokenManager {
    state: Mutex<SharedState>,
    token_acquire: Box<TokenAcquirer>,
    cvar: Condvar
}
impl TokenManager {
    pub fn new (acquire_token: Box<TokenAcquirer>) -> Self {
        Self {
            state: Mutex::new(SharedState::Empty),
            token_acquire: acquire_token,
            cvar: Condvar::new()
        }
    }

    pub fn get_token (&self) -> Result<String, String> {
        let mut state = self.state.lock().unwrap();

        loop {
            match &*state {
                SharedState::Pending => {
                    state = self.cvar.wait_while(state, |c| {
                        matches!(*c, SharedState::Pending)
                    }).unwrap();
                },
                SharedState::Valid((token, scadenza)) => {
                    if *scadenza > Instant::now() {
                        return Ok(token.clone());
                    }
                    else {
                        *state = SharedState::Empty;
                    }
                },
                SharedState::Empty => {
                    *state = SharedState::Pending;
                    drop(state);
                    let result = (self.token_acquire)();

                    let mut state = self.state.lock().unwrap();
                    if result.is_ok() {
                        let (token, instant) =  result.unwrap();
                        *state = SharedState::Valid((token.clone(), instant));
                        self.cvar.notify_all();
                        return Ok(token);
                    }
                    else {
                        *state = SharedState::Empty;
                        self.cvar.notify_all();
                        return Err(result.unwrap_err());
                    }
                }
            }
        }
    }

    pub fn try_get_token (&self) -> Option<String> {
        let mut state = self.state.lock().unwrap();

        match &*state {
            SharedState::Valid((_token, _scadenza)) => {
                if *_scadenza > Instant::now() {
                    Some(_token.clone())
                }
                else {
                    None
                }
            },
            _ => {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

use super::*;

    #[test]
    fn a_new_manager_contains_no_token() {
        let a: Box<TokenAcquirer> = Box::new(|| Err("failure".to_string()));
        let manager = TokenManager::new(a);
        assert!(manager.try_get_token().is_none());
    }

    #[test]
    fn a_failing_acquirer_always_returns_an_error() {
        let a: Box<TokenAcquirer> = Box::new(|| Err("failure".to_string()));
        let manager = TokenManager::new(a);
        assert_eq!(manager.get_token(), Err("failure".to_string()));
        assert_eq!(manager.get_token(), Err("failure".to_string()));
    }

    #[test]
    fn a_successful_acquirer_always_returns_success() {
        let a: Box<TokenAcquirer> = Box::new(|| Ok(("success".to_string(), (Instant::now() + Duration::from_secs(3600)))));
        let manager = TokenManager::new(a);
        assert_eq!(manager.get_token(), Ok("success".to_string()));
        assert_eq!(manager.get_token(), Ok("success".to_string()));
    }

    #[test]
    fn a_slow_acquirer_causes_other_threads_to_wait() {
        use std::sync::Arc;

        let a: Box<TokenAcquirer> = Box::new(|| {
            // Simuliamo un'acquisizione lenta di 300 millisecondi
            thread::sleep(Duration::from_secs(2));
            Ok(("slow_token".to_string(), (Instant::now() + Duration::from_secs(3600))))
        });
        
        // Avvolgiamo il manager in un Arc per condividerlo tra i thread
        let manager = Arc::new(TokenManager::new(a));
        let m1 = Arc::clone(&manager);
        let m2 = Arc::clone(&manager);

        // Thread 1: Inizia l'acquisizione
        let t1 = thread::spawn(move || {
            m1.get_token()
        });

        // Thread 2: Parte poco dopo, trova lo stato in Pending e si blocca in attesa
        let t2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            m2.get_token()
        });

        // Aspettiamo che entrambi i thread finiscano e recuperiamo i risultati
        let res1 = t1.join().unwrap();
        let res2 = t2.join().unwrap();

        // Entrambi devono aver ricevuto lo stesso token, senza doppie chiamate!
        assert_eq!(res1, Ok("slow_token".to_string()));
        assert_eq!(res2, Ok("slow_token".to_string()));
    }
}