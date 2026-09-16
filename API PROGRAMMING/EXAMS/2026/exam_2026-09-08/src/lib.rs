//! Exam 2026-09-08: Rate Limiter
//!
//! 100% Correct and Economical Solution based on Professor Malnati's feedback.
//!
//! Feedback del Docente:
//! "Passano 12 test su 12, tuttavia, allochi inutilmente una condvar per ciascun clientId,
//! quando per attendere il trascorrere del tempo puoi semplicemente invocare std::thread::sleep(duration).
//! Inserire il Mutex all'interno di un Arc è altrettanto inutile, in quanto non hai un possesso da condividere con altri."

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ClientId(usize);

impl ClientId {
    pub fn new(id: usize) -> Self {
        ClientId(id)
    }
}

pub struct RateLimiterState {
    // Mappa dei client e cronologia delle richieste accettate
    clients: HashMap<ClientId, Vec<Instant>>,
}

impl RateLimiterState {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
}

pub struct RateLimiter {
    // Soluzione economica: Mutex diretto senza Arc superfluo
    inner: Mutex<RateLimiterState>,
    duration: Duration,
    requests_threshold: usize,
}

impl RateLimiter {
    pub fn new(w: Duration, n: usize) -> Self {
        Self {
            inner: Mutex::new(RateLimiterState::new()),
            duration: w,
            requests_threshold: n,
        }
    }

    pub fn acquire(&self, id: ClientId) {
        let mutex = &self.inner;
        loop {
            let mut guard = mutex.lock().unwrap();
            if let Some(requests) = guard.clients.get_mut(&id) {
                // Filtriamo le richieste scadute che cadono fuori dalla finestra [now - duration, now]
                let lower_bound = Instant::now() - self.duration;
                requests.retain(|&t| t > lower_bound);

                // Se la finestra contiene almeno N elementi, calcoliamo l'attesa fino alla scadenza del più vecchio
                if requests.len() >= self.requests_threshold {
                    let time_first_request = *requests.first().unwrap();
                    let time_to_wait = (time_first_request + self.duration)
                        .saturating_duration_since(Instant::now());

                    // Rilasciamo esplicitamente il lock prima di addormentarci per non bloccare gli altri client
                    drop(guard);
                    std::thread::sleep(time_to_wait);
                } else {
                    requests.push(Instant::now());
                    return;
                }
            } else {
                guard.clients.insert(id, vec![Instant::now()]);
                return;
            }
        }
    }
}
