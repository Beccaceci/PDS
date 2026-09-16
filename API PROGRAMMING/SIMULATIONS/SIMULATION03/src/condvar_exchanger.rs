//! # Implementazione 1: Exchanger con Condvar & Mutex (`condvar_exchanger.rs`)
//!
//! Questa implementazione utilizza le primitive di sincronizzazione standard a basso livello (`Mutex` + `Condvar`):
//! - **`ExchangeSlot<T>`**: Contiene due campi opzionali `item1` e `item2` che rappresentano i due lati dello scambio.
//! - **Gestione degli Stati del Round**:
//!   1. `item1.is_none() && item2.is_none()`: Slot vuoto, il primo thread deposita e attende il partner.
//!   2. `item1.is_some() && item2.is_none()`: Il primo thread è in attesa; il secondo deposita in `item2`, preleva `item1` e sveglia tutti.
//!   3. `item1.is_none() && item2.is_some()`: Stato transitorio in cui il primo thread deve ancora ritirare `item2`; i thread successivi attendono.
//! - **Prevenzione Deadlock**: L'attesa nel ramo `else` è limitata esclusivamente alla fase transitoria.

use super::Exchanger;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Struttura interna che rappresenta lo slot di scambio per una coppia di thread.
pub struct ExchangeSlot<T: Send> {
    item1: Option<T>,
    item2: Option<T>,
}

impl<T: Send> ExchangeSlot<T> {
    pub fn new() -> Self {
        Self {
            item1: None,
            item2: None,
        }
    }
}

/// Implementazione concreta dell'Exchanger basata su `Mutex` e `Condvar`.
pub struct CondvarExchanger<T: Send> {
    state: Arc<(Mutex<ExchangeSlot<T>>, Condvar)>,
}

impl<T: Send> CondvarExchanger<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(ExchangeSlot::new()), Condvar::new())),
        }
    }

    /// Funzione unificata che gestisce sia lo scambio bloccante sia quello con timeout.
    fn exchange_internal(&self, value: T, timeout: Option<Duration>) -> Result<T, T> {
        let (mutex, cvar) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        let deadline = timeout.map(|d| Instant::now() + d);

        loop {
            // Controllo preliminare di scadenza della deadline
            if let Some(dl) = deadline {
                if dl <= Instant::now() {
                    return Err(value);
                }
            }

            if guard.item1.is_none() && guard.item2.is_none() {
                // 1° Thread della coppia: deposita in item1 e attende il partner
                guard.item1 = Some(value);

                if let Some(dl) = deadline {
                    let remaining = dl.saturating_duration_since(Instant::now());
                    let (new_guard, _) = cvar.wait_timeout_while(guard, remaining, |c| {
                        c.item2.is_none()
                    }).unwrap();
                    guard = new_guard;

                    if let Some(returned_value) = guard.item2.take() {
                        cvar.notify_all();
                        return Ok(returned_value);
                    } else {
                        // Timeout scaduto prima dell'arrivo del partner: pulizia e recupero del valore
                        let returned_value = guard.item1.take().unwrap();
                        cvar.notify_all();
                        return Err(returned_value);
                    }
                } else {
                    guard = cvar.wait_while(guard, |c| c.item2.is_none()).unwrap();
                    let returned_value = guard.item2.take().unwrap();
                    cvar.notify_all();
                    return Ok(returned_value);
                }
            } else if guard.item1.is_some() && guard.item2.is_none() {
                // 2° Thread della coppia: deposita in item2, preleva item1 e notifica il partner
                guard.item2 = Some(value);
                let returned_value = guard.item1.take().unwrap();
                cvar.notify_all();
                return Ok(returned_value);
            } else {
                // Slot in transizione (il 1° thread sta completando il ritiro di item2)
                if let Some(dl) = deadline {
                    let remaining = dl.saturating_duration_since(Instant::now());
                    let (new_guard, _) = cvar.wait_timeout_while(guard, remaining, |c| {
                        c.item1.is_none() && c.item2.is_some()
                    }).unwrap();
                    guard = new_guard;
                } else {
                    guard = cvar.wait_while(guard, |c| {
                        c.item1.is_none() && c.item2.is_some()
                    }).unwrap();
                }
            }
        }
    }
}

impl<T: Send> Clone for CondvarExchanger<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T: Send> Exchanger<T> for CondvarExchanger<T> {
    fn exchange(&self, value: T) -> T {
        self.exchange_internal(value, None).unwrap_or_else(|v| v)
    }

    fn exchange_timeout(&self, value: T, timeout: Duration) -> Result<T, T> {
        self.exchange_internal(value, Some(timeout))
    }
}

/// Costruttore per l'Exchanger basato su Condvar e Mutex.
pub fn make_condvar_exchanger<T: Send + 'static>() -> impl Exchanger<T> {
    CondvarExchanger::new()
}
