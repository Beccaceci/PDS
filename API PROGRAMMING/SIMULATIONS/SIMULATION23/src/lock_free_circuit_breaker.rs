//! # Variante Architetturale: CircuitBreaker Lock-Free (Atomic CAS State Machine)
//!
//! Questa implementazione elimina completamente l'uso di `Mutex` sfruttando un singolo registro
//! atomico a 64 bit (`AtomicU64`) e transizioni di stato tramite **Compare-And-Swap (CAS)**.
//!
//! ### 📦 Codifica dello Stato Atomico a 64-bit:
//! - **Bit 63..56 (Tag di Stato)**:
//!   - `TAG_CLOSED (0)`: Circuito chiuso $\rightarrow$ bit 31..0 contengono `consecutive_failures`.
//!   - `TAG_HALF_OPEN (1)`: Circuito in prova $\rightarrow$ esattamente 1 thread ha vinto la CAS.
//!   - `TAG_OPEN (2)`: Circuito aperto $\rightarrow$ bit 55..0 contengono i millisecondi di apertura (`opened_at_ms`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{CallError, CircuitBreaker};

const TAG_CLOSED: u64 = 0;
const TAG_HALF_OPEN: u64 = 1;
const TAG_OPEN: u64 = 2;

const TAG_SHIFT: u32 = 56;
const PAYLOAD_MASK: u64 = (1 << TAG_SHIFT) - 1;

#[inline]
fn encode_closed(failures: u32) -> u64 {
    (TAG_CLOSED << TAG_SHIFT) | (failures as u64)
}

#[inline]
fn encode_half_open() -> u64 {
    TAG_HALF_OPEN << TAG_SHIFT
}

#[inline]
fn encode_open(opened_at_ms: u64) -> u64 {
    (TAG_OPEN << TAG_SHIFT) | (opened_at_ms & PAYLOAD_MASK)
}

#[inline]
fn decode(raw: u64) -> (u64, u64) {
    (raw >> TAG_SHIFT, raw & PAYLOAD_MASK)
}

/// Implementazione Lock-Free ad altissime prestazioni del Circuit Breaker.
pub struct LockFreeCircuitBreaker {
    /// Registro di stato a 64 bit compatto.
    state: Arc<AtomicU64>,
    /// Epoca di riferimento per la misurazione temporale in millisecondi.
    epoch: Instant,
    /// Soglia di fallimenti consecutivi.
    failure_threshold: u32,
    /// Durata del cooldown in millisecondi.
    cooldown_ms: u64,
}

impl LockFreeCircuitBreaker {
    /// Crea un nuovo `LockFreeCircuitBreaker`.
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: Arc::new(AtomicU64::new(encode_closed(0))),
            epoch: Instant::now(),
            failure_threshold,
            cooldown_ms: cooldown.as_millis() as u64,
        }
    }

    #[inline]
    fn current_time_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }
}

impl<T: Send> CircuitBreaker<T> for LockFreeCircuitBreaker {
    fn call(&self, call: impl FnOnce() -> Result<T, ()>) -> Result<T, CallError> {
        let now_ms = self.current_time_ms();
        let mut current_state = self.state.load(Ordering::Acquire);

        // =========================================================================
        // FASE 1: VALUTAZIONE DELLO STATO ATOMICO
        // =========================================================================
        let (is_probe, _current_failures) = loop {
            let (tag, payload) = decode(current_state);

            match tag {
                TAG_CLOSED => {
                    // Chiamata normale autorizzata
                    break (false, payload as u32);
                }
                TAG_OPEN => {
                    let opened_at_ms = payload;
                    if now_ms >= opened_at_ms + self.cooldown_ms {
                        // Cooldown scaduto: prova atomica di transizione a HalfOpen (CAS)
                        let next_state = encode_half_open();
                        match self.state.compare_exchange_weak(
                            current_state,
                            next_state,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                // 👈 VINCITORE UNICO DELLA PROVA LOCK-FREE!
                                break (true, 0);
                            }
                            Err(actual) => {
                                // Un altro thread ha vinto la CAS nel frattempo: riprova il ciclo
                                current_state = actual;
                            }
                        }
                    } else {
                        // Cooldown ancora in corso
                        return Err(CallError::CircuitOpen);
                    }
                }
                TAG_HALF_OPEN => {
                    // Prova già in corso da parte di un altro thread: rifiuto immediato
                    return Err(CallError::CircuitOpen);
                }
                _ => unreachable!(),
            }
        };

        // =========================================================================
        // FASE 2: ESECUZIONE DELLA CHIAMATA (COMPLETAMENTE LOCK-FREE)
        // =========================================================================
        let result = call();

        // =========================================================================
        // FASE 3: AGGIORNAMENTO ATOMICO DELL'ESITO VIA CAS LOOP
        // =========================================================================
        match result {
            Ok(val) => {
                // Successo: reset dello stato a Closed(0)
                let mut current = self.state.load(Ordering::Acquire);
                loop {
                    let (tag, _) = decode(current);
                    if tag == TAG_OPEN {
                        // Un altro thread ha già aperto il circuito nel frattempo: non sovrascrivere
                        break;
                    }
                    let target = encode_closed(0);
                    match self.state.compare_exchange_weak(
                        current,
                        target,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current = actual,
                    }
                }
                Ok(val)
            }
            Err(()) => {
                let now = self.current_time_ms();
                let mut current = self.state.load(Ordering::Acquire);

                loop {
                    let (tag, payload) = decode(current);

                    let target = if is_probe || (tag == TAG_CLOSED && (payload as u32 + 1) >= self.failure_threshold) {
                        // Fallimento della prova o superamento soglia -> Open
                        encode_open(now)
                    } else if tag == TAG_CLOSED {
                        // Incremento progressivo fallimenti
                        encode_closed(payload as u32 + 1)
                    } else {
                        // Già aperto da un altro thread
                        break;
                    };

                    match self.state.compare_exchange_weak(
                        current,
                        target,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(actual) => current = actual,
                    }
                }
                Err(CallError::CircuitOpen)
            }
        }
    }
}

impl Clone for LockFreeCircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            epoch: self.epoch,
            failure_threshold: self.failure_threshold,
            cooldown_ms: self.cooldown_ms,
        }
    }
}
