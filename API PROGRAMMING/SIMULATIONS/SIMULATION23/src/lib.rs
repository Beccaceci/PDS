//! # Simulazione 023 — CircuitBreaker (Resilience Design Pattern)
//!
//! Un interruttore di circuito (*Circuit Breaker*) protegge un sistema distribuito o un servizio inaffidabile
//! evitando che chiamate ripetute verso una risorsa in avaria ne sovraccarichino ulteriormente lo stato
//! o degradino le prestazioni dell'intero sistema.
//!
//! ### 🏛️ MACCHINA A STATI FINITI DEL CIRCUIT BREAKER
//!
//! ```text
//!                          [ CLOSED (Normale) ]
//!                            │              ▲
//!          `failure_threshold`              │ Prova riuscita
//!       fallimenti consecutivi              │ (Reset conteggio a 0)
//!                            │              │
//!                            ▼              │
//!                     [ OPEN (Blocco) ]     │
//!                            │              │
//!                   `cooldown` scaduto      │
//!                 (Esattamente 1 vincitore) │
//!                            │              │
//!                            ▼              │
//!                  [ HALF-OPEN (Prova) ] ───┘
//!                            │
//!                            └───► Prova fallita: Torna a OPEN e riavvia cooldown
//! ```
//!
//! ### 🛡️ REGOLE FONDAMENTALI:
//! 1. **Esecuzione Fuori dal Lock**: La chiusura utente `call()` deve SEMPRE essere eseguita all'esterno
//!    del lock, garantendo che chiamate lente non serializzino l'accesso alla macchina a stati.
//! 2. **Single-Winner Probe**: Quando il cooldown scade, **esattamente un solo thread** è autorizzato
//!    a passare in `HalfOpen` per testare la risorsa. Tutti gli altri thread concorrenti vengono
//!    respinti immediatamente con `Err(CallError::CircuitOpen)`.

pub mod lock_free_circuit_breaker;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Errore restituito quando una chiamata viene rigettata preventivamente dal circuit breaker.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CallError {
    /// Il circuito è aperto (o una prova di ripristino è già in corso): la chiamata non è stata nemmeno tentata.
    CircuitOpen,
}

/// Trait che rappresenta un interruttore di circuito (Circuit Breaker) per la protezione di chiamate inaffidabili.
pub trait CircuitBreaker<T: Send>: Clone + Send + Sync {
    /// Esegue `call` se il circuito è abilitato; aggiorna lo stato in base all'esito.
    /// Se il circuito è aperto o in prova concorrente, fallisce immediatamente con `Err(CallError::CircuitOpen)`.
    fn call(&self, call: impl FnOnce() -> Result<T, ()>) -> Result<T, CallError>;
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE STANDARD BASATA SU MUTEX (STUDENT WORKSPACE)
// =================================================================================

/// Rappresentazione esplicita degli stati del Circuit Breaker.
#[derive(Debug, Clone, Copy)]
pub enum CircuitState {
    /// Circuito chiuso: funzionamento normale.
    /// Memorizza il numero di fallimenti consecutivi accumulati fino a questo momento.
    Closed { consecutive_failures: u32 },

    /// Circuito aperto: tutte le chiamate falliscono istantaneamente.
    /// Memorizza l'istante temporale in cui il circuito è scattato per calcolare la scadenza del cooldown.
    Open { opened_at: Instant },

    /// Circuito semi-aperto: una chiamata pilota è attualmente in corso per verificare se la risorsa è guarita.
    /// Qualsiasi altra chiamata concorrente in questo stato viene respinta immediatamente.
    HalfOpen,
}

/// Struttura del Circuit Breaker thread-safe basato su `Arc<Mutex<CircuitState>>`.
pub struct MyCircuitBreaker {
    /// Stato interno condiviso protetto da Mutex.
    inner: Arc<Mutex<CircuitState>>,
    /// Numero di fallimenti consecutivi oltre il quale il circuito scatta aprendosi.
    failure_threshold: u32,
    /// Durata della finestra temporale di isolamento prima di consentire un tentativo di ripristino.
    cooldown: Duration,
}

impl<T: Send> CircuitBreaker<T> for MyCircuitBreaker {
    fn call(&self, call: impl FnOnce() -> Result<T, ()>) -> Result<T, CallError> {
        // =========================================================================
        // FASE 1: VERIFICA PREVENTIVA DELLO STATO (Sotto Lock Istantaneo)
        // =========================================================================
        let (is_probe, current_failures) = {
            let mut guard = self.inner.lock().unwrap();

            match *guard {
                // CASO 1: Circuito Chiuso (Funzionamento Normale)
                CircuitState::Closed { consecutive_failures } => (false, consecutive_failures),

                // CASO 2: Circuito Aperto (In Cooldown)
                CircuitState::Open { opened_at } => {
                    if opened_at.elapsed() >= self.cooldown {
                        // 👈 TRANSIZIONE ATOMICA A PROVA: questo thread è l'UNICO vincitore!
                        *guard = CircuitState::HalfOpen;
                        (true, 0)
                    } else {
                        // Cooldown ancora attivo: rifiuto preventivo immediato
                        return Err(CallError::CircuitOpen);
                    }
                }

                // CASO 3: Circuito Semi-Aperto (Prova già in corso da un altro thread)
                CircuitState::HalfOpen => {
                    // Un altro thread sta già testando la risorsa: rifiuto immediato
                    return Err(CallError::CircuitOpen);
                }
            }
        }; // 👈 Il lock viene rilasciato qui automaticamente prima di eseguire la chiamata!

        // =========================================================================
        // FASE 2: ESECUZIONE DELLA CHIAMATA (FUORI DAL LOCK)
        // =========================================================================
        let result = call();

        // =========================================================================
        // FASE 3: AGGIORNAMENTO DELLO STATO IN BASE ALL'ESITO (Sotto Lock)
        // =========================================================================
        let mut guard = self.inner.lock().unwrap();

        match result {
            Ok(value) => {
                // Successo: il circuito torna (o rimane) Closed e azzera i fallimenti
                *guard = CircuitState::Closed {
                    consecutive_failures: 0,
                };
                Ok(value)
            }
            Err(()) => {
                if is_probe {
                    // Il tentativo di prova è fallito: il circuito torna Open e il cooldown riparte da adesso
                    *guard = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                } else {
                    let new_failures = current_failures + 1;
                    if new_failures >= self.failure_threshold {
                        // Raggiunta la soglia di tolleranza: scatta l'apertura del circuito
                        *guard = CircuitState::Open {
                            opened_at: Instant::now(),
                        };
                    } else {
                        // Incremento progressivo dei fallimenti consecutivi
                        *guard = CircuitState::Closed {
                            consecutive_failures: new_failures,
                        };
                    }
                }
                Err(CallError::CircuitOpen)
            }
        }
    }
}

impl Clone for MyCircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            failure_threshold: self.failure_threshold,
            cooldown: self.cooldown,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `CircuitBreaker`.
pub fn make_circuit_breaker<T: Send + 'static>(
    failure_threshold: u32,
    cooldown: Duration,
) -> impl CircuitBreaker<T> {
    MyCircuitBreaker {
        inner: Arc::new(Mutex::new(CircuitState::Closed {
            consecutive_failures: 0,
        })),
        failure_threshold,
        cooldown,
    }
}
