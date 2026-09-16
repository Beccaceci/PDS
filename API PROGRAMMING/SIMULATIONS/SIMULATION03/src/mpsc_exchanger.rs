//! # Implementazione 2: Exchanger basato su Canali MPSC (`mpsc_exchanger.rs`)
//!
//! Come studiato a lezione, i canali della libreria standard (`std::sync::mpsc`) consentono
//! di implementare un Rendezvous Exchanger in modo estremamente snello e robusto:
//!
//! ## Architettura dell'Implementazione:
//! 1. **Slot condiviso minimale**: `Mutex<Option<(T, mpsc::Sender<T>)>>`.
//! 2. **Rendezvous One-Shot**:
//!    - **Il 1° Thread** crea un canale `let (tx, rx) = mpsc::channel()`. Deposita il proprio valore `value`
//!      e il trasmettitore `tx` nello slot condiviso, quindi attende sul proprio ricevitore `rx.recv()`.
//!    - **Il 2° Thread** esegue un atomico `slot.take()`, estraendo in un colpo solo il valore del 1° thread
//!      e il canale `tx`. Invia il proprio valore direttamente al partner via `tx.send(value)` e ritorna.
//! 3. **Zero Deadlock & Zero Spurious Wakeups**:
//!    - Non ci sono `Condvar` né stati intermedi contesi: lo slot passa istantaneamente da `Some` a `None`.
//!    - Il 1° e 2° thread comunicano direttamente tramite il canale dedicato.
//! 4. **Gestione Nativa del Timeout**:
//!    - Il timeout viene gestito da `rx.recv_timeout(timeout)`. Se il timeout scade, il primo thread
//!      recupera il proprio valore dallo slot se non è ancora arrivato un partner.

use super::Exchanger;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

/// Stato condiviso che mantiene un eventuale primo partecipante in attesa di scambio.
struct SharedState<T> {
    slot: Mutex<Option<(T, mpsc::Sender<T>)>>,
}

/// Implementazione dell'Exchanger basata su canali standard `mpsc`.
#[derive(Clone)]
pub struct MpscExchanger<T: Send> {
    state: Arc<SharedState<T>>,
}

impl<T: Send> MpscExchanger<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(SharedState {
                slot: Mutex::new(None),
            }),
        }
    }
}

impl<T: Send + 'static> Exchanger<T> for MpscExchanger<T> {
    fn exchange(&self, value: T) -> T {
        let mut guard = self.state.slot.lock().unwrap();

        if let Some((partner_val, partner_tx)) = guard.take() {
            // Un partner è già in attesa nello slot: completiamo lo scambio
            drop(guard); // Rilasciamo il mutex prima di inviare il valore per minimizzare la contesa
            let _ = partner_tx.send(value);
            partner_val
        } else {
            // Siamo il 1° thread: creiamo una coppia (tx, rx) e depositiamo nello slot
            let (tx, rx) = mpsc::channel();
            *guard = Some((value, tx));
            drop(guard);

            // Sospensione bloccante in attesa che il 2° thread consegni il suo valore sul canale
            rx.recv().unwrap()
        }
    }

    fn exchange_timeout(&self, value: T, timeout: Duration) -> Result<T, T> {
        let mut guard = self.state.slot.lock().unwrap();

        if let Some((partner_val, partner_tx)) = guard.take() {
            // Un partner è già presente: scambio immediato senza attendere il timeout
            drop(guard);
            let _ = partner_tx.send(value);
            Ok(partner_val)
        } else {
            // Siamo il 1° thread: depositiamo e attendiamo con timeout su rx
            let (tx, rx) = mpsc::channel();
            *guard = Some((value, tx));
            drop(guard);

            match rx.recv_timeout(timeout) {
                Ok(partner_val) => Ok(partner_val),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Il timer è scaduto: verifichiamo lo slot sotto mutex per evitare race conditions
                    let mut guard = self.state.slot.lock().unwrap();
                    if let Some((my_val, _)) = guard.take() {
                        // Nessun partner è arrivato in tempo: slot ripulito e valore restituito
                        Err(my_val)
                    } else {
                        // Un partner è arrivato proprio all'istante di scadenza e ha preso lo slot:
                        // Lo scambio è stato completato, quindi recv() consegnerà il valore del partner!
                        drop(guard);
                        Ok(rx.recv().unwrap())
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    unreachable!("Il trasmettitore tx non viene mai distrutto prematuramente");
                }
            }
        }
    }
}

/// Costruttore per l'Exchanger basato su canali MPSC.
pub fn make_mpsc_exchanger<T: Send + 'static>() -> impl Exchanger<T> {
    MpscExchanger::new()
}
