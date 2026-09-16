# Simulazione 054 — WatchdogCoordinator

*Calibratura esatta esami 2026: complessità 6.0 punti, budget 60–75 minuti, testo compatto e concentrato. Due tratti pubblici (`WatchdogLease` e `Watchdog`), pattern a tre livelli con `Arc<(Mutex<SharedState>, Condvar)>`, gestione del ciclo di vita RAII con de-registrazione pulita su `Drop`, monitoraggio delle scadenze temporali senza attesa attiva con `Condvar::wait_timeout_while` e segnalazione tempestiva di anomalie (Dead-Man's Switch).*

---

## WatchdogCoordinator

Nei sistemi operativi affidabili, nei demoni di background (come l'integrazione watchdog di `systemd` tramite `sd_notify("WATCHDOG=1")`) e nei sistemi real-time embedded, i processi critici devono notificare periodicamente la propria vitalità ("heartbeat" o "pet the dog") a un coordinatore centrale (**Watchdog**).

Se un processo entra in deadlock, in un ciclo infinito o crasha silenziosamente, esso cessa di inviare il proprio battito: allo scadere del tempo di tolleranza pattuito, il watchdog rileva l'anomalia (Dead-Man's Switch) e scatena un allarme per consentire il ripristino del sistema. Se invece un processo termina in modo ordinato e pulito, rilascia il proprio lease (`Drop`), venendo rimosso dal monitoraggio senza generare falsi allarmi.

Si scrivano in Rust le strutture che implementano i tratti `WatchdogLease` e `Watchdog` definiti di seguito.

---

### API richiesta

```rust
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchdogError {
    /// Il watchdog è stato arrestato definitivamente: non accetta nuove registrazioni
    /// e le chiamate di attesa si sbloccano con errore.
    Shutdown,
    /// Timeout specificato non valido (es. durata pari a zero).
    InvalidTimeout,
}

pub trait WatchdogLease: Send {
    /// Invia un segnale di battito (heartbeat), rinnovando la scadenza di vitalità
    /// del servizio a partire dall'istante corrente per la durata pattuita.
    /// Restituisce true se il battito è stato registrato; false se il lease è già scaduto o il watchdog è arrestato.
    fn heartbeat(&self) -> bool;

    /// Restituisce true se questo lease ha mancato la propria scadenza di heartbeat.
    fn is_expired(&self) -> bool;
}

pub trait Watchdog: Clone + Send + Sync {
    /// Registra un nuovo servizio con il nome indicato e la durata massima di tolleranza tra heartbeat.
    /// Restituisce l'handle WatchdogLease associato.
    /// Restituisce Err(WatchdogError::Shutdown) se il watchdog è già arrestato.
    /// Restituisce Err(WatchdogError::InvalidTimeout) se timeout è Duration::ZERO.
    fn register(&self, service_name: &str, timeout: Duration) -> Result<impl WatchdogLease + 'static, WatchdogError>;

    /// Blocca il chiamante, senza consumare cicli di CPU, finché uno dei servizi registrati
    /// non manca la propria scadenza di heartbeat, restituendo il nome del servizio in allarme.
    /// Se il watchdog viene arrestato, restituisce Err(WatchdogError::Shutdown).
    fn wait_alarm(&self) -> Result<String, WatchdogError>;

    /// Variante con timeout di wait_alarm: attende l'eventuale scatto di un allarme
    /// fino alla durata specificata. Se nessun servizio fallisce entro tale intervallo,
    /// restituisce Ok(None).
    fn wait_alarm_timeout(&self, timeout: Duration) -> Result<Option<String>, WatchdogError>;

    /// Restituisce il numero di servizi attualmente registrati e attivi (non ancora terminati né scaduti).
    fn active_service_count(&self) -> usize;

    /// Arresta definitivamente il watchdog: risveglia tutti i thread attualmente bloccati
    /// con Err(WatchdogError::Shutdown) e impedisce successive registrazioni.
    fn shutdown(&self);
}

pub fn make_watchdog() -> impl Watchdog {
    ...
}
```

---

### Requisiti

- **Thread-Safety e Condivisione**:
  - Il watchdog deve essere thread-safe e condivisibile (`Clone + Send + Sync`).
- **Rinnovo della Scadenza e Heartbeat**:
  - Ciascun servizio registrato ha una propria scadenza `deadline = Instant::now() + timeout`.
  - Ogni chiamata valida a `heartbeat()` ricalcola `deadline = Instant::now() + timeout` e risveglia il coordinatore se necessario per ricalcolare la prossima scadenza imminente.
- **Rilevamento Allarmi Senza Attesa Attiva**:
  - `wait_alarm()` e `wait_alarm_timeout()` devono sospendere il chiamante su `Condvar` senza sprecare cicli di CPU.
  - Il thread in attesa deve svegliarsi esattamente quando scade il lease con la scadenza più imminente tra i servizi attivi, o tempestivamente se un servizio viene registrato, rinnovato o deregistrato.
  - Quando un servizio manca la propria scadenza, il suo nome viene emesso da `wait_alarm()`, il suo lease passa a `is_expired() == true` e non è più conteggiato tra i servizi attivi.
- **Gestione RAII (`Drop`) del Lease**:
  - Quando un handle `WatchdogLease` esce dallo scope (`Drop`), il servizio viene deregistrato in modo pulito: non potrà più generare allarmi e `active_service_count()` viene decrementato.
- **Arresto Ordinato (`shutdown`)**:
  - `shutdown()` risveglia immediatamente tutti i thread in attesa con `Err(WatchdogError::Shutdown)`.
- **Suite di Test**:
  - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
  - Se il codice consegnato non compila, non verrà valutato.

---

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con una struttura protetta da `Arc<(Mutex<SharedState>, Condvar)>`.
2. Memorizzare i servizi attivi in una mappa `HashMap<u64, ServiceEntry>` con il nome, la durata pattuita e l'istante di scadenza `Instant`.
3. In `wait_alarm()`, determinare la minima `deadline` tra i servizi attivi:
   - Se non vi sono servizi attivi, attendere indefinitamente sulla `Condvar`.
   - Se c'è una minima `deadline` futura, attendere con `cvar.wait_timeout_while` per la durata residua. Se il tempo scade senza notifiche, marcare il servizio come scaduto ed emettere l'allarme.
4. Nell'implementazione di `Drop` per la struttura che realizza `WatchdogLease`, rimuovere il proprio identificatore dallo stato condiviso e notificare la `Condvar`.
