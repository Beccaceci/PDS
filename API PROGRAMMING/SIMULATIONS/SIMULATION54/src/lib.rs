//! # Simulazione 054 — WatchdogCoordinator
//!
//! Nei sistemi operativi affidabili, nei demoni di background (come l'integrazione watchdog di
//! `systemd` tramite `sd_notify("WATCHDOG=1")`) e nei sistemi real-time embedded, i processi critici
//! devono notificare periodicamente la propria vitalità ("heartbeat" o "pet the dog") a un coordinatore
//! centrale (Watchdog).
//!
//! Se un processo entra in deadlock, in un ciclo infinito o crasha silenziosamente, esso cessa di
//! inviare il proprio battito: allo scadere del tempo di tolleranza pattuito, il watchdog rileva l'anomalia
//! (Dead-Man's Switch) e scatena un allarme per consentire il ripristino del sistema. Se invece un processo
//! termina in modo ordinato e pulito, rilascia il proprio lease (`Drop`), venendo rimosso dal monitoraggio
//! senza generare falsi allarmi.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `WatchdogLease` e `Watchdog`.

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}, thread, time::{Duration, Instant}};

use crate::WatchdogError::{InvalidTimeout, Shutdown};

/// Errori operativi restituiti durante le transazioni del watchdog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchdogError {
    /// Il watchdog è stato arrestato definitivamente: non accetta nuove registrazioni
    /// e le chiamate di attesa si sbloccano con errore.
    Shutdown,
    /// Timeout specificato non valido (es. durata pari a zero).
    InvalidTimeout,
}

/// Tratto che rappresenta il lease di liveness detenuto da un servizio registrato.
pub trait WatchdogLease: Send {
    /// Invia un segnale di battito (heartbeat), rinnovando la scadenza di vitalità
    /// del servizio a partire dall'istante corrente per la durata pattuita.
    /// Restituisce true se il battito è stato registrato; false se il lease è già scaduto o il watchdog è arrestato.
    fn heartbeat(&self) -> bool;

    /// Restituisce true se questo lease ha mancato la propria scadenza di heartbeat.
    fn is_expired(&self) -> bool;
}

/// Tratto che rappresenta il coordinatore watchdog del sistema.
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

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

/// Stato mutabile interno di un singolo lease di monitoraggio.
/// Protetto da Mutex dedicato per consentire chiamate concorrenti a `heartbeat()`
/// senza dover acquisire il lock globale del watchdog.
pub struct LeaseState {
    /// Istante di tempo oltre il quale il servizio viene considerato in avaria/stallo.
    expiration_instant: Instant,
    /// Flag che indica se questo lease ha già innescato un allarme consumato.
    /// Evita che lo stesso servizio continui a generare allarmi a cascata.
    alarmed: bool,
}

/// Implementazione concreta dell'handle `WatchdogLease`.
///
/// Rappresenta il canale attraverso cui il processo monitorato segnala la propria
/// vitalità (heartbeat) al coordinatore centrale. All'uscita dallo scope (`Drop`),
/// deregistra automaticamente il servizio per evitare falsi allarmi.
pub struct MyLease {
    /// Identificatore univoco del lease nel registro del watchdog.
    id: usize,
    /// Nome descrittivo del servizio monitorato.
    #[allow(dead_code)]
    name: String,
    /// Intervallo massimo di tolleranza pattuito tra battiti successivi.
    timeout: Duration,
    /// Stato mutabile con scadenza e flag di allarme.
    state: Arc<Mutex<LeaseState>>,
    /// Riferimento condiviso al watchdog per risvegliare il worker e deregistrarsi al Drop.
    shared_watchdog: MyWatchDog,
}

impl WatchdogLease for MyLease {
    /// Invia un segnale di battito cardiaco ("pet the dog"), rinnovando la scadenza di vitalità.
    ///
    /// Logica:
    /// 1. Se il watchdog è chiuso, restituisce `false`.
    /// 2. Se il lease è già scaduto nel momento in cui arriva il battito, il rinnovo fallisce (`false`).
    /// 3. Se ancora attivo, estende la scadenza a `Instant::now() + timeout`, sblocca il lock
    ///    e notifica il worker del watchdog per consentirgli di ricalcolare la prossima deadline (`true`).
    fn heartbeat(&self) -> bool {
        if self.shared_watchdog.is_closed() {
            return false;
        }

        let mutex_lease = &*self.state;
        let mut guard_lease = mutex_lease.lock().unwrap();

        // Se già allarmato o scaduto rispetto all'istante attuale, non può essere rinnovato
        if guard_lease.alarmed || guard_lease.expiration_instant <= Instant::now() {
            false
        } else {
            // Estendiamo la scadenza a partire da adesso
            guard_lease.expiration_instant = Instant::now() + self.timeout;
            drop(guard_lease);

            // Svegliamo il worker del watchdog: la scadenza minima potrebbe essere cambiata
            let (_, _, cvar_worker) = &*self.shared_watchdog.inner;
            cvar_worker.notify_one();

            true
        }
    }

    /// Restituisce `true` se il servizio ha mancato la propria scadenza o se è già scattato l'allarme.
    fn is_expired(&self) -> bool {
        let guard_lease = self.state.lock().unwrap();
        guard_lease.alarmed || guard_lease.expiration_instant <= Instant::now()
    }
}

/// Gestione RAII del lease: quando il servizio termina in modo ordinato,
/// rilascia il lease e viene rimosso dal monitoraggio senza generare allarmi.
impl Drop for MyLease {
    fn drop(&mut self) {
        let (mutex, _, cvar_worker) = &*self.shared_watchdog.inner;
        let mut guard = mutex.lock().unwrap();
        // Rimuoviamo il lease dalla mappa dei servizi attivi
        guard.leases.remove(&self.id);
        drop(guard);
        // Notifichiamo il worker in modo che aggiorni la prossima scadenza minima
        cvar_worker.notify_one();
    }
}

/// Struttura descrittiva di un lease registrato nella mappa interna del watchdog.
/// Manteniamo l'`Arc` allo stato e il timeout/nome per le verifiche del worker thread.
#[derive(Clone)]
pub struct RegisteredLease {
    name: String,
    state: Arc<Mutex<LeaseState>>,
}

/// Stato globale del coordinatore watchdog, protetto da Mutex.
pub struct WatchDogState {
    /// Mappa dei lease attivi indicizzati per id univoco: id -> RegisteredLease.
    leases: HashMap<usize, RegisteredLease>,
    /// Generatore progressivo di ID univoci per le nuove registrazioni.
    next_lease_id: usize,
    /// Flag di arresto definitivo del watchdog (`shutdown`).
    closed: bool,
}

impl WatchDogState {
    pub fn new() -> Self {
        Self {
            leases: HashMap::new(),
            next_lease_id: 0,
            closed: false,
        }
    }

    /// Controlla se vi è almeno un lease che ha superato la scadenza rispetto a `now`.
    /// Se trovato, lo marca come `alarmed = true` e lo rimuove dai lease monitorati
    /// consumando l'allarme esattamente una volta per evitare ri-notifiche infinite.
    pub fn pop_expired_alarm(&mut self, now: Instant) -> Option<String> {
        let mut expired_id = None;
        let mut expired_name = None;

        for (&id, reg_lease) in self.leases.iter() {
            let mut guard_lease = reg_lease.state.lock().unwrap();
            if !guard_lease.alarmed && guard_lease.expiration_instant <= now {
                guard_lease.alarmed = true;
                expired_id = Some(id);
                expired_name = Some(reg_lease.name.clone());
                break;
            }
        }

        if let Some(id) = expired_id {
            // Rimuoviamo il lease scaduto dalla tabella attiva per non ri-notificarlo
            self.leases.remove(&id);
            expired_name
        } else {
            None
        }
    }

    /// Restituisce `true` se è presente almeno un lease attualmente scaduto.
    pub fn has_expired_lease(&self, now: Instant) -> bool {
        self.leases.iter().any(|(_, reg)| {
            let guard = reg.state.lock().unwrap();
            !guard.alarmed && guard.expiration_instant <= now
        })
    }

    /// Restituisce il conteggio dei servizi ancora attivi (non terminati e non scaduti).
    pub fn number_of_active_leases(&self) -> usize {
        let now = Instant::now();
        self.leases
            .iter()
            .filter(|(_, reg)| {
                let guard = reg.state.lock().unwrap();
                !guard.alarmed && guard.expiration_instant > now
            })
            .count()
    }
}

/// Coordinatore centrale Watchdog.
pub struct MyWatchDog {
    /// Tupla sincronizzata:
    /// - `Mutex<WatchDogState>`: tabella dei lease e stato di chiusura;
    /// - `cvar_threads`: condvar per risvegliare i thread consumatori bloccati in `wait_alarm`;
    /// - `cvar_worker`: condvar per svegliare il worker thread in caso di nuove registrazioni/heartbeat.
    inner: Arc<(Mutex<WatchDogState>, Condvar, Condvar)>,
}

impl MyWatchDog {
    /// Inizializza il coordinatore e avvia il thread di background (Timer/Worker).
    pub fn new() -> Self {
        let watchdog = Arc::new((
            Mutex::new(WatchDogState::new()),
            Condvar::new(),
            Condvar::new(),
        ));
        let cloned_watchdog = watchdog.clone();

        // Worker thread autonomo per il controllo delle scadenze (Dead-Man's Switch)
        thread::spawn(move || {
            loop {
                let (mutex, cvar_threads, cvar_worker) = &*cloned_watchdog;
                let mut guard = mutex.lock().unwrap();

                if guard.closed {
                    break;
                }

                let now = Instant::now();

                // Calcoliamo la scadenza più imminente tra tutti i lease ancora attivi
                let earliest_exp_time = guard.leases.iter().filter_map(|(_, reg)| {
                    let guard_lease = reg.state.lock().unwrap();
                    if !guard_lease.alarmed && guard_lease.expiration_instant > now {
                        Some(guard_lease.expiration_instant)
                    } else {
                        None
                    }
                }).min();

                // Se c'è già un servizio scaduto, notifichiamo subito i waiter
                if guard.has_expired_lease(now) {
                    drop(guard);
                    cvar_threads.notify_all();
                    thread::yield_now();
                    continue;
                }

                match earliest_exp_time {
                    Some(exp_time) => {
                        let dur = exp_time.saturating_duration_since(Instant::now());
                        if dur.is_zero() {
                            // Scadenza già trascorsa: svegliamo i thread consumatori
                            drop(guard);
                            cvar_threads.notify_all();
                        } else {
                            // Attesa temporizzata: si risveglia o allo scadere del timeout o se cvar_worker viene notificata
                            let (new_guard, _) = cvar_worker.wait_timeout(guard, dur).unwrap();
                            guard = new_guard;

                            if guard.closed {
                                break;
                            }

                            // Se al risveglio qualche servizio è scaduto, risvegliamo i thread in attesa di allarme
                            if guard.has_expired_lease(Instant::now()) {
                                drop(guard);
                                cvar_threads.notify_all();
                            }
                        }
                    }
                    None => {
                        // Nessun servizio attivo registrato: attesa passiva a tempo indefinito
                        guard = cvar_worker.wait(guard).unwrap();
                        if guard.closed {
                            break;
                        }
                    }
                }
            }
        });

        Self { inner: watchdog }
    }

    /// Verifica se il watchdog è stato arrestato.
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.0.lock().unwrap();
        guard.closed
    }
}

impl Watchdog for MyWatchDog {
    /// Registra un nuovo servizio rilasciando un lease di monitoraggio.
    fn register(
        &self,
        service_name: &str,
        timeout: Duration,
    ) -> Result<impl WatchdogLease + 'static, WatchdogError> {
        if timeout.is_zero() {
            return Err(InvalidTimeout);
        }

        let (mutex, _, cvar_worker) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.closed {
            Err(Shutdown)
        } else {
            let actual_lease_id = guard.next_lease_id;
            guard.next_lease_id += 1;

            let lease_state = Arc::new(Mutex::new(LeaseState {
                expiration_instant: Instant::now() + timeout,
                alarmed: false,
            }));

            // Salviamo la registrazione nella mappa globale del watchdog
            guard.leases.insert(
                actual_lease_id,
                RegisteredLease {
                    name: service_name.to_string(),
                    state: lease_state.clone(),
                },
            );

            let new_lease = MyLease {
                id: actual_lease_id,
                name: service_name.to_string(),
                timeout,
                state: lease_state,
                shared_watchdog: self.clone(),
            };

            drop(guard);
            // Svegliamo il worker thread per aggiornare la scadenza minima calcolata
            cvar_worker.notify_one();

            Ok(new_lease)
        }
    }

    /// Blocca il thread chiamante finché uno dei servizi registrati non manca il proprio heartbeat.
    fn wait_alarm(&self) -> Result<String, WatchdogError> {
        let (mutex, cvar_threads, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            if guard.closed {
                return Err(Shutdown);
            }

            // Se troviamo un servizio scaduto, consumiamo l'allarme e lo restituiamo
            if let Some(service_name) = guard.pop_expired_alarm(Instant::now()) {
                return Ok(service_name);
            }

            // Altrimenti ci sospendiamo passivamente sulla condvar dei thread
            guard = cvar_threads.wait(guard).unwrap();
        }
    }

    /// Variante con timeout di wait_alarm: attende l'eventuale scatto di un allarme
    /// fino alla durata specificata. Se nessun servizio fallisce entro tale intervallo, restituisce Ok(None).
    fn wait_alarm_timeout(&self, timeout: Duration) -> Result<Option<String>, WatchdogError> {
        let (mutex, cvar_threads, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        let deadline = Instant::now() + timeout;

        loop {
            if guard.closed {
                return Err(Shutdown);
            }

            let now = Instant::now();
            if let Some(service_name) = guard.pop_expired_alarm(now) {
                return Ok(Some(service_name));
            }

            if now >= deadline {
                return Ok(None);
            }

            let remaining = deadline.saturating_duration_since(now);
            let (new_guard, timeout_res) = cvar_threads.wait_timeout(guard, remaining).unwrap();
            guard = new_guard;

            if guard.closed {
                return Err(Shutdown);
            }

            if let Some(service_name) = guard.pop_expired_alarm(Instant::now()) {
                return Ok(Some(service_name));
            }

            if timeout_res.timed_out() {
                return Ok(None);
            }
        }
    }

    /// Restituisce il numero di servizi registrati e attualmente in vita.
    fn active_service_count(&self) -> usize {
        let (mutex, _, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.number_of_active_leases()
    }

    /// Arresta definitivamente il coordinatore watchdog.
    fn shutdown(&self) {
        let (mutex, cvar_threads, cvar_worker) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        // Risvegliamo tutti i thread host in attesa di allarmi e il worker thread
        cvar_threads.notify_all();
        cvar_worker.notify_one();
    }
}

impl Clone for MyWatchDog {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Inizializza un nuovo coordinatore watchdog di sistema.
pub fn make_watchdog() -> impl Watchdog {
    MyWatchDog::new()
}

