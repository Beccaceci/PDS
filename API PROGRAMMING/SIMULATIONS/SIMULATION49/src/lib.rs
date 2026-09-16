//! # Simulazione 049 — SupervisorTree (capstone)
//!
//! Nei sistemi concorrenti e distribuiti ad alta affidabilità (Erlang/OTP, Akka),
//! il modello ad attori delega la tolleranza ai guasti a gerarchie di supervisione
//! guidate dal principio "Let it crash".
//!
//! Ciascun attore possiede una propria mailbox limitata ed un thread di lavoro dedicato.
//! Se l'elaborazione di un messaggio fallisce, l'attore crasha e il supervisore interviene
//! applicando la strategia di riavvio configurata (`OneForOne` o `AllForOne`), prevenendo
//! cicli infiniti di crash (*flapping*) tramite finestre temporali mobili di riavvio.
//! I client bloccati in attesa sincrona (`ask`) devono essere risvegliati immediatamente
//! in caso di anomalia, e i messaggi scartati devono essere dirottati verso la Dead-Letter Queue.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `ActorRef` e `Supervisor`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::ActorError::{ActorCrashed, MailboxClosed, SupervisorTerminated};
use crate::ActorStatus::{Running, Terminated};
use crate::RestartStrategy::AllForOne;

pub type ActorId = u64;

/// Strategia di ripristino applicata dal supervisore quando un attore crasha.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStrategy {
    /// Se un attore crasha, solo quell'attore viene riavviato.
    OneForOne,
    /// Se un attore crasha, tutti gli attori appartenenti al medesimo supervisore
    /// vengono terminati e riavviati atomicamente.
    AllForOne,
}

/// Possibili errori restituiti durante l'invio o l'attesa di un messaggio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorError {
    /// L'attore è crashato durante l'elaborazione del messaggio richiesto.
    ActorCrashed,
    /// L'attore è stato terminato forzatamente a causa del fallimento di un fratello
    /// (sotto strategia AllForOne) o per superamento del budget massimo di riavvii.
    SupervisorTerminated,
    /// La mailbox dell'attore è chiusa, satura o l'attore è già terminato definitivamente.
    MailboxClosed,
}

/// Stato attuale del ciclo di vita di un attore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorStatus {
    Running,
    Restarting,
    Terminated,
}

/// Tratto che rappresenta l'handle per interagire con un attore supervisionato.
pub trait ActorRef: Send + Sync {
    /// Invia un messaggio/job all'attore e si blocca, senza consumare cicli di CPU,
    /// finché l'attore non completa l'esecuzione del messaggio e restituisce l'esito.
    ///
    /// Se il messaggio restituisce true, la chiamata ha successo e restituisce Ok(true).
    /// Se il messaggio restituisce false, l'attore crasha: il chiamante riceve Err(ActorError::ActorCrashed).
    /// Se l'attore viene abbattuto dal supervisore (es. AllForOne per colpa di un fratello)
    /// mentre questa ask era in attesa, la chiamata si sblocca immediatamente con
    /// Err(ActorError::SupervisorTerminated).
    fn ask(&self, message: impl FnOnce() -> bool + Send + 'static) -> Result<bool, ActorError>;

    /// Invia un messaggio asincrono nella mailbox dell'attore (fire-and-forget).
    /// Restituisce true se il messaggio è stato inserito con successo nella mailbox;
    /// restituisce false se la mailbox è satura o se l'attore non è nello stato Running.
    fn tell(&self, message: impl FnOnce() -> bool + Send + 'static) -> bool;

    /// Richiede l'arresto controllato di questo attore.
    /// Restituisce true se l'attore era attivo ed è stato arrestato;
    /// restituisce false se era già terminato.
    fn stop(&self) -> bool;

    /// Restituisce lo stato attuale dell'attore.
    fn status(&self) -> ActorStatus;
}

/// Tratto che rappresenta il coordinatore supervisore degli attori.
pub trait Supervisor: Clone + Send + Sync {
    /// Registra e avvia un nuovo attore all'interno di questo supervisore,
    /// allocando la relativa mailbox con la capacità indicata.
    /// Restituisce l'ActorId univoco assegnato e il rispettivo ActorRef.
    fn spawn_actor(&self, mailbox_capacity: usize) -> (ActorId, impl ActorRef + 'static);

    /// Restituisce il numero di attori attualmente registrati e non terminati definitivamente.
    fn active_actor_count(&self) -> usize;

    /// Restituisce il numero cumulativo di riavvii effettuati dal supervisore dall'avvio.
    fn total_restarts(&self) -> usize;

    /// Restituisce il numero di messaggi finiti nella Dead-Letter Queue (ovvero messaggi
    /// scartati a seguito di crash o terminazione di un attore).
    fn dead_letter_count(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyMessage {
    job: Option<Box<dyn FnOnce() -> bool + Send + 'static>>,
    reply_to: Option<Arc<(Mutex<Option<Result<bool, ActorError>>>, Condvar)>>
}

pub struct ActorState {
    messages: VecDeque<MyMessage>,
    status: ActorStatus
}

impl ActorState {
    pub fn new () -> Self {
        Self {
            messages: VecDeque::new(),
            status: Running
        }
    }
}

pub struct MyActor {
    actor_id: ActorId,
    state: Arc<(Mutex<ActorState>, Condvar, Condvar)>, // (actor_state, cvar_threads, cvar_worker)
    max_capacity: usize,
    shared_supervisor: MySupervisor
}

impl MyActor {
    pub fn new (_id: ActorId, _capacity: usize, _supervisor: &MySupervisor) -> Self {
        let state = Arc::new((Mutex::new(ActorState::new()), Condvar::new(), Condvar::new()));
        let cloned_state = state.clone();
        let cloned_supervisor = _supervisor.clone();

        thread::spawn(move || {
            loop {
                let (mutex, cvar_threads, cvar_worker) = &*cloned_state;
                let mut guard = mutex.lock().unwrap();
                guard = cvar_worker.wait_while(guard, |c| {
                    c.messages.is_empty() && !matches!(c.status, Terminated)
                }).unwrap();

                if matches!(guard.status, Terminated) {
                    guard.messages.clear();
                    break;
                }

                let mut message = guard.messages.pop_front().unwrap();
                drop(guard);

                let outcome = (message.job.take().unwrap())();

                if let Some(reply_to) = message.reply_to {
                    let (mutex_message, cvar_message) = &*reply_to;
                    let mut guard_message = mutex_message.lock().unwrap();
                    *guard_message = if outcome { Some(Ok(true)) } else { Some(Err(ActorCrashed)) };
                    drop(guard_message);
                    cvar_message.notify_all();
                }
                
                if !outcome {
                    let mut supervisor_guard = cloned_supervisor.inner.lock().unwrap();
                    supervisor_guard.failures.push(Instant::now());

                    if supervisor_guard.should_terminate(cloned_supervisor.max_restarts, cloned_supervisor.restart_window) {
                        supervisor_guard.apply_termination();
                    }
                    else {
                        supervisor_guard.total_restarts += 1;
                        if matches!(cloned_supervisor.strategy, AllForOne) {
                            supervisor_guard.apply_global_restart();
                        }
                        else {
                            supervisor_guard.apply_local_restart(_id);
                        }
                    }
                    
                }
            }
        });

        Self {
            actor_id: _id,
            state,
            max_capacity: _capacity,
            shared_supervisor: _supervisor.clone()
        }
    }

    pub fn is_active (&self) -> bool {
        let (mutex, _, _) = &*self.state;
        let guard = mutex.lock().unwrap();

        if matches!(guard.status, Running) {
            true
        }
        else {
            false
        }
    }

    pub fn clean_up_messages (&mut self) {
        let (mutex, cvar_threads, _) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        while let Some(message) = guard.messages.pop_front() {
            self.shared_supervisor.inner.lock().unwrap().num_discarded_messages += 1;
            if let Some(reply_to) = message.reply_to {
                let (mutex_message, cvar_message) = &*reply_to;
                let mut guard_message = mutex_message.lock().unwrap();
                *guard_message = Some(Err(SupervisorTerminated));
                drop(guard_message);
                cvar_message.notify_all();
            }
        }

        drop(guard);
        cvar_threads.notify_all();
    }
}

impl ActorRef for MyActor {
    fn ask(&self, message: impl FnOnce() -> bool + Send + 'static) -> Result<bool, ActorError> {
        let (mutex, cvar_threads, cvar_worker) = &*self.state;
        let mut guard = mutex.lock().unwrap();
        guard = cvar_threads.wait_while(guard, |c| {
            c.messages.len() == self.max_capacity && !matches!(c.status, Terminated)
        }).unwrap();

        if matches!(guard.status, Terminated) {
            self.shared_supervisor.inner.lock().unwrap().num_discarded_messages += 1;
            return Err(MailboxClosed);
        }

        let reply_to = Arc::new((Mutex::new(None), Condvar::new()));
        guard.messages.push_back(MyMessage {
            job: Some(Box::new(message)),
            reply_to: Some(reply_to.clone())
        });
        drop(guard);
        cvar_worker.notify_one();

        let (mutex_message, cvar_message) = &*reply_to;
        let mut guard_message = mutex_message.lock().unwrap();
        guard_message = cvar_message.wait_while(guard_message, |c| {
            c.is_none()
        }).unwrap();
        
        let final_outcome = guard_message.as_ref().unwrap();
        final_outcome.clone()
    }

    fn tell(&self, message: impl FnOnce() -> bool + Send + 'static) -> bool {
        let (mutex, _, cvar_worker) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        if guard.messages.len() == self.max_capacity || !matches!(guard.status, Running) {
            self.shared_supervisor.inner.lock().unwrap().num_discarded_messages += 1;
            false
        }
        else {
            guard.messages.push_back(MyMessage {
                job: Some(Box::new(message)),
                reply_to: None
            });
            drop(guard);
            cvar_worker.notify_one();
            true
        }
    }

    fn stop(&self) -> bool {
        let (mutex, cvar_threads, cvar_worker) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        if !matches!(guard.status, Terminated) {
            guard.status = Terminated;
            while let Some(message) = guard.messages.pop_front() {
                self.shared_supervisor.inner.lock().unwrap().num_discarded_messages += 1;
                if let Some(reply_to) = message.reply_to {
                    let (mutex_message, cvar_message) = &*reply_to;
                    let mut guard_message = mutex_message.lock().unwrap();
                    *guard_message = Some(Err(SupervisorTerminated));
                    drop(guard_message);
                    cvar_message.notify_all();
                }                
            }

            drop(guard);
            cvar_threads.notify_all();
            cvar_worker.notify_all();
            true
        }
        else {
            false
        }
    }

    fn status(&self) -> ActorStatus {
        let (mutex, _, _) = &*self.state;
        let guard = mutex.lock().unwrap();
        guard.status
    }
}

impl Clone for MyActor {
    fn clone(&self) -> Self {
        Self {
            actor_id: self.actor_id,
            state: self.state.clone(),
            max_capacity: self.max_capacity,
            shared_supervisor: self.shared_supervisor.clone()
        }
    }
}

pub struct SupervisorState {
    actors: Vec<MyActor>,
    failures: Vec<Instant>,
    num_discarded_messages: usize,
    total_restarts: usize
}

impl SupervisorState {
    pub fn new () -> Self {
        Self {
            actors: Vec::new(),
            failures: Vec::new(),
            num_discarded_messages: 0,
            total_restarts: 0
        }
    }

    pub fn should_terminate (&self, _max_restarts: usize, _restart_window: Duration) -> bool {
        let now = Instant::now();
        let num_failures = self.failures.iter().filter(|&fail| {
            now.duration_since(*fail) <= _restart_window
        }).count();
        num_failures > _max_restarts
    }

    pub fn apply_termination (&mut self) {
        for actor in self.actors.iter_mut() {
            let _ = actor.stop();
        }
    }

    pub fn apply_global_restart (&mut self) {
        for actor in self.actors.iter_mut() {
            actor.clean_up_messages();
        }
    }

    pub fn apply_local_restart (&mut self, actor_id: ActorId) {
        let actor = &mut self.actors[actor_id as usize];
        actor.clean_up_messages();
    }
}

pub struct MySupervisor {
    inner: Arc<Mutex<SupervisorState>>,
    strategy: RestartStrategy,
    max_restarts: usize,
    restart_window: Duration
}

impl Supervisor for MySupervisor {
    fn spawn_actor(&self, mailbox_capacity: usize) -> (ActorId, impl ActorRef + 'static) {
        let mut guard = self.inner.lock().unwrap();
        let actor_id = guard.actors.len() as ActorId;

        let new_actor = MyActor::new(actor_id, mailbox_capacity, &self);
        guard.actors.push(new_actor.clone());
        (actor_id, new_actor)
    }

    fn active_actor_count(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.actors.iter().filter(|&actor| actor.is_active()).count()
    }

    fn total_restarts(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.total_restarts
    }

    fn dead_letter_count(&self) -> usize {
        let guard = self.inner.lock().unwrap();
        guard.num_discarded_messages
    }
}

impl Clone for MySupervisor {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            strategy: self.strategy,
            max_restarts: self.max_restarts,
            restart_window: self.restart_window
        }
    }
}

/// Inizializza un nuovo supervisore per attori concorrenti con strategia di riavvio e budget temporale.
pub fn make_supervisor(
    _strategy: RestartStrategy,
    _max_restarts: usize,
    _restart_window: Duration,
) -> impl Supervisor {
    MySupervisor {
        inner: Arc::new(Mutex::new(SupervisorState::new())),
        strategy: _strategy,
        max_restarts: _max_restarts,
        restart_window: _restart_window
    }
}
