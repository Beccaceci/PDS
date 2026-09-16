//! # Simulazione 053 — GpuDispatcher (capstone)
//!
//! Nei moderni sistemi operativi e nelle API grafiche/computazionali a basso overhead
//! (Vulkan, DirectX 12, Metal, WebGPU), il processore centrale (CPU Host) interagisce con
//! l'acceleratore grafico/computazionale (GPU Device) attraverso code di comando hardware
//! (`GpuQueue`) multiple e concorrenti (`Graphics`, `Compute`, `Transfer`).
//!
//! Ciascuna coda esegue i propri comandi in modo asincrono rispetto alla CPU e parallelamente
//! alle altre code. Per sincronizzare le code tra loro e con la CPU senza blocchi bloccanti
//! o overhead di polling, le architetture moderne adottano i Timeline Semaphores:
//! 1. Semafori Timeline a Progressione Monotona: contatori a 64 bit strettamente non decrescenti.
//! 2. Sincronizzazione Inter-Queue Indipendente dall'Host: code consumatrici sospese passivamente
//!    finché le code produttrici non avanzano i semafori target.
//! 3. Contropressione su Ring Buffer di Memoria (VRAM): sottomissioni sospese senza attesa attiva
//!    su Condvar se la memoria del ring buffer è satura.
//! 4. Sincronizzazione Bidirezionale Host-Device: `host_wait_semaphore`, `host_signal_semaphore`
//!    e `wait_idle` per sincronizzare la CPU con i traguardi della GPU.
//! 5. Resilienza TDR (Timeout Detection and Recovery): gestione del reset di una coda o del dispositivo.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `GpuQueue` e `GpuDispatcher`.

use std::{collections::{HashMap, VecDeque}, sync::{Arc, Condvar, Mutex}, thread};

use crate::GpuError::{BatchTooLarge, DeviceLost, InvalidSemaphore, NonMonotonicTimeline};

pub type QueueId = u64;
pub type SemaphoreId = u64;
pub type BatchId = u64;

/// Tipologia di coda di esecuzione hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueType {
    Graphics,
    Compute,
    Transfer,
}

/// Errori operativi restituiti durante le transazioni del sottosistema GPU.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GpuError {
    /// La coda o il dispositivo ha subito un reset hardware (TDR / Timeout Detection and Recovery).
    DeviceLost,
    /// Il semaforo specificato non esiste.
    InvalidSemaphore,
    /// Il valore del semaforo specificato non è monotonicamente non decrescente rispetto al valore attuale.
    NonMonotonicTimeline,
    /// La dimensione del batch supera la capacità totale del ring buffer della coda.
    BatchTooLarge,
    /// Parametro o operazione non valida.
    InvalidOperation,
}

/// Descrizione di un'operazione o comando eseguito dalla GPU all'interno di un batch.
pub struct GpuCommand {
    /// Costo simulato o tempo stimato per il comando (se utile per telemetria/ordine).
    pub execution_cost_ms: u64,
    /// Azione eseguibile dal comando (eseguita dal worker thread della coda).
    pub action: Box<dyn FnOnce() + Send>,
}

impl std::fmt::Debug for GpuCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuCommand")
            .field("execution_cost_ms", &self.execution_cost_ms)
            .finish()
    }
}

/// Tratto che rappresenta una coda di esecuzione hardware (Graphics, Compute, Transfer).
pub trait GpuQueue: Send + Sync {
    /// Sottomette un batch di comandi alla coda di esecuzione.
    /// - `commands`: lista di comandi da eseguire in sequenza.
    /// - `wait_semaphores`: lista di (SemaphoreId, target_value) da attendere prima di iniziare.
    /// - `signal_semaphores`: lista di (SemaphoreId, signal_value) da impostare al termine.
    /// - `memory_bytes`: occupazione nel ring buffer richiesta da questo batch.
    ///
    /// Se il ring buffer non ha memoria sufficiente per ospitare il batch, il chiamante viene sospeso
    /// senza consumare cicli di CPU finché i batch precedenti non completano l'esecuzione e liberano memoria.
    /// Restituisce il BatchId univoco assegnato al batch.
    fn submit(
        &self,
        commands: Vec<GpuCommand>,
        wait_semaphores: &[(SemaphoreId, u64)],
        signal_semaphores: &[(SemaphoreId, u64)],
        memory_bytes: usize,
    ) -> Result<BatchId, GpuError>;

    /// Restituisce l'ID univoco di questa coda.
    fn queue_id(&self) -> QueueId;

    /// Restituisce la tipologia di questa coda (Graphics, Compute, Transfer).
    fn queue_type(&self) -> QueueType;

    /// Restituisce il numero di batch attualmente in attesa o in esecuzione in questa coda.
    fn pending_batches_count(&self) -> usize;

    /// Restituisce la memoria attualmente occupata nel ring buffer di questa coda.
    fn allocated_ring_bytes(&self) -> usize;
}

/// Tratto che rappresenta il coordinatore centrale del dispositivo GPU e dei semafori timeline.
pub trait GpuDispatcher: Clone + Send + Sync {
    /// Alloca e attiva una nuova coda di esecuzione del tipo specificato con la capacità di ring buffer indicata.
    /// Restituisce l'ID univoco assegnato e il rispettivo handle GpuQueue.
    fn create_queue(&self, queue_type: QueueType, ring_capacity_bytes: usize) -> (QueueId, impl GpuQueue + 'static);

    /// Crea un nuovo semaforo timeline inizializzato con il valore specificato.
    /// Restituisce l'ID univoco assegnato al semaforo.
    fn create_semaphore(&self, initial_value: u64) -> SemaphoreId;

    /// Restituisce il valore di timeline attualmente raggiunto dal semaforo specificato.
    fn read_semaphore(&self, sem: SemaphoreId) -> Result<u64, GpuError>;

    /// Avanza manualmente dal thread host della CPU il valore del semaforo timeline specificato.
    /// Il valore deve essere strettamente maggiore del valore attuale del semaforo, altrimenti
    /// restituisce Err(GpuError::NonMonotonicTimeline).
    /// Risveglia tutti i thread host e le code GPU in attesa di tale valore.
    fn host_signal_semaphore(&self, sem: SemaphoreId, new_value: u64) -> Result<(), GpuError>;

    /// Blocca il thread chiamante (CPU host), senza consumare cicli di CPU, finché il semaforo
    /// specificato non raggiunge un valore maggiore o uguale a `target_value`.
    /// Se il dispositivo o la coda subisce un reset che impedisce il raggiungimento del valore,
    /// restituisce Err(GpuError::DeviceLost).
    fn host_wait_semaphore(&self, sem: SemaphoreId, target_value: u64) -> Result<(), GpuError>;

    /// Blocca il thread chiamante, senza consumare cicli di CPU, finché tutte le code
    /// non hanno completato l'esecuzione di tutti i batch sottomessi.
    fn wait_idle(&self);

    /// Simula un reset hardware (TDR) sulla coda indicata:
    /// tutti i batch in attesa o in esecuzione vengono abortiti con DeviceLost, la memoria
    /// del ring buffer viene azzerata e le attese su semafori sbloccate con errore.
    fn reset_queue(&self, queue: QueueId) -> Result<(), GpuError>;

    /// Restituisce il numero di code attualmente attive.
    fn active_queue_count(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

/// Rappresentazione interna di una sottomissione di lavoro (Batch) per la GPU.
/// Incapsula i comandi da eseguire in sequenza, i semafori di pre-condizione (wait)
/// e i semafori di post-condizione (signal), oltre alla memoria VRAM occupata.
pub struct MyBatch {
    /// ID univoco assegnato al batch.
    id: BatchId,
    /// Quantitativo di byte occupato all'interno del ring buffer circolare della coda.
    memory_used: usize,
    /// Comandi sequenziali da processare.
    commands: Vec<GpuCommand>,
    /// Dipendenze inter-coda / host: (SemaphoreId, target_value).
    wait_semaphores: Vec<(SemaphoreId, u64)>,
    /// Avanzamenti timeline da emettere al completamento: (SemaphoreId, signal_value).
    signal_semaphores: Vec<(SemaphoreId, u64)>,
}

/// Stato interno condiviso e protetto da Mutex per ciascuna coda GPU (`MyQueue`).
pub struct QueueState {
    /// Memoria attualmente occupata dai batch in attesa o in esecuzione nel Ring Buffer.
    memory_used: usize,
    /// Generatore sequenziale monotono per i BatchId.
    next_batch_id: BatchId,
    /// Flag che indica se il worker thread sta correntemente processando un batch.
    is_running: bool,
    /// Coda FIFO dei batch sottomessi e in attesa di essere consumati dal worker.
    batches: VecDeque<MyBatch>,
    /// Flag di reset hardware / TDR (Timeout Detection and Recovery).
    /// Se impostato a `true`, la coda rifiuta ogni nuova `submit` con `Err(GpuError::DeviceLost)`.
    is_reset: bool,
    /// Flag per la terminazione cooperativa del thread worker quando la coda viene deallocata (RAII).
    is_shutdown: bool,
}

/// Implementazione concreta della coda hardware GPU (`GpuQueue`).
/// Gestisce la sottomissione dei comandi, la contropressione di memoria sul ring buffer
/// e l'esecuzione asincrona tramite un thread worker dedicato.
#[derive(Clone)]
pub struct MyQueue {
    id: QueueId,
    queue_type: QueueType,
    capacity_memory_bytes: usize,
    state: Arc<(Mutex<QueueState>, Condvar)>,
    shared_dispatcher: MyGpuDispatcher,
}

impl GpuQueue for MyQueue {
    /// Sottomette un batch alla coda.
    ///
    /// Logica implementativa:
    /// 1. Verifica immediata: se la memoria richiesta supera la capacità massima della coda,
    ///    restituisce `Err(BatchTooLarge)`.
    /// 2. Lock sullo stato della coda: se la coda ha subito un reset TDR, restituisce `Err(DeviceLost)`.
    /// 3. Backpressure: se non c'è memoria libera sufficiente nel ring buffer, il thread chiamante
    ///    si sospende passivamente sulla `Condvar` finché l'esecuzione dei batch precedenti non libera byte.
    /// 4. Accodamento FIFO (`push_back`), aggiornamento contatori e notifica del worker thread.
    fn submit(
        &self,
        commands: Vec<GpuCommand>,
        wait_semaphores: &[(SemaphoreId, u64)],
        signal_semaphores: &[(SemaphoreId, u64)],
        memory_bytes: usize,
    ) -> Result<BatchId, GpuError> {
        if memory_bytes > self.capacity_memory_bytes {
            return Err(BatchTooLarge);
        }

        let (mutex_queue, cvar_queue) = &*self.state;
        let mut guard_queue = mutex_queue.lock().unwrap();

        // Controllo se la coda è già stata invalidata da un TDR
        if guard_queue.is_reset {
            return Err(DeviceLost);
        }

        // Sospensione passiva se il ring buffer è saturo (Contropressione / Backpressure)
        guard_queue = cvar_queue
            .wait_while(guard_queue, |c| {
                !c.is_reset && (self.capacity_memory_bytes - c.memory_used < memory_bytes)
            })
            .unwrap();

        // Se durante l'attesa si è verificato un TDR, abortiamo la sottomissione
        if guard_queue.is_reset {
            return Err(DeviceLost);
        }

        let batch_id = guard_queue.next_batch_id;
        guard_queue.next_batch_id += 1;
        guard_queue.memory_used += memory_bytes;

        // Inserimento rigorosamente in ordine FIFO (push_back per estrarre con pop_front)
        guard_queue.batches.push_back(MyBatch {
            id: batch_id,
            memory_used: memory_bytes,
            commands,
            wait_semaphores: wait_semaphores.to_vec(),
            signal_semaphores: signal_semaphores.to_vec(),
        });

        drop(guard_queue);
        // Notifichiamo il worker thread che è disponibile un nuovo batch da eseguire
        cvar_queue.notify_all();

        Ok(batch_id)
    }

    fn queue_id(&self) -> QueueId {
        self.id
    }

    fn queue_type(&self) -> QueueType {
        self.queue_type
    }

    /// Restituisce il numero totale di batch attualmente pendenti (in coda FIFO o correntemente in esecuzione).
    fn pending_batches_count(&self) -> usize {
        let (mutex_queue, _) = &*self.state;
        let guard_queue = mutex_queue.lock().unwrap();
        guard_queue.batches.len() + if guard_queue.is_running { 1 } else { 0 }
    }

    /// Restituisce i byte di memoria del Ring Buffer correntemente allocati da batch non ancora conclusi.
    fn allocated_ring_bytes(&self) -> usize {
        let (mutex_queue, _) = &*self.state;
        let guard_queue = mutex_queue.lock().unwrap();
        guard_queue.memory_used
    }
}

impl MyQueue {
    /// Inizializza una nuova coda GPU e lancia il thread worker dedicato.
    pub fn new(
        id: QueueId,
        queue_type: QueueType,
        available_memory: usize,
        shared_dispatcher: &MyGpuDispatcher,
    ) -> Self {
        let new_queue = MyQueue {
            id,
            queue_type,
            capacity_memory_bytes: available_memory,
            state: Arc::new((
                Mutex::new(QueueState {
                    memory_used: 0,
                    is_running: false,
                    next_batch_id: 0,
                    batches: VecDeque::new(),
                    is_reset: false,
                    is_shutdown: false,
                }),
                Condvar::new(),
            )),
            shared_dispatcher: shared_dispatcher.clone(),
        };

        let cloned_queue = new_queue.clone();

        // Worker thread dedicato che simula il processore di comandi hardware della GPU
        thread::spawn(move || {
            loop {
                let (mutex_queue, cvar_queue) = &*cloned_queue.state;
                let mut guard_queue = mutex_queue.lock().unwrap();

                // Attesa passiva finché la coda non riceve un batch, viene spenta (shutdown) o resettata (TDR)
                guard_queue = cvar_queue
                    .wait_while(guard_queue, |c| {
                        !c.is_shutdown && !c.is_reset && c.batches.is_empty()
                    })
                    .unwrap();

                // Terminazione pulita se la coda è stata deallocata
                if guard_queue.is_shutdown {
                    break;
                }

                // Se la coda è in stato di reset, attendiamo finché non viene sbloccata o spenta
                if guard_queue.is_reset {
                    if guard_queue.batches.is_empty() {
                        guard_queue = cvar_queue
                            .wait_while(guard_queue, |c| c.is_reset && !c.is_shutdown)
                            .unwrap();
                        if guard_queue.is_shutdown {
                            break;
                        }
                    }
                }

                // Estrazione in ordine FIFO del prossimo batch
                if let Some(batch) = guard_queue.batches.pop_front() {
                    guard_queue.is_running = true;
                    drop(guard_queue);

                    let mut computation_error = false;

                    // -------------------------------------------------------------
                    // FASE 1: ATTESA DELLE DIPENDENZE (WAIT SEMAPHORES)
                    // Il worker attende sequenzialmente che tutti i semafori target
                    // siano stati avanzati a un valore >= target_value.
                    // -------------------------------------------------------------
                    for (sem_id, target_value) in batch.wait_semaphores {
                        let outcome = cloned_queue
                            .shared_dispatcher
                            .host_wait_semaphore(sem_id, target_value);
                        if outcome.is_err() {
                            computation_error = true;
                            break;
                        }
                    }

                    // Verifica che la coda non sia stata resettata durante l'attesa dei semafori
                    {
                        let guard_check = cloned_queue.state.0.lock().unwrap();
                        if guard_check.is_reset {
                            computation_error = true;
                        }
                    }

                    // -------------------------------------------------------------
                    // FASE 2: ESECUZIONE EFFETTIVA DEI COMANDI SULLA GPU
                    // Se le dipendenze sono state soddisfatte, eseguiamo le azioni
                    // -------------------------------------------------------------
                    if !computation_error {
                        for command in batch.commands {
                            (command.action)();
                        }

                        // ---------------------------------------------------------
                        // FASE 3: SEGNALAZIONE DEI SEMAFORI DI COMPLETAMENTO
                        // Notifichiamo l'avanzamento dei semafori timeline
                        // ---------------------------------------------------------
                        for (sem_id, target_value) in batch.signal_semaphores {
                            let _ = cloned_queue
                                .shared_dispatcher
                                .host_signal_semaphore(sem_id, target_value);
                        }
                    }

                    // -------------------------------------------------------------
                    // FASE 4: BONIFICA RISORSE (RING BUFFER RECLAMATION)
                    // Rilasciamo la memoria VRAM e notifichiamo eventuali produttori
                    // bloccati in submit(), oltre a risvegliare wait_idle().
                    // -------------------------------------------------------------
                    let (mutex_queue, cvar_queue) = &*cloned_queue.state;
                    let mut guard_queue = mutex_queue.lock().unwrap();
                    guard_queue.memory_used = guard_queue
                        .memory_used
                        .saturating_sub(batch.memory_used);
                    guard_queue.is_running = false;
                    drop(guard_queue);

                    // Svegliamo i thread in attesa di memoria nella coda
                    cvar_queue.notify_all();
                    // Svegliamo eventuali thread in attesa sul dispatcher (es. wait_idle)
                    cloned_queue.shared_dispatcher.inner.1.notify_all();
                }
            }
        });

        new_queue
    }

    /// Restituisce true se la coda è completamente vuota e inattiva (nessun batch in coda né in esecuzione).
    pub fn is_idle(&self) -> bool {
        let (mutex_queue, _) = &*self.state;
        let guard_queue = mutex_queue.lock().unwrap();
        guard_queue.batches.is_empty() && !guard_queue.is_running
    }
}

/// Wrapper RAII attorno a `MyQueue` che garantisce la deregistrazione automatica
/// della coda dal dispatcher e la terminazione del relativo worker thread quando
/// l'handle esce dallo scope (`test_raii_drop_queue_decrements_count`).
pub struct QueueHandle {
    queue: MyQueue,
}

impl GpuQueue for QueueHandle {
    fn submit(
        &self,
        commands: Vec<GpuCommand>,
        wait_semaphores: &[(SemaphoreId, u64)],
        signal_semaphores: &[(SemaphoreId, u64)],
        memory_bytes: usize,
    ) -> Result<BatchId, GpuError> {
        self.queue
            .submit(commands, wait_semaphores, signal_semaphores, memory_bytes)
    }

    fn queue_id(&self) -> QueueId {
        self.queue.queue_id()
    }

    fn queue_type(&self) -> QueueType {
        self.queue.queue_type()
    }

    fn pending_batches_count(&self) -> usize {
        self.queue.pending_batches_count()
    }

    fn allocated_ring_bytes(&self) -> usize {
        self.queue.allocated_ring_bytes()
    }
}

impl Drop for QueueHandle {
    fn drop(&mut self) {
        let qid = self.queue.id;
        // 1. Spegniamo il worker thread cooperativamente
        {
            let (mutex_queue, cvar_queue) = &*self.queue.state;
            let mut guard_queue = mutex_queue.lock().unwrap();
            guard_queue.is_shutdown = true;
            drop(guard_queue);
            cvar_queue.notify_all();
        }

        // 2. Rimuoviamo la coda dalla tabella delle code attive del dispatcher
        let (disp_mutex, disp_cvar) = &*self.queue.shared_dispatcher.inner;
        let mut disp_guard = disp_mutex.lock().unwrap();
        disp_guard.queues.remove(&qid);
        drop(disp_guard);
        disp_cvar.notify_all();
    }
}

/// Stato interno di un semaforo timeline a progressione monotona a 64 bit.
pub struct SemaphoreState {
    /// Valore attuale raggiunto dalla timeline del semaforo.
    value: u64,
    /// Contatore di generazione incrementato ad ogni reset hardware (TDR).
    /// Permette di invalidare in modo sicuro tutti i thread in attesa con `DeviceLost`.
    generation: usize,
}

/// Struttura descrittiva di un semaforo timeline sincronizzato.
pub struct MySemaphore {
    state: Arc<(Mutex<SemaphoreState>, Condvar)>,
}

impl Clone for MySemaphore {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// Tabella globale delle risorse gestite dal dispositivo GPU:
/// code attive e semafori timeline registrati.
pub struct DispatcherState {
    next_queue_id: QueueId,
    queues: HashMap<QueueId, MyQueue>,

    next_semaphore_id: SemaphoreId,
    semaphores: HashMap<SemaphoreId, MySemaphore>,
}

impl DispatcherState {
    pub fn new() -> Self {
        Self {
            next_queue_id: 0,
            queues: HashMap::new(),
            next_semaphore_id: 0,
            semaphores: HashMap::new(),
        }
    }

    /// Restituisce `true` solo se TUTTE le code attualmente attive sono completamente inattive.
    pub fn all_completed_queues(&self) -> bool {
        self.queues.iter().all(|(_, queue)| queue.is_idle())
    }
}

/// Coordinatore centrale del dispositivo GPU (`GpuDispatcher`).
pub struct MyGpuDispatcher {
    inner: Arc<(Mutex<DispatcherState>, Condvar)>,
}

impl MyGpuDispatcher {
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(DispatcherState::new()), Condvar::new())),
        }
    }
}

impl GpuDispatcher for MyGpuDispatcher {
    /// Crea e attiva una nuova coda hardware restituendo l'handle RAII associato.
    fn create_queue(
        &self,
        queue_type: QueueType,
        ring_capacity_bytes: usize,
    ) -> (QueueId, impl GpuQueue + 'static) {
        let mut guard = self.inner.0.lock().unwrap();
        let actual_queue_id = guard.next_queue_id;
        guard.next_queue_id += 1;

        let new_queue = MyQueue::new(
            actual_queue_id,
            queue_type,
            ring_capacity_bytes,
            &self,
        );
        guard.queues.insert(actual_queue_id, new_queue.clone());

        (actual_queue_id, QueueHandle { queue: new_queue })
    }

    /// Crea un nuovo semaforo timeline inizializzato con il valore specificato.
    fn create_semaphore(&self, initial_value: u64) -> SemaphoreId {
        let mut guard = self.inner.0.lock().unwrap();
        let actual_semaphore_id = guard.next_semaphore_id;
        guard.next_semaphore_id += 1;

        guard.semaphores.insert(
            actual_semaphore_id,
            MySemaphore {
                state: Arc::new((
                    Mutex::new(SemaphoreState {
                        value: initial_value,
                        generation: 0,
                    }),
                    Condvar::new(),
                )),
            },
        );

        actual_semaphore_id
    }

    /// Restituisce il valore corrente della timeline del semaforo specificato.
    fn read_semaphore(&self, sem: SemaphoreId) -> Result<u64, GpuError> {
        let guard = self.inner.0.lock().unwrap();
        if let Some(semaphore_state) = guard.semaphores.get(&sem) {
            let semaphore_guard = semaphore_state.state.0.lock().unwrap();
            Ok(semaphore_guard.value)
        } else {
            Err(InvalidSemaphore)
        }
    }

    /// Avanza il semaforo timeline dal thread host (CPU).
    ///
    /// Per prevenire deadlock:
    /// Estraiamo l'`Arc` del semaforo dalla mappa e rilasciamo IMMEDIATAMENTE il lock
    /// globale del dispatcher prima di bloccarci sul mutex del semaforo o notificare i waiter.
    fn host_signal_semaphore(&self, sem: SemaphoreId, new_value: u64) -> Result<(), GpuError> {
        let semaphore_arc = {
            let guard = self.inner.0.lock().unwrap();
            guard.semaphores.get(&sem).cloned()
        };

        if let Some(semaphore_state) = semaphore_arc {
            let (mutex_semaphore, cvar_semaphore) = &*semaphore_state.state;
            let mut guard_semaphore = mutex_semaphore.lock().unwrap();
            if new_value > guard_semaphore.value {
                guard_semaphore.value = new_value;
                drop(guard_semaphore);
                // Notifichiamo tutti i thread host e le code GPU in attesa su questo semaforo
                cvar_semaphore.notify_all();
                Ok(())
            } else {
                Err(NonMonotonicTimeline)
            }
        } else {
            Err(InvalidSemaphore)
        }
    }

    /// Blocca il thread chiamante (CPU host) finché il semaforo non raggiunge `target_value`.
    ///
    /// Se durante l'attesa il semaforo viene invalidato da un reset TDR (la sua generazione aumenta),
    /// la funzione si sblocca immediatamente restituendo `Err(GpuError::DeviceLost)`.
    fn host_wait_semaphore(&self, sem: SemaphoreId, target_value: u64) -> Result<(), GpuError> {
        let semaphore_arc = {
            let guard = self.inner.0.lock().unwrap();
            guard.semaphores.get(&sem).cloned()
        };

        if let Some(semaphore_state) = semaphore_arc {
            let (mutex_semaphore, cvar_semaphore) = &*semaphore_state.state;
            let mut guard_semaphore = mutex_semaphore.lock().unwrap();
            let actual_generation = guard_semaphore.generation;

            // Attesa passiva su condvar finché il valore non raggiunge la soglia o cambia la generazione
            guard_semaphore = cvar_semaphore
                .wait_while(guard_semaphore, |c| {
                    c.value < target_value && c.generation == actual_generation
                })
                .unwrap();

            if guard_semaphore.generation > actual_generation {
                Err(DeviceLost)
            } else {
                Ok(())
            }
        } else {
            Err(InvalidSemaphore)
        }
    }

    /// Blocca il thread chiamante finché tutte le code non hanno completato ogni sottomissione pendente.
    fn wait_idle(&self) {
        let (mutex, cvar) = &*self.inner;
        let guard = mutex.lock().unwrap();
        let _guard = cvar
            .wait_while(guard, |c| !c.all_completed_queues())
            .unwrap();
    }

    /// Simula un reset hardware (TDR - Timeout Detection and Recovery) sulla coda indicata:
    /// 1. La coda viene marcata come `is_reset = true`.
    /// 2. Tutti i batch pendenti vengono svuotati e la memoria del ring buffer azzerata.
    /// 3. I semafori dipendenti vengono invalidati (incrementando la generazione) per sbloccare
    ///    i waiter con `Err(GpuError::DeviceLost)`.
    fn reset_queue(&self, queue: QueueId) -> Result<(), GpuError> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if let Some(queue_state) = guard.queues.get_mut(&queue) {
            let (mutex_queue, cvar_queue) = &*queue_state.state;
            let mut guard_queue = mutex_queue.lock().unwrap();
            // Marcatura di reset permanente per la coda
            guard_queue.is_reset = true;
            guard_queue.batches.clear();
            guard_queue.memory_used = 0;
            drop(guard_queue);
            cvar_queue.notify_all();

            // Avanziamo la generazione dei semafori registrati per sbloccare i thread in attesa con DeviceLost
            for (_, sem_state) in guard.semaphores.iter_mut() {
                let (sem_mutex, sem_cvar) = &*sem_state.state;
                let mut sem_guard = sem_mutex.lock().unwrap();
                sem_guard.generation += 1;
                drop(sem_guard);
                sem_cvar.notify_all();
            }

            drop(guard);
            cvar.notify_all();
            Ok(())
        } else {
            Err(InvalidSemaphore)
        }
    }

    /// Restituisce il numero di code attualmente attive e registrate nel dispositivo.
    fn active_queue_count(&self) -> usize {
        let guard = self.inner.0.lock().unwrap();
        guard.queues.len()
    }
}

impl Clone for MyGpuDispatcher {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Inizializza un nuovo coordinatore del dispositivo GPU e dei semafori timeline.
pub fn make_gpu_dispatcher() -> impl GpuDispatcher {
    MyGpuDispatcher::new()
}

