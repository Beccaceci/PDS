//! # Simulazione 048 — LockGraph (capstone)
//!
//! Nei sistemi transazionali e nei DBMS, più transazioni concorrenti richiedono
//! l'accesso a risorse identificate univocamente tramite lock Shared (`S`) o Exclusive (`X`).
//!
//! Quando una transazione richiede un lock incompatibile con lo stato attuale della risorsa,
//! deve essere sospesa senza consumare cicli di CPU finché la risorsa non torna disponibile.
//! Per prevenire stalli indefiniti del sistema (deadlock), il coordinatore mantiene un grafo
//! orientato delle attese (Wait-For-Graph): se l'aggiunta di una richiesta di lock genera
//! un ciclo, il coordinatore rileva il deadlock, designa una vittima, la abortisce d'ufficio
//! revocando tutti i suoi lock e consentendo alle altre transazioni di proseguire.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `TxHandle` e `LockManager`.

use std::{collections::{HashMap, HashSet}, sync::{Arc, Condvar, Mutex}};

use crate::{LockError::{AlreadyAborted, DeadlockVictim}, LockMode::Shared, TxStatus::{Aborted, Active, Committed}};

pub type TxId = u64;
pub type ResourceId = u64;

/// Modalità di lock richiesta su una specifica risorsa.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Più transazioni possono condividere la risorsa in lettura.
    Shared,
    /// Accesso esclusivo per una sola transazione (esclude sia Shared che Exclusive).
    Exclusive,
}

/// Possibili errori restituiti durante l'acquisizione di un lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    /// La transazione è stata selezionata come vittima per spezzare un ciclo di deadlock.
    DeadlockVictim,
    /// La transazione era già stata abortita in precedenza.
    AlreadyAborted,
}

/// Tratto che rappresenta l'handle di una transazione attiva.
pub trait TxHandle: Send {
    /// Tenta di acquisire il lock sulla risorsa nella modalità richiesta.
    /// Blocca il chiamante, senza consumare cicli di CPU, se la risorsa è occupata
    /// in modo incompatibile, finché non viene concessa o finché la transazione
    /// non viene abortita come vittima di un deadlock.
    fn acquire(&self, resource: ResourceId, mode: LockMode) -> Result<(), LockError>;

    /// Rilascia anticipatamente una risorsa precedentemente acquisita da questa transazione.
    /// Risveglia eventuali transazioni in attesa su tale risorsa.
    /// Restituisce false se la transazione non deteneva la risorsa o se è già abortita.
    fn release(&self, resource: ResourceId) -> bool;

    /// Consuma l'handle, completando la transazione e rilasciando atomicamente
    /// tutte le risorse ancora detenute.
    /// Restituisce true se la transazione era attiva ed è stata committata con successo;
    /// restituisce false se la transazione era già stata abortita.
    fn commit(self) -> bool;
}

/// Tratto che rappresenta il coordinatore del grafo dei lock.
pub trait LockManager: Clone + Send + Sync {
    /// Avvia una nuova transazione assegnandole un TxId univoco crescente e restituendo il rispettivo handle di controllo.
    fn begin_tx(&self) -> (TxId, impl TxHandle + 'static);

    /// Restituisce il numero di transazioni attualmente attive (non ancora committate né abortite).
    fn active_tx_count(&self) -> usize;

    /// Restituisce il numero totale di archi orientati attualmente presenti nel Wait-For-Graph.
    fn wait_edge_count(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// ---------------------------------------------------------------------------------
// 📐 ARCHITETTURA DEL SISTEMA E RELAZIONI TRA LE STRUTTURE (LOCK GRAPH / WFG):
//
// 1. `MyLockManager`:
//    - Coordinatore globale del sistema transazionale (`LockManager`), condivisibile
//      tra thread tramite `Clone + Send + Sync`.
//    - Incapsula l'intero stato in `Arc<(Mutex<ManagerState>, Condvar)>`.
//    - La `Condvar` globale consente di risvegliare in modo non attivo (senza consumo
//      di cicli CPU) tutti i thread in attesa quando una risorsa si libera o una
//      vittima viene abortita.
//
// 2. `ManagerState`:
//    - Rappresenta il punto unico di verità protetto dall'unico `Mutex` del coordinatore:
//      * `transactions: Vec<MyTrasaction>`: registro di tutte le transazioni create.
//      * `resources: HashMap<ResourceId, ResourceState>`: stato di ciascuna risorsa
//        (`Free`, `Shared` con insieme dei lettori, `Exclusive` con singolo scrittore).
//      * `wait_for_graph: HashMap<TxId, HashSet<TxId>>`: grafo orientato delle attese
//        ($T_{wait} \to T_{holder}$), interrogabile sia per contare gli archi
//        (`wait_edge_count`), sia per rilevare cicli tramite DFS prima di addormentarsi.
//
// 3. `MyTrasaction`:
//    - Stato privato interno di una transazione memorizzato dentro `ManagerState`:
//      * `status`: stato del ciclo di vita (`Active`, `Committed`, `Aborted`).
//      * `acquired_resources`: insieme delle risorse attualmente detenute con successo.
//
// 4. `MyTxHandle`:
//    - Handle leggero restituito al client da `begin_tx()`.
//    - Implementa il tratto `TxHandle` (`acquire`, `release`, `commit`) e il tratto `Drop`.
//    - Gestione RAII: se l'handle esce dallo scope senza `commit()`, `Drop` rilascia
//      automaticamente tutti i lock detenuti e marca la transazione come `Aborted`.
//
// 🔄 DINAMICA DEL WAIT-FOR-GRAPH (WFG) E RISOLUZIONE DEI DEADLOCK:
//    - Quando una transazione $T$ richiede un lock incompatibile, calcola l'insieme $H$
//      dei possessori attuali e registra gli archi $T \to h$ nel `wait_for_graph`.
//    - Esegue una visita DFS orientata partendo da $T$: se rileva un cammino che ritorna
//      a $T$, si è formato un ciclo (deadlock!).
//    - Elezione della vittima: la transazione chiamante $T$ si auto-sacrifica, rilasciando
//      subito tutte le proprie risorse, rimuovendosi dal WFG, notificando i thread
//      in attesa (`cvar.notify_all()`) e restituendo `Err(LockError::DeadlockVictim)`.
//    - Se nessun ciclo si forma, il thread si sospende sulla `Condvar`. Al risveglio,
//      se il lock viene concesso, rimuove i propri archi dal WFG.
// ---------------------------------------------------------------------------------

/// Handle transazionale restituito da `begin_tx()`.
/// Consente alla transazione di acquisire/rilasciare risorse in modo coordinato,
/// committare l'operazione o abortire automaticamente all'uscita dallo scope (RAII).
pub struct MyTxHandle {
    tx_id: TxId,
    manager: MyLockManager,
}

impl TxHandle for MyTxHandle {
    /// Tenta di acquisire il lock sulla risorsa nella modalità richiesta (`Shared` o `Exclusive`).
    /// - Se la transazione è già abortita, restituisce immediatamente `Err(AlreadyAborted)`.
    /// - Se la risorsa è disponibile, concede il lock, rimuove eventuali archi dal WFG e restituisce `Ok(())`.
    /// - Se la risorsa è occupata, registra gli archi nel WFG ed esegue la rilevazione dei cicli (DFS):
    ///   * Se si rileva un ciclo: la transazione si auto-elegge vittima, bonifica i lock e restituisce `Err(DeadlockVictim)`.
    ///   * Altrimenti si sospende sulla Condvar in modo non attivo finché la risorsa non si libera.
    fn acquire(&self, resource: ResourceId, mode: LockMode) -> Result<(), LockError> {
        let mut num_loops = 0usize;
        let (mutex, cvar) = &*self.manager.inner;

        loop {
            num_loops += 1;
            let mut guard = mutex.lock().unwrap();
            let status = &guard.transactions[self.tx_id as usize].status;

            if matches!(status, Active) {
                // Transazione attiva: verifichiamo la risorsa
                let resource_state = guard.resources.entry(resource).or_insert(ResourceState::Free);

                if matches!(resource_state, ResourceState::Free) {
                    if matches!(mode, Shared) {
                        let new_set = HashSet::new();
                        *resource_state = ResourceState::Shared(new_set);
                    } else {
                        *resource_state = ResourceState::Exclusive(self.tx_id);

                        // Lock concesso: rimozione dell'attesa dal WFG
                        guard.wait_for_graph.remove(&self.tx_id);
                        let this_transaction = &mut guard.transactions[self.tx_id as usize];
                        this_transaction.acquired_resources.insert(resource);
                        return Ok(());
                    }
                } else if let (ResourceState::Shared(set), Shared) = (&mut *resource_state, mode) {
                    // Risorsa condivisa richiesta in lettura: compatibile
                    set.insert(self.tx_id);

                    guard.wait_for_graph.remove(&self.tx_id);
                    let this_transaction = &mut guard.transactions[self.tx_id as usize];
                    this_transaction.acquired_resources.insert(resource);
                    return Ok(());
                } else {
                    // Risorsa occupata in modo incompatibile: individuiamo i detentori conflittuali
                    let mut holders = HashSet::new();
                    if let ResourceState::Shared(set) = resource_state {
                        holders = set.clone();
                    } else if let ResourceState::Exclusive(holder) = resource_state {
                        holders.insert(*holder);
                    }

                    // Registriamo gli archi orientati nel WFG: self.tx_id -> holder
                    for holder in holders {
                        if let Some(edges) = guard.wait_for_graph.get_mut(&self.tx_id) {
                            edges.insert(holder);
                        } else {
                            let mut new_set = HashSet::new();
                            new_set.insert(holder);
                            guard.wait_for_graph.insert(self.tx_id, new_set);
                        }
                    }

                    // Rilevamento attivo del ciclo nel Wait-For-Graph
                    if guard.has_cycle(self.tx_id) {
                        // Deadlock rilevato: questa transazione si sacrifica come vittima
                        let this_transaction = &mut guard.transactions[self.tx_id as usize];
                        let acquired_resources = this_transaction.acquired_resources.clone();
                        drop(guard);

                        // Rilasciamo tutte le risorse precedentemente acquisite
                        for resource in acquired_resources {
                            let _ = self.release(resource);
                        }

                        let mut guard = mutex.lock().unwrap();
                        guard.wait_for_graph.remove(&self.tx_id);
                        let this_transaction = &mut guard.transactions[self.tx_id as usize];
                        this_transaction.status = Aborted;
                        this_transaction.acquired_resources.clear();
                        drop(guard);

                        // Risvegliamo le altre transazioni che attendevano le nostre risorse
                        cvar.notify_all();
                        return Err(DeadlockVictim);
                    } else {
                        // Nessun ciclo: ci sospendiamo sulla Condvar
                        guard = cvar
                            .wait_while(guard, |c| {
                                let resource_state = c.resources.get(&resource).unwrap();
                                if matches!(*resource_state, ResourceState::Free)
                                    || (matches!(*resource_state, ResourceState::Shared(_))
                                        && matches!(mode, Shared))
                                {
                                    return false;
                                }
                                true
                            })
                            .unwrap();
                    }
                }
            } else if matches!(status, Aborted) {
                // Distinguiamo se era già abortita prima della chiamata o se è stata abortita durante l'attesa
                if num_loops == 1 {
                    return Err(AlreadyAborted);
                } else {
                    return Err(DeadlockVictim);
                }
            } else {
                unreachable!("The transaction was already completed")
            }
        }
    }

    /// Rilascia anticipatamente un lock su una risorsa precedentemente acquisita.
    /// Se la risorsa torna completamente libera, risveglia le transazioni in attesa.
    /// Restituisce `false` se la transazione non deteneva la risorsa o se non è attiva.
    fn release(&self, resource: ResourceId) -> bool {
        let (mutex, cvar) = &*self.manager.inner;
        let mut guard = mutex.lock().unwrap();
        let this_transaction = &mut guard.transactions[self.tx_id as usize];

        if matches!(this_transaction.status, Active)
            && this_transaction.acquired_resources.contains(&resource)
        {
            this_transaction.acquired_resources.remove(&resource);
            let target_resource = guard.resources.get_mut(&resource).unwrap();

            if let ResourceState::Shared(set) = target_resource {
                if set.contains(&self.tx_id) {
                    set.remove(&self.tx_id);

                    if set.is_empty() {
                        *target_resource = ResourceState::Free;
                        drop(guard);
                        cvar.notify_all();
                    }
                } else {
                    return false;
                }
            } else if let ResourceState::Exclusive(tx_id) = target_resource {
                if *tx_id == self.tx_id {
                    *target_resource = ResourceState::Free;
                    drop(guard);
                    cvar.notify_all();
                } else {
                    return false;
                }
            }

            true
        } else {
            false
        }
    }

    /// Consuma l'handle, completando con successo la transazione e rilasciando tutte le risorse detenute.
    /// Restituisce `true` se la transazione era attiva ed è stata committata;
    /// restituisce `false` se la transazione era già stata abortita in precedenza.
    fn commit(self) -> bool {
        let (mutex, _) = &*self.manager.inner;
        let mut guard = mutex.lock().unwrap();
        let this_transaction = &mut guard.transactions[self.tx_id as usize];
        let acquired_resources = this_transaction.acquired_resources.clone();

        if matches!(this_transaction.status, Aborted) {
            return false;
        } else if acquired_resources.is_empty() {
            this_transaction.status = Committed;
            return true;
        } else if matches!(this_transaction.status, Committed) {
            unreachable!()
        }

        // Rilasciamo il lock prima di invocare release() per evitare lock re-entranti
        drop(guard);

        let mut commit = true;
        for resource in acquired_resources {
            let outcome = self.release(resource);

            if commit && !outcome {
                commit = false;
            }
        }

        let mut guard = mutex.lock().unwrap();
        let this_transaction = &mut guard.transactions[self.tx_id as usize];
        this_transaction.status = if commit { Committed } else { Aborted };
        commit
    }
}

impl Drop for MyTxHandle {
    /// Distruttore RAII: se l'handle esce dallo scope senza `commit()`,
    /// la transazione viene considerata abortita e tutte le risorse detenute vengono rilasciate.
    fn drop(&mut self) {
        let (mutex, _) = &*self.manager.inner;
        let mut guard = mutex.lock().unwrap();
        let this_transaction = &mut guard.transactions[self.tx_id as usize];
        let acquired_resources = this_transaction.acquired_resources.clone();

        if matches!(this_transaction.status, Active) {
            drop(guard);

            // Rilasciamo ciascuna risorsa svegliando i thread in attesa
            for resource in acquired_resources {
                let _ = self.release(resource);
            }

            let mut guard = mutex.lock().unwrap();
            let this_transaction = &mut guard.transactions[self.tx_id as usize];
            this_transaction.status = Aborted;
        }
    }
}

/// Stato del ciclo di vita di una transazione.
pub enum TxStatus {
    Active,
    Committed,
    Aborted,
}

/// Struttura interna che memorizza i dati di una singola transazione dentro il Manager.
pub struct MyTrasaction {
    status: TxStatus,
    acquired_resources: HashSet<ResourceId>,
}

/// Stato corrente di una risorsa condivisa all'interno del gestore.
pub enum ResourceState {
    /// Nessuna transazione detiene il lock su questa risorsa.
    Free,
    /// Uno o più lettori detengono contemporaneamente il lock condiviso.
    Shared(HashSet<TxId>),
    /// Un singolo scrittore detiene il lock esclusivo sulla risorsa.
    Exclusive(TxId),
}

/// Stato globale del sistema protetto dal lock unico del manager.
pub struct ManagerState {
    next_id: TxId,
    transactions: Vec<MyTrasaction>,
    wait_for_graph: HashMap<TxId, HashSet<TxId>>,
    resources: HashMap<ResourceId, ResourceState>,
}

impl ManagerState {
    /// Inizializza un nuovo `ManagerState` vuoto.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            transactions: Vec::new(),
            wait_for_graph: HashMap::new(),
            resources: HashMap::new(),
        }
    }

    /// Rileva se l'inserimento di archi a partire da `start` crea almeno un ciclo nel Wait-For-Graph.
    /// Utilizza una visita orientata DFS ricorsiva verificando se da uno dei vicini è raggiungibile `start`.
    pub fn has_cycle(&self, start: TxId) -> bool {
        let mut visited = HashSet::new();
        // Controlliamo se da ciascun vicino a cui 'start' punta si può tornare a 'start'
        if let Some(neighbors) = self.wait_for_graph.get(&start) {
            for &next in neighbors {
                if self.dfs_cycle(next, start, &mut visited) {
                    return true;
                }
            }
        }
        false
    }

    /// Helper ricorsivo per la visita DFS: verifica se `target` è raggiungibile a partire da `current`.
    fn dfs_cycle(&self, current: TxId, target: TxId, visited: &mut HashSet<TxId>) -> bool {
        if current == target {
            return true;
        }
        if !visited.insert(current) {
            return false;
        }
        if let Some(neighbors) = self.wait_for_graph.get(&current) {
            for &next in neighbors {
                if self.dfs_cycle(next, target, visited) {
                    return true;
                }
            }
        }
        false
    }
}

/// Coordinatore centrale thread-safe del gestore dei lock.
pub struct MyLockManager {
    inner: Arc<(Mutex<ManagerState>, Condvar)>,
}

impl MyLockManager {
    /// Inizializza una nuova istanza di `MyLockManager`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(ManagerState::new()), Condvar::new())),
        }
    }
}

impl Clone for MyLockManager {
    /// Clona il gestore condividendo l'istanza `Arc` sottostante.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl LockManager for MyLockManager {
    /// Avvia una nuova transazione assegnandole un TxId univoco strettamente crescente
    /// e restituendo il rispettivo handle di controllo `MyTxHandle`.
    fn begin_tx(&self) -> (TxId, impl TxHandle + 'static) {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        let tx_id = guard.next_id;
        guard.next_id += 1;

        guard.transactions.push(MyTrasaction {
            status: Active,
            acquired_resources: HashSet::new(),
        });

        (
            tx_id,
            MyTxHandle {
                tx_id,
                manager: self.clone(),
            },
        )
    }

    /// Restituisce il numero di transazioni attualmente attive (non ancora committate né abortite).
    fn active_tx_count(&self) -> usize {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard
            .transactions
            .iter()
            .filter(|&tx| matches!(tx.status, Active))
            .count()
    }

    /// Restituisce il numero totale di archi orientati attualmente presenti nel Wait-For-Graph.
    fn wait_edge_count(&self) -> usize {
        let mut num_edges = 0usize;

        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.wait_for_graph.iter().for_each(|(_, set)| {
            num_edges += set.len();
        });

        num_edges
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Crea e inizializza un nuovo gestore di lock transazionale con rilevamento attivo dei deadlock.
pub fn make_lock_manager() -> impl LockManager {
    MyLockManager::new()
}
