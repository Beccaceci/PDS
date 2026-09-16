//! # Simulazione 028 — QuotaManager
//!
//! Un servizio condiviso tra più clienti (*tenant*) deve impedire che uno solo di essi esaurisca l'intera
//! capacità del sistema, pur non conoscendo in anticipo l'insieme completo dei tenant che lo useranno:
//! ogni tenant ha una propria quota individuale di operazioni concorrenti (`per_tenant_limit`), ma esiste
//! anche un tetto complessivo (`global_limit`) che si applica alla somma di tutte le operazioni di tutti i tenant insieme.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Permit` e `QuotaManager` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait Permit: Send {}
//!
//! pub trait QuotaManager: Clone + Send + Sync {
//!     fn acquire(&self, tenant_id: &str) -> impl Permit;
//! }
//!
//! pub fn make_quota_manager(per_tenant_limit: usize, global_limit: usize) -> impl QuotaManager {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`); i tenant non sono noti in anticipo (allocazione on-demand).
//! - `acquire(tenant_id)` blocca il chiamante finché non è possibile concedere il permesso rispettando SIA `per_tenant_limit` SIA `global_limit`.
//! - Il rilascio avviene tramite RAII (`Drop` sul tipo che implementa `Permit`), liberando capacità sia per il tenant sia globalmente.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
};

/// Trait marcatore che rappresenta un permesso di esecuzione acquisito.
///
/// Il rilascio del permesso e la conseguente liberazione di quota (sia per il tenant specifico
/// sia a livello di capacità globale) avvengono in modo deterministico tramite il trait [`Drop`].
pub trait Permit: Send {}

/// Trait che rappresenta il gestore di quote gerarchiche (limite individuale per-tenant + limite aggregato globale).
pub trait QuotaManager: Clone + Send + Sync {
    /// Acquisisce un permesso per il tenant dato, bloccando il chiamante finché la capacità lo consente.
    fn acquire(&self, tenant_id: &str) -> impl Permit;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// =================================================================================
// 🏛️ ARCHITETTURA DEL SISTEMA & INVARIANTI DI SINCRONIZZAZIONE
// =================================================================================
//
// 1. GERARCHIA DEI VINCOLI CONCORRENTI:
//    Ogni richiesta di permesso per un `tenant_id` deve soddisfare contemporaneamente DUE vincoli:
//    - Vincolo Individuale: `permessi_attivi(tenant_id) < per_tenant_limit`
//    - Vincolo Globale:     `somma_tutti_i_permessi_attivi < global_limit`
//
// 2. ATOMICITÀ DELLA DECISIONE:
//    Entrambi i vincoli vengono valutati sotto lo STESSO Mutex. Non è possibile separare il conteggio
//    globale da quello per-tenant in due lock distinti senza introdurre corse critiche o rischi di deadlock.
//
// 3. NECESSITÀ MATEMATICA DI `notify_all()`:
//    Poiché i thread in attesa valutano predicati ETEROGENEI (ciascuno attende per il proprio `tenant_id`),
//    un `notify_one()` alla liberazione di un permesso rischierebbe di svegliare un thread di un tenant
//    già saturo (che tornerebbe a dormire), lasciando addormentato per sempre un thread di un altro tenant
//    la cui quota sarebbe invece disponibile. `notify_all()` previene il "Lost Wakeup".
//
// =================================================================================

/// Handle RAII restituito al chiamante di `acquire()`.
///
/// Quando questo oggetto esce dallo scope, il suo distruttore [`Drop`] decrementa atomicamente
/// il conteggio del tenant associato e notifica tutti i thread in attesa sulla `Condvar`.
pub struct MyPermit {
    /// Nome del tenant proprietario di questo permesso.
    name: String,
    /// Riferimento condiviso allo stato della mappa dei permessi attivi.
    shared_manager: Arc<(Mutex<HashMap<String, usize>>, Condvar)>,
}

impl Permit for MyPermit {}

impl Drop for MyPermit {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.shared_manager;
        let mut guard = mutex.lock().unwrap();

        // Decrementa il contatore dei permessi per questo tenant
        if let Some(num_tenants) = guard.get_mut(&self.name) {
            if *num_tenants > 1 {
                *num_tenants -= 1;
            } else {
                // Se era l'ultimo permesso attivo per questo tenant, rimuove la voce per pulizia di memoria
                guard.remove(&self.name);
            }

            // Rilascia il lock prima della notifica per ridurre la contesa
            drop(guard);

            // Sveglia tutti i thread in attesa (sia dello stesso tenant che di altri tenant)
            cvar.notify_all();
        }
    }
}

/// Implementazione concreta del gestore di quote multi-tenant.
pub struct MyPermitManager {
    /// Mappa condivisa: `tenant_id` -> numero di permessi attualmente concessi.
    inner: Arc<(Mutex<HashMap<String, usize>>, Condvar)>,
    /// Limite massimo di permessi concorrenti per singolo tenant.
    per_tenant_limit: usize,
    /// Limite massimo aggregato di permessi concorrenti per l'intero sistema.
    global_limit: usize,
}

impl MyPermitManager {
    /// Crea una nuova istanza di `MyPermitManager` con le quote specificate.
    pub fn new(per_tenant_limit: usize, global_limit: usize) -> Self {
        Self {
            inner: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
            per_tenant_limit,
            global_limit,
        }
    }
}

impl QuotaManager for MyPermitManager {
    fn acquire(&self, tenant_id: &str) -> impl Permit {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa bloccante su Condvar: il thread attende finché la capacità
        // è insufficiente a livello individuale O a livello globale
        guard = cvar
            .wait_while(guard, |c| {
                let tenant_usage = c.get(tenant_id).copied().unwrap_or(0);
                let total_usage: usize = c.values().sum();

                // Blocca se il tenant ha raggiunto la sua quota OPPURE se il totale globale è saturo
                tenant_usage >= self.per_tenant_limit || total_usage >= self.global_limit
            })
            .unwrap();

        // Incremento atomico della quota per il tenant richiedente
        if let Some(num_tenants) = guard.get_mut(tenant_id) {
            *num_tenants += 1;
        } else {
            guard.insert(tenant_id.to_string(), 1usize);
        }

        // Restituisce l'handle RAII che rilascerà la quota su Drop
        MyPermit {
            name: tenant_id.to_string(),
            shared_manager: self.inner.clone(),
        }
    }
}

impl Clone for MyPermitManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            per_tenant_limit: self.per_tenant_limit,
            global_limit: self.global_limit,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `QuotaManager`.
pub fn make_quota_manager(
    per_tenant_limit: usize,
    global_limit: usize,
) -> impl QuotaManager {
    MyPermitManager::new(per_tenant_limit, global_limit)
}
