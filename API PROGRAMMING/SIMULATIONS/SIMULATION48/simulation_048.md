# Simulazione 048 — LockGraph (capstone)

*Esattamente due tratti pubblici, come `TaskGraph` (046) e `Signal` (047) — ma qui il problema architetturale si sposta su un terreno ancora inesplorato: la gestione di un grafo di attese dinamico (Wait-For-Graph) per il rilevamento e la risoluzione attiva dei deadlock in un motore transazionale. A differenza di un DAG di task le cui dipendenze sono dichiarate staticamente prima dell'esecuzione, qui gli archi si creano e si distruggono a runtime a ogni tentativo di acquisizione o rilascio di risorsa: la vera sfida è identificare i cicli nel momento esatto in cui si formano, designare deterministicamente una vittima e propagarne l'annullamento a cascata senza corrompere lo stato né causare attese attive.*

---

## LockGraph

Nei sistemi di gestione di database (DBMS) e nei motori transazionali distribuiti, più transazioni concorrenti richiedono l'accesso a risorse condivise identificabili univocamente. Per garantire l'isolamento e la coerenza dei dati, l'accesso avviene mediante lock che possono essere concessi in due modalità:
- **Shared (`S`)**: più transazioni possono detenere contemporaneamente un lock condiviso sulla medesima risorsa per operazioni di sola lettura.
- **Exclusive (`X`)**: una sola transazione alla volta può detenere un lock esclusivo, precludendo qualsiasi altro accesso concorrente (sia `S` che `X`).

Quando una transazione richiede un lock incompatibile con lo stato attuale della risorsa, deve essere sospesa senza consumare cicli di CPU finché la risorsa non torna disponibile. Tuttavia, se le transazioni acquisiscono le risorse dinamicamente in ordini arbitrari, possono insorgere **deadlock ciclici**: la transazione $T_1$ detiene $R_A$ e attende $R_B$, mentre la transazione $T_2$ detiene $R_B$ e attende $R_A$. Nessuna delle due potrà mai avanzare spontaneamente.

Per prevenire stalli indefiniti del sistema, il coordinatore dei lock deve mantenere un grafo orientato delle attese (**Wait-For-Graph**, WFG): ogni volta che una transazione si blocca in attesa di una risorsa, registra un arco orientato verso ciascuna transazione che attualmente detiene quella risorsa. Se l'inserimento di un arco crea un ciclo nel grafo, è insorto un deadlock: il coordinatore deve immediatamente rilevare il ciclo, selezionare una delle transazioni coinvolte come **vittima**, abortirla d'ufficio e revocare tutti i suoi lock per spezzare il ciclo e consentire alle altre transazioni di proseguire.

Si scrivano in Rust le strutture che implementano i tratti `TxHandle` e `LockManager` definiti di seguito.

---

### API richiesta

```rust
pub type TxId = u64;
pub type ResourceId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockError {
    DeadlockVictim,
    AlreadyAborted,
}

pub trait TxHandle: Send {
    // Tenta di acquisire il lock sulla risorsa nella modalità richiesta.
    // Blocca il chiamante, senza consumare cicli di CPU, se la risorsa è occupata
    // in modo incompatibile, finché non viene concessa oppure finché la transazione
    // non viene selezionata come vittima di un deadlock.
    //
    // Restituisce:
    // - Ok(()) se il lock è stato acquisito con successo.
    // - Err(LockError::DeadlockVictim) se la transazione è stata abortita dal rilevatore di deadlock.
    // - Err(LockError::AlreadyAborted) se la transazione era già stata abortita in precedenza.
    fn acquire(&self, resource: ResourceId, mode: LockMode) -> Result<(), LockError>;

    // Rilascia anticipatamente una risorsa precedentemente acquisita da questa transazione.
    // Risveglia eventuali altre transazioni bloccate in attesa di quella risorsa.
    // Restituisce false se la transazione non deteneva tale risorsa o se è già abortita.
    fn release(&self, resource: ResourceId) -> bool;

    // Consuma l'handle, completando con successo la transazione e rilasciando atomicamente
    // tutte le risorse ancora detenute.
    // Restituisce true se la transazione era attiva ed è stata committata con successo;
    // restituisce false se la transazione era già stata abortita (ad esempio per deadlock).
    fn commit(self) -> bool;
}

pub trait LockManager: Clone + Send + Sync {
    // Avvia una nuova transazione assegnandole un TxId univoco strettamente crescente
    // e restituendo il rispettivo handle di controllo.
    fn begin_tx(&self) -> (TxId, impl TxHandle);

    // Restituisce il numero di transazioni attualmente attive (non ancora committate né abortite).
    fn active_tx_count(&self) -> usize;

    // Restituisce il numero totale di archi orientati attualmente presenti nel Wait-For-Graph.
    fn wait_edge_count(&self) -> usize;
}

pub fn make_lock_manager() -> impl LockManager {
    ...
}
```

---

### Requisiti

- **Modalità di Lock**:
  - Più transazioni possono detenere contemporaneamente la modalità `Shared` sulla stessa risorsa.
  - Una transazione che richiede `Exclusive` deve attendere finché non ci sono né detentori `Shared` né detentori `Exclusive`.
  - Più richieste in attesa devono essere servite in modo fair (FIFO o ordine di risveglio) senza starvation.
- **Rilevamento del Deadlock (Wait-For-Graph)**:
  - Nel momento esatto in cui una transazione $T$ non può ottenere immediatamente un lock e deve bloccarsi, vengono aggiunti al WFG gli archi orientati $T \to H$ per ogni transazione $H$ che attualmente detiene quella risorsa in modo conflittuale.
  - Se tale aggiunta genera un ciclo orientato nel WFG (ad esempio $T \to T_1 \to \dots \to T$), il coordinatore deve rilevare il ciclo ed eleggere **una vittima** (convenzione standard: la transazione chiamante $T$, oppure la transazione nel ciclo con il `TxId` più recente) per interrompere il deadlock.
  - La vittima eletta viene immediatamente marcata come `Aborted`: il suo metodo `acquire()` in corso si sblocca restituendo `Err(LockError::DeadlockVictim)`, tutti i suoi lock precedentemente acquisiti vengono rilasciati d'ufficio e tutti gli archi nel WFG che la coinvolgono (sia entranti che uscenti) vengono rimossi.
- **Gestione RAII (`Drop`)**:
  - Se un `TxHandle` esce dallo scope senza che sia stato invocato esplicitamente `commit()`, il suo distruttore (`Drop`) deve considerare la transazione abortita, rilasciando tutte le risorse ancora detenute e risvegliando chi è in attesa, senza causare panico né stalli.
- **Thread-Safety & Sincronizzazione**:
  - Thread-safe, condivisibile (`Clone + Send + Sync`).
  - Nessuna attesa attiva ("senza consumare cicli di CPU").
  - `release()` e il rilascio in `commit()` o `Drop` devono risvegliare in modo non attivo le transazioni in coda sulla risorsa rilasciata.
  - Nessun deadlock interno tra i lock del coordinatore.
- **I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).**
- **Se il codice consegnato non compila, non verrà valutato.**

---

### Suggerimenti implementativi

1. Ciascuna risorsa deve tenere traccia sia dello stato corrente (se è libera, se è in modalità `Shared` con l'elenco dei `TxId` detentori, oppure in modalità `Exclusive` con il singolo `TxId` detentore), sia della coda delle transazioni attualmente in attesa di acquisirla.
2. Il grafo delle attese (WFG) può essere modellato come una mappa di adiacenza tra `TxId`: per rilevare se l'aggiunta di un arco $T \to H$ genera un ciclo, è sufficiente verificare se $T$ è raggiungibile da $H$ mediante una visita orientata (DFS/BFS) del grafo.
3. Se si opta per far attendere ciascuna transazione sulla propria condizione o su un `Condvar` condiviso, occorre assicurarsi che la notifica avvenga sia quando una risorsa si libera, sia quando la transazione stessa viene abortita d'ufficio come vittima di un deadlock.
4. Quando una transazione rilascia una risorsa (o viene abortita), occorre aggiornare sia lo stato della risorsa sia gli archi del WFG, verificando se le transazioni in attesa possono ora ottenere il lock.

---

## Meta-commentario

**Perché `LockGraph` è l'equivalente sistemistico e speculare di `TaskGraph` (046):**
In `TaskGraph`, il grafo è un DAG aciclico costruito *a priori* mediante `depends_on`: la complessità risiede nel costruire la relazione inversa ("chi dipende da me") e nel propagare i fallimenti in avanti verso le foglie. In `LockGraph`, invece, il grafo è *dinamico, mutevole e potenzialmente ciclico*: gli archi rappresentano contese concorrenti non prevedibili a priori. La complessità si sposta quindi sull'invariante di aciclicità: il sistema deve tollerare la richiesta di archi che creerebbero cicli, intercettarli istantaneamente prima che congelino i thread, e spezzarli sacrificando la vittima corretta.

**La scoperta architetturale chiave: la dissociazione tra lock di risorsa e identità transazionale:**
Un'implementazione ingenua tenderebbe a proteggere ogni risorsa con un singolo `RwLock` di sistema. Questa scelta fallisce miseramente nel contesto transazionale per due ragioni:
1. Un `RwLock` standard del sistema operativo non sa *chi* detiene il lock (non memorizza i `TxId`), impedendo di costruire il Wait-For-Graph;
2. Un thread bloccato su un `RwLock` standard non può essere "risvegliato forzatamente" dal di fuori quando il coordinatore decide di sacrificarlo per deadlock.
Serve quindi una struttura dati coordinata centralmente (protetta da un unico lock di stato con relative `Condvar`), in cui le risorse sono entità astratte e il blocco delle transazioni è governato da predicati espliciti su `Condvar`.

**Convergenza tra abort manuale, abort da deadlock e distruzione RAII (`Drop`):**
Proprio come raccomandato in `TaskGraph` (046) e `PoisonableBarrier` (039), le tre cause di terminazione anomala di una transazione (`acquire()` che subisce deadlock, transazione abbandonata che esce dallo scope chiamando `Drop`, o abort conseguente a un errore precedente) non devono avere tre percorsi di sblocco differenti. Tutte e tre devono convergere su una singola funzione interna di teardown (`abort_tx_internal`) che bonifica i lock detenuti, pota gli archi del grafo e risveglia i thread in attesa.

**Strutture cooperanti stimate (senza prescriverle):**
1. Uno stato globale protetto da `Mutex` contenente:
   - Registro delle risorse (`HashMap<ResourceId, ResourceState>`).
   - Grafo delle attese (`HashMap<TxId, HashSet<TxId>>`).
   - Tabella delle transazioni (`HashMap<TxId, TxData>`).
2. Una o più `Condvar` per il risveglio mirato dei thread in attesa di risorse o notificati di abort.
3. L'handle transazionale leggero (`TxHandle`) che mantiene il proprio `TxId` e il riferimento condiviso al coordinatore.

**Difficoltà stimata:** paragonabile a `TaskGraph` (046) e `MvccStore` (034). Richiede padronanza di lock cooperanti, algoritmi di visita su grafi concorrenti e gestione precisa del risveglio di thread bloccati. Budget stimato: 120–150 minuti.
