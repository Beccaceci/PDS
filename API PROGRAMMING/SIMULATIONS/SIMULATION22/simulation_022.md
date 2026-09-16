# Simulazione 022 — JobScheduler (capstone)

*Seconda simulazione capstone: fonde `AgingScheduler` (013, priorità con invecchiamento), `MultiResourceManager` (011, corrispondenza di insiemi di risorse), `TaskExecutor` (004, thread di lavoro in background, qui moltiplicati) e `WaitGroup` (009, tracciamento del completamento) in un unico sistema centralizzato — un pianificatore di job in miniatura, dello stesso spirito di uno scheduler di sistema operativo o di un job scheduler distribuito semplificato.*

---

## JobScheduler

Un sistema che esegue lavori in background su un pool di thread deve, per ciascun lavoro, rispettare sia una priorità (con invecchiamento, per evitare la starvation dei lavori meno urgenti) sia la disponibilità delle risorse nominate di cui quel lavoro ha bisogno per essere eseguito in sicurezza — due lavori che richiedono risorse in comune non possono mai essere eseguiti contemporaneamente, ma due lavori con risorse disgiunte devono poter procedere in parallelo su thread di lavoro diversi.

Si scrivano in Rust le strutture che implementano i tratti generici `JobHandle` e `JobScheduler` definiti di seguito.

### API richiesta

```rust
pub trait JobHandle {
    // Annulla il job se non è ancora stato preso in carico da un thread di
    // lavoro. Restituisce true se l'annullamento ha avuto effetto, false
    // se il job era già in esecuzione o già terminato.
    fn cancel(&self) -> bool;

    // Blocca il chiamante, senza consumare cicli di CPU, finché questo
    // specifico job non è terminato (con successo o perché annullato).
    // Chiamate ripetute non hanno ulteriori effetti oltre a ritornare.
    fn join(&self);
}

pub trait JobScheduler: Clone {
    // Registra un nuovo job con priorità di base `priority` (soggetta a
    // invecchiamento: la priorità effettiva cresce con il tempo di attesa in coda
    // secondo la formula: priorita_effettiva = base_priority + tempo_attesa_in_ms)
    // e l'insieme di risorse nominate richieste per la sua esecuzione, identificate da stringhe.
    //
    // Un job diventa eleggibile per l'esecuzione da parte di un thread di
    // lavoro libero quando, contemporaneamente: ha la priorità effettiva
    // più alta tra i job pendenti le cui risorse sono tutte disponibili
    // in quel momento, e tutte le risorse che richiede sono libere (non
    // occupate da nessun job in esecuzione). Un job con priorità più alta
    // ma risorse non disponibili non deve impedire l'esecuzione di un job
    // a priorità inferiore le cui risorse sono libere.
    //
    // Due job le cui risorse richieste si sovrappongono anche solo
    // parzialmente non devono mai essere in esecuzione contemporaneamente.
    fn submit(
        &self,
        priority: u32,
        resources: &[String],
        job: impl FnOnce() + Send + 'static,
    ) -> impl JobHandle;

    // Impedisce l'accodamento di nuovi job; i job già pendenti o in
    // esecuzione proseguono normalmente.
    fn close(&self);

    // Blocca il chiamante, senza consumare cicli di CPU, finché tutti i
    // job già sottomessi al momento della chiamata (pendenti o in
    // esecuzione) non sono terminati.
    fn join_all(&self);
}

pub fn make_job_scheduler(worker_count: usize) -> impl JobScheduler {
    ...
}
```

### Requisiti

- Alla costruzione, `worker_count` thread di lavoro devono essere avviati immediatamente, ciascuno capace di eseguire qualunque job eleggibile trovi.
- Thread-safe, condivisibile (`Clone`); `submit`, `close`, `join_all` possono essere chiamati da più thread contemporaneamente.
- La decisione "quali risorse sono libere, quale job eseguire" deve essere presa in modo atomico: non deve mai verificarsi che due thread di lavoro verifichino contemporaneamente la disponibilità delle stesse risorse e le assegnino entrambi allo stesso momento a job diversi.
- `cancel()` ha effetto solo su un job ancora pendente (non ancora preso in carico da un thread di lavoro).
- Nessuna attesa attiva in nessun punto, inclusi i thread di lavoro in attesa di un job eleggibile.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con: una collezione di job pendenti (ciascuno con priorità di base, istante di inserimento, risorse richieste, e la chiusura da eseguire), un insieme delle risorse nominate correntemente occupate (`HashSet<String>`), protetti da `Mutex` + `Condvar` dentro un `Arc`. Ogni thread di lavoro, in un ciclo: acquisisce il lock, scorre i job pendenti in ordine di priorità effettiva decrescente cercando il primo le cui risorse richieste non intersecano l'insieme delle risorse occupate; se lo trova, lo rimuove dalla coda pendente e aggiunge le sue risorse all'insieme occupato — nella stessa operazione, sotto lo stesso lock, per evitare che un altro thread di lavoro veda una finestra in cui le risorse risultano libere ma il job è già stato assegnato altrove; rilascia il lock, esegue il job, poi riacquisisce il lock per liberare le risorse e notificare. Se nessun job pendente è eleggibile, il thread di lavoro attende (`wait_while`) che lo stato cambi.

---

## Meta-commentario

**In cosa il rischio è diverso da `MultiResourceManager` (011), non solo "lo stesso ma con thread di lavoro invece di chiamanti diretti":** lì, più chiamanti indipendenti competevano per acquisire insiemi di risorse specifiche, ciascuno agendo per proprio conto — da cui il rischio di stallo da acquisizione multipla e la necessità di un ordine totale. Qui la decisione "chi ottiene cosa" è presa da un solo arbitro alla volta (qualunque thread di lavoro detenga il lock in quell'istante): non c'è competizione multi-parte nello stesso senso, quindi non serve alcun ordinamento delle risorse per evitare lo stallo. Il rischio si sposta altrove: un controllo di disponibilità seguito da una prenotazione in due passi separati (rilasciando il lock tra i due) permetterebbe a un secondo thread di lavoro di intrufolarsi nel mezzo e assegnare le stesse risorse a un job diverso — un classico bug *time-of-check to time-of-use* (TOCTOU), concettualmente distinto dallo stallo per quanto altrettanto insidioso.

**In cosa il rischio è diverso da `AgingScheduler` (013):** lì, la priorità effettiva più alta vinceva sempre. Qui, un job a priorità altissima ma con risorse occupate non deve bloccare l'intero sistema: un thread di lavoro libero deve poter scorrere oltre quel job e trovarne uno a priorità inferiore ma eseguibile subito — un'implementazione che si ferma al primo job per priorità, verificandone solo le risorse e bloccandosi se non disponibili, lascia inutilizzati thread di lavoro che potrebbero invece eseguire lavoro pronto.

**Perché conta esplicitamente come esercizio "capstone":** la combinazione di tutti questi vincoli contemporaneamente (priorità con invecchiamento, corrispondenza di risorse atomica, più thread di lavoro, cancellazione, completamento per-job e globale) eccede quasi certamente il tempo di un singolo appello reale. Va trattato come esercizio di progettazione architetturale integrata, non come previsione puntuale. Budget stimato: 150–180 minuti.
