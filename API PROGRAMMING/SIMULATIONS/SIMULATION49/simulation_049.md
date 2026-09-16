# Simulazione 049 — SupervisorTree (capstone)

*Esattamente due tratti pubblici, come `TaskGraph` (046) e `LockGraph` (048) — ma qui il problema architetturale esplora il modello di supervisione a tolleranza di guasto in stile Erlang/OTP: un coordinatore ad albero che governa il ciclo di vita, le mailbox e le strategie di riavvio concorrenti di più attori indipendenti. La vera difficoltà progettuale risiede nella gestione coordinata del fallimento: quando un attore crasha o fallisce un'operazione, come si propagano l'annullamento e il riavvio ai suoi fratelli sotto la strategia `AllForOne`? E come si garantisce che i chiamanti esterni bloccati in attesa di una risposta (`ask`) vengano risvegliati immediatamente senza stalli né attese attive, anche se l'attore bersaglio viene distrutto da una cascata innescata da un altro thread?*

---

## SupervisorTree

Nei sistemi distribuiti ad alta affidabilità (sistemi telecomunicativi, infrastrutture cloud, motori ad attori in stile Erlang/OTP o Akka), il principio cardine della tolleranza ai guasti è il paradigma *"Let it crash"*: i singoli componenti di lavoro (gli **attori**) non tentano di gestire internamente ogni anomalia catastrofica mascherandola, ma lasciano che il fallimento si manifesti, delegando a un'entità gerarchicamente superiore (il **Supervisore**) la responsabilità del ripristino della coerenza sistemica.

Ogni attore possiede una propria **mailbox** limitata di messaggi (computazioni) e un proprio thread di esecuzione dedicato. I client esterni possono interagire con un attore in due modalità:
1. **`tell` (asincrono / fire-and-forget)**: il messaggio viene accodato nella mailbox dell'attore.
2. **`ask` (sincrono / request-reply)**: il chiamante invia il messaggio e attende, senza consumare cicli di CPU, che l'attore lo estragga, lo esegua e ne produca l'esito (`bool`).

Se l'esecuzione di un messaggio fallisce (restituisce `false` o panica), l'attore viene considerato **crashato**. Il supervisore interviene applicando una delle due strategie canoniche di riavvio:
- **`OneForOne`**: solo l'attore crashato viene terminato e riavviato con una mailbox pulita. Gli altri attori continuano la propria esecuzione inalterati.
- **`AllForOne`**: la morte di un singolo attore compromette la coerenza dell'intero gruppo di lavoro; il supervisore termina forzatamente **tutti** gli attori fratelli appartenenti allo stesso gruppo di supervisione, scartando i messaggi pendenti e riavviando l'intero collettivo in modo coordinato.

In aggiunta, il supervisore deve prevenire i cicli infiniti di crash (*flapping*): se il numero totale di riavvii supera una soglia prestabilita (`max_restarts`) all'interno di una finestra temporale mobile (`restart_window`), il supervisore dichiara il fallimento definitivo ed esaurimento del budget: tutti gli attori vengono marcati permanentemente come `Terminated`, ogni richiesta `ask` ancora in attesa viene sbloccata con errore e i messaggi non recapitabili vengono dirottati verso la **Dead-Letter Queue**.

Si scrivano in Rust le strutture che implementano i tratti `ActorRef` e `Supervisor` definiti di seguito.

---

### API richiesta

```rust
use std::time::Duration;

pub type ActorId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStrategy {
    /// Se un attore crasha, solo quell'attore viene riavviato.
    OneForOne,
    /// Se un attore crasha, tutti gli attori appartenenti al medesimo supervisore
    /// vengono terminati e riavviati atomicamente.
    AllForOne,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorStatus {
    Running,
    Restarting,
    Terminated,
}

pub trait ActorRef: Send + Sync {
    // Invia un messaggio/job all'attore e si blocca, senza consumare cicli di CPU,
    // finché l'attore non completa l'esecuzione del messaggio e restituisce l'esito.
    //
    // Se il messaggio restituisce true, la chiamata ha successo e restituisce Ok(true).
    // Se il messaggio restituisce false, l'attore crasha: il chiamante riceve Err(ActorError::ActorCrashed).
    // Se l'attore viene abbattuto dal supervisore (es. AllForOne per colpa di un fratello)
    // mentre questa ask era in attesa, la chiamata si sblocca immediatamente con
    // Err(ActorError::SupervisorTerminated).
    fn ask(&self, message: impl FnOnce() -> bool + Send + 'static) -> Result<bool, ActorError>;

    // Invia un messaggio asincrono nella mailbox dell'attore (fire-and-forget).
    // Restituisce true se il messaggio è stato inserito con successo nella mailbox;
    // restituisce false se la mailbox è satura o se l'attore non è nello stato Running.
    fn tell(&self, message: impl FnOnce() -> bool + Send + 'static) -> bool;

    // Richiede l'arresto controllato di questo attore.
    // Restituisce true se l'attore era attivo ed è stato arrestato;
    // restituisce false se era già terminato.
    fn stop(&self) -> bool;

    // Restituisce lo stato attuale dell'attore.
    fn status(&self) -> ActorStatus;
}

pub trait Supervisor: Clone + Send + Sync {
    // Registra e avvia un nuovo attore all'interno di questo supervisore, allocando la relativa mailbox con la capacità indicata.
    // Restituisce l'ActorId univoco assegnato e il rispettivo ActorRef.
    fn spawn_actor(&self, mailbox_capacity: usize) -> (ActorId, impl ActorRef + 'static);

    // Restituisce il numero di attori attualmente registrati e non terminati definitivamente.
    fn active_actor_count(&self) -> usize;

    // Restituisce il numero cumulativo di riavvii effettuati dal supervisore dall'avvio.
    fn total_restarts(&self) -> usize;

    // Restituisce il numero di messaggi finiti nella Dead-Letter Queue (ovvero messaggi
    // scartati a seguito di crash o terminazione di un attore).
    fn dead_letter_count(&self) -> usize;
}

pub fn make_supervisor(
    strategy: RestartStrategy,
    max_restarts: usize,
    restart_window: Duration,
) -> impl Supervisor {
    ...
}
```

---

### Requisiti

- **Mailbox & Esecuzione Dedicata**:
  - Ogni attore deve possedere un thread di lavoro dedicato che preleva sequenzialmente i messaggi dalla propria mailbox ed esegue il job.
  - L'estrazione deve avvenire senza attesa attiva ("senza consumare cicli di CPU" quando la mailbox è vuota).
  - Il thread di lavoro non deve mai mantenere il lock della mailbox o dello stato globale del supervisore durante l'esecuzione del job dell'utente.
- **Strategie di Ripristino (`OneForOne` vs `AllForOne`)**:
  - Quando l'esecuzione di un messaggio da parte dell'attore $A$ restituisce `false`, l'attore crasha:
    * Sotto `OneForOne`: solo $A$ viene riavviato (lo stato transita a `Restarting` e poi nuovamente a `Running` con un nuovo worker thread). Eventuali altri attori rimangono `Running`.
    * Sotto `AllForOne`: la morte di $A$ comporta l'arresto a cascata di **tutti** gli altri attori gestiti da questo supervisore. Per ciascun fratello, eventuali computazioni pendenti o `ask()` bloccate in attesa vengono revocate d'ufficio con `Err(ActorError::SupervisorTerminated)`. Successivamente, tutti gli attori del gruppo vengono riavviati insieme.
- **Finestra Mobile di Riavvio (*Flapping Prevention*)**:
  - Il supervisore mantiene la cronologia dei timestamp dei riavvii.
  - Se il numero di riavvii all'interno degli ultimi `restart_window` istanti temporali supera `max_restarts`, il supervisore dichiara il fallimento permanente dell'albero: tutti gli attori passano definitivamente allo stato `Terminated`, i relativi thread si arrestano e tutte le `ask()` pendenti si sbloccano con errore.
- **Dead-Letter Accounting**:
  - Ogni messaggio presente in una mailbox che viene spazzato via a causa di un crash o di un riavvio forzato `AllForOne` (o che non può essere consegnato perché l'attore è terminato) deve incrementare il contatore `dead_letter_count()`.
- **Sblocco Immediato dei Chiamanti di `ask()`**:
  - Nessun thread client chiamante di `ask()` deve rimanere bloccato indefinitamente se l'attore bersaglio crasha o se viene abbattuto da una terminazione a cascata.
- **Thread-Safety & Condivisione**:
  - Thread-safe, condivisibile (`Clone + Send + Sync`).
  - Nessun deadlock tra attori o tra attori e supervisore.
- **I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).**
- **Se il codice consegnato non compila, non verrà valutato.**

---

### Suggerimenti implementativi

1. Ciascun attore ha bisogno di una propria struttura mailbox protetta da un lock locale o integrata nello stato del supervisore, con una `Condvar` per sospendere il thread worker quando la mailbox è vuota.
2. Per supportare `ask()`, il messaggio inviato nella mailbox non è una semplice chiusura `FnOnce() -> bool`, ma una busta contenente sia la computazione sia un canale di risposta (ad esempio un `Arc<(Mutex<Option<Result<bool, ActorError>>>, Condvar)>`) su cui il chiamante di `ask()` può attendere il verdetto.
3. Se l'attore crasha o viene terminato a monte dal supervisore, il supervisore deve scorrere tutti i messaggi rimasti nella mailbox, notificare ciascun mittente di `ask` con l'opportuno `Err(ActorError::...)`, ed incrementare il contatore delle dead-letter.
4. Per la strategia `AllForOne`, il supervisore deve mantenere l'elenco di tutti gli `ActorId` attivi; quando uno notifica il proprio fallimento al coordinatore, quest'ultimo arresta e risveglia tutti i fratelli prima di effettuare il riavvio congiunto.

---

## Meta-commentario

**Perché `SupervisorTree` raggiunge la massima densità architetturale (pari a `TaskGraph` 046 e `LockGraph` 048):**
In `TaskGraph`, la complessità consisteva nel costruire la relazione inversa statica (chi dipende da me) per propagare a catena il fallimento. In `LockGraph`, consisteva nell'ispezionare dinamicamente i cicli nel grafo delle contese. In `SupervisorTree`, la complessità si sposta sul **coordinamento dei cicli di vita a cascata tra attori concorrenti**:
- Sotto `AllForOne`, gli attori sono entità formalmente disgiunte (ciascuno ha la propria mailbox e il proprio thread), ma il loro destino vitale è vincolato a livello di gruppo: un thread deve poter causare l'interruzione pulita e il risveglio con errore di tutti i client bloccati sui suoi fratelli, senza mai causare corruzione di memoria né lock contesi.
- La combinazione di mailbox asincrone (`tell`) e canali di sincronizzazione punto-a-punto (`ask`) esige che la morte di un attore non lasci "fantasmi": ogni `ask` in coda deve essere garantita contro il deadlock.

**La scoperta architetturale chiave: il disaccoppiamento tra il thread worker e la vita dell'attore:**
Un errore comune è identificare l'attore con il proprio `thread::JoinHandle`. Se il thread worker muore a causa di un panico o di un fallimento, l'identità dell'attore (`ActorId` e l'`ActorRef` posseduto dai client) non deve diventare invalida! L'handle esterno deve sopravvivere al riavvio, re-instradando i messaggi futuri verso la nuova incarnazione del thread worker generata dal supervisore.

**Convergenza tra fallimento locale, terminazione di gruppo e superamento del budget:**
Esattamente come la convergenza di fallimento e `cancel()` in `TaskGraph`, in questo sistema la terminazione di un messaggio pendente può avvenire per tre ragioni distinte:
1. Fallimento diretto del proprio job;
2. Uccisione indotta da un fratello sotto `AllForOne`;
3. Chiusura globale dell'albero per superamento della soglia `max_restarts` nella finestra temporale.
Tutti e tre i percorsi devono convergere sullo stesso meccanismo di drenaggio della mailbox verso la Dead-Letter Queue e di risveglio dei client bloccati.

**Strutture cooperanti stimate (senza prescriverle):**
1. Uno stato globale del supervisore (`SupervisorState`) contenente la mappa degli attori, la strategia, la cronologia temporale dei riavvii e il contatore dead-letter.
2. Uno stato dedicato per ciascun attore (`ActorState`) con la coda dei messaggi, lo stato di attività (`Running`, `Restarting`, `Terminated`) e il puntatore al worker.
3. La busta del messaggio (`Envelope`) contenente la closure e l'eventuale slot di risposta con `Condvar` per il chiamante di `ask()`.
4. L'handle pubblico condivisibile (`MyActorRef`) che invia messaggi alla mailbox corretta.

**Difficoltà stimata:** 5.0 / 5.0. Budget stimato: 120–150 minuti.
