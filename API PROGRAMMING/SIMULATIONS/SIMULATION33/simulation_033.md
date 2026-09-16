# Simulazione 033 — WorkStealingPool

*Direzione nuova: work-stealing, la tecnica di scheduling dietro Rayon, il `ForkJoinPool` di Java, e lo scheduler di Tokio. Diverso da ogni scheduler precedente della serie (`TaskExecutor`, `JobScheduler`) in una scelta architetturale di fondo: nessuna coda condivisa unica — ogni worker possiede la propria, e il parallelismo vero viene dal non dover mai contendersi un solo lock per operazioni ordinarie. La difficoltà: garantire che due "ladri" non possano mai rubare lo stesso ultimo task, e segnalare "è arrivato lavoro da qualche parte" senza legare quel segnale a nessuna delle code stesse.*

---

## WorkStealingPool

In uno scheduler a più worker, far condividere a tutti un'unica coda centrale crea un collo di bottiglia: ogni worker deve contendersi lo stesso lock anche per operazioni che non hanno nulla a che fare con gli altri. Il work-stealing risolve il problema dando a ciascun worker una coda propria, su cui lavora liberamente senza contesa; solo quando la propria coda è vuota, un worker tenta di "rubare" un task dalla coda di un altro — dall'estremità opposta rispetto a quella che il proprietario usa, per motivi di cache locality e per ridurre la frequenza dei furti.

Si scrivano in Rust le strutture che implementano i tratti `WorkQueue<T>` e `WorkStealingPool<T>` definiti di seguito.

### API richiesta

```rust
pub trait WorkQueue<T: Send> {
    // Il proprietario accoda un task alla propria estremità locale.
    fn push(&self, task: T);

    // Il proprietario preleva il task più recente dalla propria estremità
    // locale (LIFO). None se la coda è vuota.
    fn pop(&self) -> Option<T>;

    // Un altro worker tenta di rubare il task meno recente da questa
    // coda, dall'estremità opposta rispetto a push/pop (FIFO rispetto al
    // proprietario). None se la coda è vuota.
    fn steal(&self) -> Option<T>;
}

pub trait WorkStealingPool<T: Send>: Clone {
    fn worker_count(&self) -> usize;

    // Restituisce la coda locale del worker worker_id (0..worker_count()).
    fn queue(&self, worker_id: usize) -> impl WorkQueue<T>;

    // Tenta di rubare un task da una qualunque coda diversa da own_id che
    // ne abbia disponibile in questo momento, restituendo l'indice del
    // worker derubato insieme al task. None se nessuna coda diversa da
    // own_id ha task disponibili al momento del tentativo. L'ordine in cui
    // le code vengono tentate deve variare tra chiamate successive, per
    // non concentrare sistematicamente la contesa sulla stessa coda.
    fn steal_any(&self, own_id: usize) -> Option<(usize, T)>;

    // Blocca il chiamante, senza consumare cicli di CPU, finché push()
    // non viene chiamato su una qualunque coda del pool dopo l'inizio di
    // questa chiamata.
    fn wait_for_work(&self);
}

pub fn make_work_stealing_pool<T: Send>(worker_count: usize) -> impl WorkStealingPool<T> {
    ...
}
```

### Requisiti

- Ogni coda è indipendente dalle altre: operare su una non deve mai bloccare l'accesso a una diversa.
- Se una coda contiene esattamente un task e il proprietario chiama `pop()` mentre un altro worker chiama `steal()` su di essa concorrentemente, esattamente una delle due chiamate deve ottenere il task, l'altra deve ricevere `None` — mai entrambe lo stesso task, mai il task perso. Lo stesso vale per due `steal()` concorrenti da worker diversi sulla stessa coda con un solo task disponibile.
- `steal_any` deve variare l'ordine di tentativo tra chiamate successive.
- `wait_for_work` deve sbloccarsi per un `push()` su una qualunque coda, non solo quella del chiamante.
- Thread-safe, condivisibile (`Clone` sul tratto `WorkStealingPool`).
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ogni coda come un `Mutex<VecDeque<T>>` indipendente (`push`/`pop` operano sul retro, `steal` sul fronte), raccolte in un `Vec` condiviso dentro un `Arc`. Per `wait_for_work`, **non** riusare il lock di una coda specifica come base del `Condvar`: un `Condvar` in Rust va sempre atteso tramite il `MutexGuard` di un unico `Mutex` coerente nel tempo, e qui il segnale "è arrivato lavoro" deve riguardare *tutte* le code insieme, non una in particolare. Usare invece un piccolo `Mutex<()>` dedicato esclusivamente alla segnalazione, accoppiato al proprio `Condvar`: `push()`, dopo aver inserito il task nella propria coda sotto il proprio lock, acquisisce brevemente anche questo mutex dedicato per notificare (`notify_all`). Per la rotazione dell'ordine di tentativo in `steal_any`, un contatore protetto dallo stesso mutex dedicato, incrementato ad ogni chiamata e usato come punto di partenza, è sufficiente — non serve una randomizzazione vera.

---

## Meta-commentario

**Perché "nessuna coda condivisa" è la scelta architetturale, non un dettaglio prestazionale:** in `TaskExecutor` e `JobScheduler`, un solo `Mutex` proteggeva l'intera coda dei lavori, ed era una scelta corretta perché un solo punto di decisione centralizzata (si veda il confronto tra `JobScheduler` e `MultiResourceManager` nella simulazione 022) semplificava altri problemi. Qui l'assenza di un lock condiviso *è* il punto: N worker devono poter operare sulla propria coda in parallelo, senza mai attendersi a vicenda per operazioni ordinarie — introdurre anche un solo lock condiviso per `push`/`pop`/`steal` vanificherebbe lo scopo dell'esercizio, anche se producesse codice corretto.

**Perché un `Condvar` non legato ai lock delle code è un idioma nuovo nella serie, non un dettaglio implementativo:** ogni problema precedente accoppiava il `Condvar` allo stesso `Mutex` che protegge il dato rilevante — qui non esiste un singolo dato "rilevante" a cui accoppiarlo, perché il segnale riguarda l'unione di N stati indipendenti. Usare un `Mutex<()>` puramente come veicolo per un `Condvar`, disaccoppiato da qualunque dato reale, è una tecnica reale e non ovvia: il `Mutex` qui non protegge nulla — esiste solo perché `Condvar::wait` lo richiede.

**Perché la correttezza del singolo furto non è, di per sé, la parte difficile:** un `Mutex<VecDeque<T>>` per coda rende banale evitare che due chiamate rubino lo stesso ultimo elemento — chi ottiene il lock per primo vince, senza bisogno di alcuna logica aggiuntiva. La vera difficoltà è tutta architetturale: quante strutture di sincronizzazione servono (N code indipendenti più un canale di segnalazione separato), e perché nessuna di esse può fare a meno dell'altra.

**Difficoltà stimata:** eccede probabilmente il tempo di un singolo appello, per il numero di componenti indipendenti da coordinare correttamente insieme. Budget stimato: 100–130 minuti.
