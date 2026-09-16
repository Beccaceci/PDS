> **Nota di aggiornamento:** confermato che Tokio/async non rientra nell'ambito dell'esame — questa simulazione va considerata invalidata come previsione. Resta come esercizio sul "Drop non può essere async", ma non come ipotesi sul prossimo esame.

# Simulazione 010 — AsyncResourcePool

*Prima simulazione della serie che lascia il modello puramente sincrono (`std::thread`/`std::sync`) per quello asincrono (Tokio) — coperto dal corso ma mai testato in nessuno dei due campioni reali raccolti finora. Confidenza volutamente più bassa delle precedenti: qui non sto solo cambiando dominio, sto cambiando modello di programmazione, ed è un salto che nessuna evidenza diretta conferma. Se il prossimo esame reale resta sincrono, questa simulazione va scartata, non ricalibrata.*

---

## AsyncResourcePool

Nei server che gestiscono molte connessioni concorrenti tramite un runtime asincrono, un pool di risorse riutilizzabili (come connessioni a un database) non deve mai far bloccare l'intero thread del runtime mentre un task attende che una risorsa si liberi — farlo impedirebbe a tutti gli altri task in esecuzione sullo stesso thread di progredire, vanificando il vantaggio della programmazione asincrona.

Si scriva in Rust, usando Tokio, una struttura che implementi il tratto generico `AsyncResourcePool<T: Send>`, che gestisce un insieme fisso di elementi riutilizzabili di tipo `T` e li concede in prestito esclusivo ai task che ne fanno richiesta, in modo interamente asincrono.

### API richiesta

```rust
use std::time::Duration;

pub trait AsyncResource<T: Send> {
    fn get(&self) -> &T;
}

pub trait AsyncResourcePool<T: Send> {
    // Numero totale di elementi gestiti dal pool.
    fn capacity(&self) -> usize;

    // Attende in modo asincrono, senza bloccare il thread del runtime né
    // consumare cicli di CPU, finché un elemento non è disponibile, quindi
    // lo concede in prestito esclusivo al chiamante.
    async fn acquire(&self) -> impl AsyncResource<T>;

    // Variante con attesa limitata: come acquire, ma se non ottiene un
    // elemento entro timeout rinuncia e restituisce None. L'attesa non deve
    // bloccare il thread del runtime.
    async fn acquire_timeout(&self, timeout: Duration) -> Option<impl AsyncResource<T>>;
}

pub fn make_async_resource_pool<T: Send>(items: Vec<T>) -> impl AsyncResourcePool<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più task asincroni concorrenti, anche eseguiti su thread diversi dello stesso runtime multi-thread.
- Ogni elemento deve essere concesso ad al più un task alla volta.
- Quando l'oggetto che implementa `AsyncResource<T>` esce dallo scope, l'elemento deve tornare disponibile nel pool (RAII, tramite il tratto `Drop`) — **questo rilascio non deve mai richiedere `.await` né bloccare il thread del runtime**: deve essere immediato e completamente sincrono.
- Nessuna attesa attiva, e nessun uso di primitive di blocco sincrone che impedirebbero ad altri task sullo stesso thread di progredire mentre `acquire`/`acquire_timeout` sono in sospeso.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`, con `#[tokio::test]`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. Usare `tokio::sync::Semaphore` per contare gli elementi disponibili: `acquire()` può ottenere un permesso in modo asincrono senza bloccare il thread del runtime.
2. Conservare gli elementi effettivamente disponibili in una struttura protetta da un `std::sync::Mutex` **sincrono**, non `tokio::sync::Mutex` — l'accesso è sempre breve e non attraversa mai un punto `.await`, quindi un mutex sincrono è sicuro qui, ed è l'unico tipo di mutex utilizzabile anche dentro `Drop`.
3. L'oggetto che implementa `AsyncResource<T>` deve tenere sia l'elemento (in un `Option<T>`) sia il permesso ottenuto dal semaforo; il suo `Drop` estrae l'elemento con `.take()` e lo reinserisce nella struttura sincrona — nessuna di queste operazioni richiede `.await`.
4. Evitare `tokio::sync::Mutex` per qualunque stato che debba essere toccato da `Drop`: essendo un tipo asincrono, il suo lock richiede `.await` (o `blocking_lock()`, sconsigliato e potenzialmente causa di panico se chiamato da dentro un task Tokio), e `Drop::drop` non può mai essere una funzione `async`.

---

## Meta-commentario

**Perché questo dominio:** è la traduzione più diretta possibile di `ResourcePool`, il campione reale meglio documentato, nel modello asincrono — permette di isolare esattamente cosa cambia passando da `std::sync` a Tokio, senza introdurre anche un nuovo dominio concettuale.

**L'insidia centrale, ed è deliberata:** `Drop::drop` in Rust è, e resterà sempre, una funzione sincrona — non esiste un "async Drop" nel linguaggio. Qualunque risorsa che uno studente scelga di proteggere con un tipo *asincrono* (`tokio::sync::Mutex`, un canale che richiede `.send().await`) diventa impossibile da rilasciare correttamente dentro `Drop`. La soluzione corretta richiede di riconoscere in anticipo quali parti dello stato condiviso saranno toccate da `Drop`, e di proteggerle con primitive **sincrone** fin dall'inizio — esattamente il tipo di scelta architetturale a monte che, nei campioni reali, viene penalizzata pesantemente se sbagliata (si veda il commento sulla "struttura dati... non ti consente di risolvere l'esercizio" nel feedback di correzione di `ResourcePool`).

**Ulteriore nota tecnica per chi affronta questo problema:** una `async fn` dichiarata in un tratto non garantisce automaticamente che la `Future` restituita sia `Send` — se i test nascosti usano `tokio::spawn` per invocare `acquire()` da più task concorrenti su un runtime multi-thread, potrebbe essere necessario vincolare esplicitamente il tipo restituito (`-> impl Future<Output = ...> + Send` invece della sola sintassi `async fn`, o l'uso di un attributo come quello fornito dal crate `trait-variant`).

**Difficoltà stimata:** difficile calibrare senza un campione reale asincrono di riferimento; per struttura è paragonabile a `ResourcePool`, ma il salto concettuale (sincrono → asincrono, e la trappola di `Drop`) probabilmente la renderebbe più lunga per chi non ha già interiorizzato la distinzione. Budget stimato: 90–120 minuti.
