# Simulazione 016 — BatchPool

*Chiude il prossimo vuoto prioritario in `coverage_plan.md` (asse C: cardinalità delle risorse per handle). Costruita deliberatamente in contrasto con `MultiResourceManager` (simulazione 011): stesso tema di superficie — un chiamante ottiene più risorse in una volta sola — ma senza alcun rischio di stallo, perché qui le risorse sono intercambiabili. È tanto un esercizio di riconoscimento di quando la tecnica già imparata *non* serve, quanto uno di quando serve.*

---

## BatchPool

Un sistema che elabora lavori a lotti (ad esempio un motore di rendering che distribuisce i fotogrammi su un certo numero di buffer di calcolo) a volte ha bisogno di più unità della stessa risorsa contemporaneamente — non unità specifiche e identificate, ma un numero qualsiasi di unità equivalenti tra loro, prese da un insieme comune.

Si scrivano in Rust le strutture che implementano i tratti generici `ResourceBatch<T: Send>` e `BatchPool<T: Send>` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait ResourceBatch<T: Send> {
    // Accesso agli elementi ottenuti in prestito con questa richiesta, in
    // un ordine non specificato.
    fn items(&self) -> &[T];
}

pub trait BatchPool<T: Send> {
    // Numero totale di elementi gestiti dal pool.
    fn capacity(&self) -> usize;

    // Preleva esattamente `count` elementi dal pool — un numero qualsiasi
    // tra quelli disponibili, senza che la loro identità individuale sia
    // rilevante — bloccando il chiamante, senza consumare cicli di CPU,
    // finché non ce ne sono almeno `count` disponibili simultaneamente.
    fn acquire_batch(&self, count: usize) -> impl ResourceBatch<T>;

    // Variante con attesa limitata: come `acquire_batch`, ma se non
    // riesce ad ottenere `count` elementi entro `timeout` rinuncia e
    // restituisce `None`. L'attesa non deve consumare cicli di CPU.
    fn acquire_batch_timeout(&self, count: usize, timeout: Duration) -> Option<impl ResourceBatch<T>>;
}

pub fn make_batch_pool<T: Send>(items: Vec<T>) -> impl BatchPool<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread.
- `acquire_batch(count)` deve restituire esattamente `count` elementi presi dal pool, senza vincoli sulla loro identità: qualunque sottoinsieme di `count` elementi correntemente disponibili va bene.
- Si assuma che nessun chiamante richieda mai un `count` maggiore di `capacity()`.
- Quando l'oggetto che implementa `ResourceBatch<T>` esce dallo scope, tutti gli elementi che conteneva tornano disponibili contemporaneamente (RAII, tramite il tratto `Drop`).
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con gli elementi disponibili (ad esempio un `Vec<T>`), protetto da `Mutex` + `Condvar` dentro un `Arc`; `acquire_batch` attende con `wait_while` finché il numero di elementi disponibili è inferiore a `count`, poi ne preleva esattamente `count` in un'unica operazione sotto lo stesso lock; il `Drop` di `ResourceBatch<T>` restituisce tutti gli elementi posseduti in un'unica operazione, e notifica dopo aver terminato.

---

## Meta-commentario

**Il contrasto con `MultiResourceManager` (simulazione 011) è il punto centrale dell'esercizio, non un dettaglio:** lì, il chiamante specificava *quali* risorse voleva (`ids: &[usize]`), il che significa che due chiamate concorrenti potevano competere per le stesse identità specifiche in ordine diverso — da cui il rischio di stallo, e la necessità di un ordine totale di acquisizione. Qui, il chiamante specifica solo *quante* risorse vuole, non quali: non esiste alcuna nozione di "la stessa risorsa richiesta da due thread diversi" da cui possa nascere uno stallo — una singola condizione (`disponibili >= count`) valutata sotto un solo lock è sufficiente e corretta, esattamente come in `ResourcePool`, solo con un contatore invece di un singolo elemento.

**La trappola, in entrambe le direzioni:** chi ha appena risolto `MultiResourceManager` ha un incentivo naturale a reintrodurre qui un ordinamento o una logica di acquisizione "prova-e-rilascia" per prudenza — codice che compila, supera i test, ma introduce complessità superflua che il criterio di correzione osservato nei campioni reali penalizza esplicitamente quanto un errore vero e proprio. Viceversa, chi affrontasse `MultiResourceManager` *dopo* aver risolto questo (nell'ordine sbagliato) rischierebbe il contrario: applicare qui la stessa idea "controlla-e-basta" a un problema dove invece serve davvero un ordine totale.

**Difficoltà stimata:** l'implementazione è quasi identica a `ResourcePool`; la vera valutazione è se il codice resta appropriatamente semplice o si complica senza motivo. Budget consigliato: 45–60 minuti — deliberatamente più breve delle ultime simulazioni, perché qui la sinteticità della soluzione è essa stessa parte della risposta corretta.
