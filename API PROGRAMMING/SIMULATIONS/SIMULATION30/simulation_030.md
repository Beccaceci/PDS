# Simulazione 030 — LoadBalancer

*Dominio nuovo: un bilanciatore di carico con controllo di salute per-backend — non uno, ma N stati indipendenti (uno per backend) da gestire insieme a un livello di selezione sopra di essi. Diverso da `CircuitBreaker` (023) in una dimensione precisa: lì un solo stato globale; qui una mappa di stati indipendenti più la logica per scegliere il migliore candidato tra quelli eleggibili — la combinazione dei due livelli, non nessuno dei due da solo, è la difficoltà.*

---

## LoadBalancer

Un bilanciatore che instrada richieste verso più backend deve tenere traccia della salute di ciascuno indipendentemente dagli altri — un backend che fallisce ripetutamente va escluso temporaneamente, con una singola richiesta di prova concessa dopo un periodo di raffreddamento per verificarne il recupero — e, tra i backend attualmente eleggibili, preferire sempre quello con il carico più basso.

Si scrivano in Rust le strutture che implementano i tratti `RequestHandle` e `LoadBalancer` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait RequestHandle {
    // Segnala l'esito della richiesta completata su questo backend,
    // aggiornandone lo stato di salute di conseguenza. Va invocato al più
    // una volta. Se l'oggetto esce dallo scope senza che report() sia mai
    // stato chiamato, l'esito è considerato un fallimento (ad esempio a
    // causa di un panico durante la gestione della richiesta).
    fn report(self, success: bool);
}

pub trait LoadBalancer: Clone {
    // Registra un nuovo backend identificato da `id`, inizialmente sano.
    // Chiamate ripetute con lo stesso id non hanno ulteriori effetti.
    fn register(&self, id: &str);

    // Seleziona un backend a cui instradare una richiesta, incrementandone
    // il carico, e restituisce un handle per segnalarne l'esito.
    //
    // Se esiste almeno un backend sano, ne sceglie uno tra quelli con il
    // carico più basso. Se nessun backend è sano ma almeno uno è
    // "in prova" — raffreddamento scaduto da quando è diventato non sano,
    // e nessun'altra prova già in corso su di esso — lo seleziona come
    // prova. Se nessun backend è eleggibile in alcun modo, blocca il
    // chiamante, senza consumare cicli di CPU, finché non lo diventa.
    fn route(&self) -> impl RequestHandle;
}

pub fn make_load_balancer(failure_threshold: u32, recovery_after: Duration) -> impl LoadBalancer {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- `report(false)` su un backend sano ne incrementa i fallimenti consecutivi; al raggiungimento di `failure_threshold`, il backend passa a non sano, registrando l'istante della transizione. `report(true)` su un backend sano azzera i suoi fallimenti consecutivi.
- Un backend non sano diventa eleggibile come "in prova" non appena trascorre `recovery_after` dal momento in cui è diventato tale — nessun thread dedicato è necessario: è sufficiente valutarlo ogni volta che `route()` lo esamina.
- `route()` non deve mai assegnare più di una richiesta di prova alla volta allo stesso backend in prova.
- `report(true)` su un backend in prova lo riporta a sano, fallimenti azzerati; `report(false)`, o l'assenza di `report()`, lo riporta a non sano, con il raffreddamento che riparte da questo momento.
- Il carico di un backend va incrementato da `route()` e decrementato esattamente una volta quando il `RequestHandle` corrispondente esce dallo scope, indipendentemente dal fatto che `report()` sia stato chiamato.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ciascun backend con un `enum` — `Sano { carico: usize, fallimenti: u32 }`, `NonSano { da_quando: Instant }`, `InProva { carico: usize }` — dentro una `HashMap<String, StatoBackend>` protetta da `Mutex` + `Condvar` in un `Arc`. `route()` scorre la mappa cercando prima il backend `Sano` con carico minimo; se non ne trova, cerca un `NonSano` il cui raffreddamento è scaduto e lo promuove sul posto a `InProva` — nella stessa operazione, sotto lo stesso lock, così che nessun'altra chiamata concorrente possa scegliere lo stesso backend come prova. Il `RequestHandle` conserva l'`id` del backend scelto e un `Option` che distingue "esito già gestito esplicitamente da `report()`" da "ancora da gestire secondo il default" — la stessa tecnica già vista più volte, qui applicata a distinguere due default *diversi* invece di prevenire un doppio conteggio dello stesso esito.

---

## Meta-commentario

**Perché il default "nessun report = fallimento" non è la stessa situazione già vista in `WaitGroup` (009):** lì, che `done()` venisse chiamato esplicitamente o che il token venisse semplicemente lasciato uscire dallo scope, il significato era identico — "completato". Qui i due percorsi hanno significati opposti: un `report(true)` esplicito e un `Drop` senza `report()` producono conseguenze diverse per la salute del backend. Riconoscerlo è la parte concettuale del problema; implementarlo richiede solo il solito `Option` + `.take()`, ma applicato con una logica di default invertita rispetto a ogni caso precedente della serie.

**Perché la promozione a `InProva` deve avvenire durante la stessa scansione di `route()`, non come passo successivo:** è la stessa lezione di `CircuitBreaker` (023) — lo stato stesso deve fungere da marcatore di esclusività, senza un flag separato — ma qui va applicata dentro un ciclo che esamina più backend, non a un singolo stato piatto: la tentazione naturale è separare "trova un candidato idoneo" da "aggiornalo", il che riaprirebbe esattamente la finestra di corsa che l'atomicità della singola operazione in `CircuitBreaker` era pensata per chiudere.

**Difficoltà stimata:** paragonabile a `JobScheduler` (022) per numero di aspetti che interagiscono (stato multi-chiave, selezione a livelli, doppia via di risoluzione con default divergenti, RAII sul carico) più che per singola difficoltà tecnica. Budget consigliato: 100–130 minuti.
