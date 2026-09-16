# Simulazione 009 — WaitGroup

*Verificata contro `exam_reverse_engineering.md` per fedeltà completa al formato osservato: paragrafo motivante, blocco "API richiesta" in Rust con commenti Italiani sopra ogni metodo, sezione "Requisiti" chiusa dalle due clausole standard, "Suggerimenti implementativi". Dominio non presente nel catalogo storico né nei due campioni reali: un pattern di coordinamento ben noto (Go `sync.WaitGroup`, crossbeam `WaitGroup`) mai ancora tradotto in questo stile.*

---

## WaitGroup

Quando un thread principale avvia un numero di compiti indipendenti su altri thread e deve proseguire solo dopo che **tutti** sono terminati, non è sufficiente un semplice contatore: bisogna anche garantire che ogni compito venga contato esattamente una volta, indipendentemente dal fatto che segnali il proprio completamento esplicitamente o che il proprio "segnaposto" venga semplicemente lasciato uscire dallo scope.

Si scrivano in Rust le strutture che implementano i tratti generici `Token` e `WaitGroup` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Token: Send {
    // Segnala che il compito rappresentato da questo token è terminato,
    // decrementando di uno il contatore del gruppo di attesa. Se questo
    // metodo non viene mai chiamato esplicitamente e il token esce dallo
    // scope, il completamento viene comunque segnalato automaticamente
    // (RAII, tramite Drop). In nessun caso lo stesso token deve poter
    // decrementare il contatore più di una volta.
    fn done(self);
}

pub trait WaitGroup: Clone + Send + Sync {
    // Registra un nuovo compito da attendere, incrementando il contatore
    // interno di uno, e restituisce il token corrispondente.
    fn add(&self) -> impl Token + Send + 'static;

    // Blocca il chiamante, senza consumare cicli di CPU, finché il
    // contatore interno non torna a zero.
    fn wait(&self);

    // Variante con attesa limitata: come `wait`, ma se il contatore non
    // torna a zero entro `timeout` rinuncia e restituisce `false`;
    // restituisce `true` se il contatore è tornato a zero in tempo.
    fn wait_timeout(&self, timeout: Duration) -> bool;
}

pub fn make_wait_group() -> impl WaitGroup {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread (da cui `Clone` sul tratto `WaitGroup` stesso).
- Ogni `add()` incrementa il contatore interno di uno; ogni completamento — tramite `done()` esplicito oppure tramite `Drop` del token — lo decrementa di uno; il contatore non deve mai scendere sotto zero, né essere decrementato due volte per lo stesso token.
- `wait()` e `wait_timeout()` devono sbloccarsi non appena il contatore torna a zero. Se nuovi `add()` vengono registrati dopo che il contatore è già tornato a zero, i thread la cui `wait()` era già ritornata non ne sono influenzati, ma una nuova chiamata a `wait()`/`wait_timeout()` successiva a quei nuovi `add()` deve attendere il nuovo azzeramento.
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con un contatore `usize`, protetto da `Mutex` + `Condvar` dentro un `Arc` clonato sia dal gruppo sia da ogni token.
2. `add()` deve incrementare il contatore *prima* di restituire il token, così che una `wait()` chiamata subito dopo lo veda correttamente non ancora a zero.
3. Il tipo che implementa `Token` deve tenere traccia — con la stessa tecnica `Option<...>` + `.take()` già vista in `ResourcePool` e `TransactionalQueue` — se il proprio completamento è già stato segnalato, in modo che `done(self)` possa segnalarlo una volta e far sì che il successivo `Drop` (che scatta comunque alla fine di `done`, essendo `self` consumato per valore) non lo segnali una seconda volta.
4. `wait()`/`wait_timeout()` attendono con `wait_while`/`wait_timeout_while` finché il contatore non è zero, ricontrollando la condizione ad ogni risveglio.

---

## Meta-commentario

**Perché questo dominio:** è un primitivo di coordinamento estremamente comune nella programmazione di sistema reale (Go lo offre in `sync.WaitGroup`, l'ecosistema Rust lo offre tramite il crate `crossbeam`), concettualmente semplice da spiegare ma non banale da implementare correttamente — esattamente il tipo di primitivo che rientra nello spirito dei problemi già visti senza esserne una variazione superficiale.

**Cosa lo rende non banale:** il tratto `Token` consuma `self` in `done()`, ma quell'istanza *continua a essere distrutta subito dopo* (essendo stata presa per valore) — quindi `Drop` scatta comunque. Uno studente che implementa `Drop` senza tenere conto che `done()` può essere già stato chiamato produrrà un doppio decremento, portando il contatore sotto zero: un bug che, a seconda di come viene gestito l'underflow su `usize`, causa un panico oppure un blocco indefinito di `wait()` (il contatore "avvolge" a un valore enorme e non torna mai a zero) — di nuovo, esattamente il tipo di fallimento silenzioso che un test nascosto è pensato per scoprire.

**Difficoltà stimata:** paragonabile a `TransactionalQueue`; la parte architetturale è più semplice, ma l'insidia del doppio decremento è sottile quanto quelle già viste. Budget consigliato: 75–90 minuti.
