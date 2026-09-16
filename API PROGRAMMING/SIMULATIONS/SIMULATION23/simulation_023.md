# Simulazione 023 — CircuitBreaker

*Dominio nuovo: non più un contenitore di dati o un coordinatore di thread, ma un pattern di resilienza (il *circuit breaker*, comune nei client verso servizi remoti inaffidabili). La difficoltà è quasi interamente nella progettazione dell'`enum` di stato: farlo bene significa che lo stato stesso funge da marcatore di "prova in corso", senza bisogno di un flag separato che rischierebbe di disallinearsi da esso.*

---

## CircuitBreaker

Un client verso un servizio remoto inaffidabile non dovrebbe continuare a tentare chiamate destinate a fallire quando il servizio è chiaramente non disponibile: dopo un certo numero di fallimenti consecutivi, è meglio fallire immediatamente, senza nemmeno tentare la chiamata, per un periodo di raffreddamento — per poi lasciar passare, con cautela, una singola chiamata di prova per verificare se il servizio è tornato disponibile.

Si scriva in Rust una struttura che implementi il tratto generico `CircuitBreaker<T: Send>` definito di seguito.

### API richiesta

```rust
use std::time::Duration;

pub enum CallError {
    // Il circuito è aperto (o una prova è già in corso): la chiamata non
    // è stata nemmeno tentata.
    CircuitOpen,
}

pub trait CircuitBreaker<T: Send>: Clone {
    // Se il circuito è chiuso, esegue `call` ed aggiorna lo stato in base
    // al suo esito (Ok = successo, Err = fallimento), restituendone il
    // risultato. Se il circuito è aperto e non è ancora trascorso il
    // periodo di raffreddamento, restituisce immediatamente
    // CallError::CircuitOpen senza eseguire `call`. Se il periodo di
    // raffreddamento è trascorso, esattamente una chiamata concorrente
    // viene lasciata passare come prova (le altre ricevono
    // CallError::CircuitOpen senza essere eseguite); se la prova ha
    // successo il circuito torna chiuso, altrimenti torna aperto e il
    // raffreddamento riparte da questo momento. Non blocca mai il
    // chiamante.
    fn call(&self, call: impl FnOnce() -> Result<T, ()>) -> Result<T, CallError>;
}

pub fn make_circuit_breaker<T: Send>(
    failure_threshold: u32,
    cooldown: Duration,
) -> impl CircuitBreaker<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- A circuito chiuso, chiamate concorrenti a `call()` devono poter eseguire le rispettive `call` in parallelo, senza serializzarsi a vicenda: solo l'aggiornamento del contatore di fallimenti consecutivi va protetto, non l'esecuzione di `call` stessa.
- Il contatore di fallimenti consecutivi si azzera ad ogni successo; al raggiungimento di `failure_threshold` fallimenti consecutivi, il circuito passa allo stato aperto.
- La transizione da aperto a "prova in corso" non richiede alcun thread dedicato: è sufficiente che ogni chiamata a `call()`, trovando il circuito aperto, verifichi se `cooldown` è trascorso dall'apertura.
- Se due o più chiamate concorrenti trovano il circuito aperto con il raffreddamento appena scaduto, al più una di esse deve eseguire la prova; le altre devono ricevere `CircuitOpen` senza eseguire nulla.
- Nessuna attesa attiva (`call()` non blocca mai).
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato con un `enum` a tre varianti — `Chiuso { fallimenti_consecutivi: u32 }`, `Aperto { aperto_dal: Instant }`, `Semiaperto` — protetto da un semplice `Mutex` (nessun `Condvar`: nulla attende mai) dentro un `Arc`. Il solo fatto che lo stato sia `Semiaperto` è già il marcatore di "prova in corso": la transizione a `Semiaperto` avviene sotto lock, in un'unica operazione atomica insieme alla decisione "questa chiamata è la prova", per cui non serve nessun campo booleano aggiuntivo per tracciarlo separatamente. `call()` decide sotto lock cosa fare, rilascia il lock, esegue eventualmente la chiusura fornita, poi riacquisisce il lock solo per registrare l'esito.

---

## Meta-commentario

**Perché `Semiaperto` non ha bisogno di un campo "prova in corso":** un'implementazione ingenua potrebbe essere tentata di aggiungere un flag `bool prova_in_corso` separato dallo stato — ma questo introduce la possibilità che i due si disallineino (uno stato `Semiaperto` con il flag falso, o viceversa), un bug di rappresentazione impossibile se la sola esistenza della variante `Semiaperto` è già sufficiente a significare "prova in corso": finché lo stato resta `Semiaperto`, nessun'altra chiamata può reclamare la prova, perché il codice che decide "sono io la prova" è lo stesso codice che effettua la transizione a `Semiaperto`, sotto lo stesso lock.

**Perché l'esecuzione di `call` fuori dal lock è essenziale anche a circuito chiuso, non solo durante la prova:** è la stessa lezione di `SingleFlightCache` (015), applicata qui a un caso ancora più stringente — a circuito chiuso, *nessuna* chiamata deve mai essere ritardata da un'altra in corso, non solo quelle sulla stessa "chiave" (qui non esistono chiavi, tutte le chiamate condividono lo stesso circuito).

**Difficoltà stimata:** contenuta nel tempo di un esame reale, a differenza delle simulazioni 027/028 — la difficoltà è interamente concettuale (progettare l'enum giusto), non nel volume di codice. Budget consigliato: 60–75 minuti.
