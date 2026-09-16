# Simulazione 047 — Signal (capstone)

*Due tratti, come richiesto — e un dominio completamente diverso da `TaskGraph` (046): non un grafo di dipendenze, ma un sistema di segnali/slot (come `Boost.Signals2` in C++, o i segnali di Qt), dove il problema classico e reale è che uno slot deve potersi disconnettere da solo — o disconnetterne un altro — proprio mentre viene invocato dall'interno della stessa `emit()` che lo sta chiamando, senza corrompere l'iterazione né bloccarsi contro se stesso.*

---

## Signal

Un sistema di notifiche in cui più osservatori (slot) si registrano per essere invocati a ogni evento deve gestire correttamente un caso che può verificarsi durante la notifica: uno slot, mentre gestisce l’evento, può decidere di non voler più ricevere notifiche in futuro e quindi disconnettersi autonomamente. In alternativa, può invalidare un altro slot che è ancora in attesa di essere invocato nella stessa tornata di notifiche. In entrambi i casi, la modifica degli slot non deve compromettere né interrompere la tornata di notifiche attualmente in corso.

Si scrivano in Rust le strutture che implementano i tratti `Connection` e `Signal<T>` definiti di seguito.

### API richiesta

```rust
pub trait Connection {
    // Disconnette questo slot. Può essere chiamato in qualunque momento,
    // inclusa dall'interno del callback dello slot stesso mentre viene
    // invocato da emit() — sia per disconnettere se stesso, sia (tramite
    // una Connection ottenuta altrove) per disconnetterne un altro. Se
    // già disconnesso, non ha ulteriori effetti. Non blocca mai.
    fn disconnect(&self);
}

pub trait Signal<T: Clone + Send>: Clone + Send + Sync {
    // Connette un nuovo slot, invocato ad ogni emit() futura finché non
    // viene disconnesso. Restituisce un handle per disconnetterlo.
    fn connect(&self, slot: impl Fn(&T) + Send + Sync + 'static) -> impl Connection + 'static;

    // Invoca, nell'ordine di connessione, ogni slot connesso al momento
    // in cui emit() inizia. Uno slot disconnesso — da fuori, o da un
    // altro slot precedente nella stessa emit(), o da se stesso durante
    // la propria stessa invocazione — non deve essere invocato da questa
    // emit() se la disconnessione avviene prima che emit() lo raggiunga,
    // anche se quello slot era presente al momento in cui emit() è
    // iniziata.
    fn emit(&self, value: T);
}

pub fn make_signal<T: Clone + Send + 'static>() -> impl Signal<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone + Send + Sync`).
- `emit()` deve invocare i soli slot connessi al proprio inizio, nell'ordine di connessione — ma deve anche rispettare, per ciascuno di essi, un'eventuale disconnessione avvenuta *durante* la stessa `emit()`, prima che essa lo raggiunga.
- Uno slot che chiama `disconnect()` su se stesso o su un altro, dall'interno della propria invocazione, non deve causare panico, blocco, né alcuna forma di comportamento indefinito nell'`emit()` in corso.
- `connect()` chiamato mentre un'`emit()` è già in corso non deve essere invocato da quella stessa `emit()`, ma deve essere pienamente valido e osservabile da qualunque `emit()` successiva.
- `disconnect()` non deve mai bloccare, né deve bloccare o essere bloccato da un'`emit()` in corso.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

`emit()` non può mantenere il lock dell'elenco dei collegamenti mentre invoca ciascuno slot — altrimenti un `disconnect()` chiamato da dentro uno slot, che ha bisogno di modificare quello stesso elenco, si troverbe a contendere un lock già tenuto dal thread che lo sta eseguendo.

---

## Meta-commentario

**Le tre cose che l'API non dice esplicitamente, e che vanno scoperte progettando:** primo, `emit()` ha bisogno di una fotografia dei collegamenti presenti al proprio inizio, non dell'elenco "vivo" — altrimenti l'iterazione stessa diventa insicura nel momento in cui qualcosa la modifica da sotto. Secondo, quella fotografia non basta da sola: serve comunque un modo per sapere, slot per slot, se nel frattempo è stato disconnesso — cioè uno stato condiviso *tra* la fotografia e l'elenco vivo, non duplicato nei due. Terzo, quello stato condiviso deve essere leggibile e scrivibile senza il lock dell'elenco, altrimenti si ricade nello stesso problema che la fotografia doveva risolvere.

**Perché questo è un problema reale, non un caso limite inventato:** è esattamente il motivo per cui `Boost.Signals2` e i framework a eventi di Qt documentano esplicitamente il comportamento di una disconnessione durante l'emissione — è una delle prime cose che va storta in un sistema a callback ingenuo, tipicamente con un panico da doppio prestito o da modifica di una collezione durante la sua iterazione, in un linguaggio che lo permettesse senza controlli.

**In cosa differisce dalla propagazione di `TaskGraph` (046):** lì la struttura da scoprire era una relazione (chi dipende da chi) mai dichiarata esplicitamente. Qui non manca una relazione, manca un *livello di indirezione*: senza di esso, "lo slot che sto per invocare" e "la sua possibilità di essere già invalidato nel frattempo" finiscono per essere la stessa cosa, quando devono restare deliberatamente separati.

**Difficoltà stimata:** paragonabile a `TaskGraph`, con una superficie di codice più contenuta ma un'insidia di re-ingresso specifica e facile da sottovalutare finché non si scrive il test giusto per scovarla. Budget stimato: 90–120 minuti.
