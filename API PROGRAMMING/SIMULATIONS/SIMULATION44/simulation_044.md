# Simulazione 045 — InternPool (capstone)

*Dominio nuovo: un pool di deduplicazione (*interning*) — lo stesso principio dietro l'interning delle stringhe in Java/Python, o il crate `string-interner` in Rust. Generalizza `Weak<T>` da un singolo genitore/figlio (`Publisher`/`ChildHandle`, 023) a una mappa di infinite chiavi indipendenti, ciascuna con il proprio ciclo di vita — e introduce una corsa specifica, nota e reale, che qualunque cache basata su riferimenti deboli deve risolvere esplicitamente: la "corsa alla resurrezione" tra l'ultimo rilascio di un valore e una richiesta concorrente dello stesso valore.*

---

## InternPool

Un pool che deduplica valori costosi da costruire (parsing, compilazione, allocazioni grandi) associati a una chiave dovrebbe costruire ciascun valore una sola volta e condividerlo tra tutti i richiedenti concorrenti — ma senza tenerlo in vita per sempre: una volta che l'ultimo utilizzatore lo rilascia, la voce va rimossa, in modo che una richiesta successiva per la stessa chiave lo ricostruisca da capo invece di trovare un valore "morto" ma ancora presente.

Si scrivano in Rust le strutture che implementano i tratti `Interned<V>` e `InternPool<K, V>` definiti di seguito.

### API richiesta

```rust
use std::hash::Hash;

pub trait Interned<V: Send> {
    fn get(&self) -> &V;
}

pub trait InternPool<K: Eq + Hash + Clone + Send, V: Send>: Clone {
    // Restituisce un handle condiviso al valore associato a key. Se
    // esiste già un valore vivo per quella chiave (almeno un altro
    // Interned<V> non ancora rilasciato), restituisce un nuovo handle che
    // condivide lo STESSO valore sottostante, senza invocare create.
    // Altrimenti invoca create per costruirne uno nuovo — mai mentre è
    // mantenuto un lock condiviso, per non serializzare richieste su
    // chiavi diverse dietro una create lenta — e lo registra nel pool.
    fn intern(&self, key: K, create: impl FnOnce() -> V) -> impl Interned<V>;

    // Numero di chiavi attualmente vive nel pool (con almeno un handle
    // non ancora rilasciato). Pensato per l'ispezione nei test.
    fn len(&self) -> usize;
}

pub fn make_intern_pool<K: Eq + Hash + Clone + Send, V: Send>() -> impl InternPool<K, V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- Due chiamate a `intern()` con la stessa chiave, entrambe eseguite mentre almeno un handle per quella chiave è già vivo, devono condividere lo stesso valore sottostante — `create` invocata una sola volta complessivamente in quell'intervallo.
- Quando l'ultimo `Interned<V>` per una data chiave viene rilasciato, la voce deve essere rimossa dal pool — osservabile tramite `len()` che diminuisce — in modo che una `intern()` successiva per quella stessa chiave invochi di nuovo `create`, costruendo un valore nuovo e indipendente.
- Una `intern(key, ...)` concorrente al rilascio dell'ultimo handle esistente per quella stessa `key` deve risolversi in uno dei due modi in maniera coerente: o ottiene un handle condiviso al valore che stava per essere rimosso (se il proprio tentativo la precede effettivamente), oppure ne crea correttamente uno nuovo (se il rilascio la precede) — mai un handle "condiviso" con un valore già rimosso dal pool ma che nessun altro riferimento raggiunge più, né una voce fantasma contata da `len()` senza alcun handle vivo corrispondente.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con una mappa `K -> Weak<V>` (avvolgendo ogni valore in un `Arc<V>` di cui la mappa conserva solo un riferimento debole), protetta da `Mutex` dentro un `Arc`. `intern(key, create)`: sotto lock, se la mappa ha un `Weak` per `key` e il suo `upgrade()` riesce, clona quell'`Arc` e restituisce un `Interned` che lo avvolge, senza chiamare `create`; altrimenti rilascia il lock, invoca `create`, poi riacquisisce il lock e **ricontrolla** se nel frattempo un'altra chiamata concorrente ha già inserito un valore vivo per la stessa chiave (verifica a doppio controllo) — in tal caso usa quello già presente, scartando il proprio; altrimenti inserisce il proprio nuovo `Arc` (come `Weak`) nella mappa. Il `Drop` di `Interned<V>` deve, sotto lo **stesso** lock della mappa, verificare se il proprio è l'ultimo riferimento forte rimasto (`Arc::strong_count` uguale a 1, contando ancora il proprio) — in tal caso rimuovere la voce dalla mappa in quella stessa sezione critica, prima che il proprio `Arc` venga effettivamente rilasciato: solo così nessuna `intern()` concorrente può osservare una finestra in cui il valore è già logicamente morto ma ancora trovabile.

---

## Meta-commentario

**Perché il controllo "sono l'ultimo?" e la rimozione dalla mappa devono avvenire sotto lo stesso lock, in quest'ordine preciso:** se `Drop` controllasse `strong_count` senza tenere il lock della mappa, un `intern()` concorrente potrebbe, nella finestra tra quel controllo e l'effettiva rimozione dalla mappa, clonare un `Arc` che sta per diventare l'unico riferimento a un valore che il `Drop` originario sta comunque per rimuovere dalla mappa un istante dopo — lasciando quell'`Arc` clonato "orfano": ancora vivo in memoria (nessun crash, nessun panico), ma irraggiungibile da qualunque `intern()` futuro, che non lo troverà più nella mappa e ne costruirà uno del tutto nuovo e indipendente. Non un bug che si manifesta con un errore, ma con una violazione silenziosa della deduplicazione promessa.

**Perché la doppia verifica dopo `create` è necessaria, e non ridondante:** rilasciare il lock durante `create` (obbligatorio, per non serializzare chiavi indipendenti dietro una costruzione lenta) apre una finestra in cui un'altra chiamata a `intern()` per la stessa chiave potrebbe completarsi per intero nel frattempo — senza il doppio controllo al rientro, due valori distinti finirebbero entrambi nella mappa per la stessa chiave in rapida successione, l'uno silenziosamente sovrascrivendo l'altro, e la garanzia "un solo `create` per episodio di richiesta condivisa" si romperebbe silenziosamente.

**Perché questo generalizza `Publisher`/`ChildHandle` (023) invece di ripeterlo:** lì un solo `Weak` collegava un singolo genitore a un singolo figlio, senza alcuna corsa da risolvere (il genitore veniva distrutto esplicitamente da chi lo possedeva, non da un conteggio di riferimenti condiviso). Qui ogni chiave della mappa è, in un certo senso, il proprio "genitore e figlio insieme" — la voce vive esattamente finché esiste almeno un riferimento forte, e la sua rimozione deve essere innescata dall'ultimo `Drop`, non da un possessore esterno — introducendo la corsa alla resurrezione che una relazione fissa uno-a-uno non presenta mai.

**Difficoltà stimata:** tra le più alte della serie per densità concettuale in poco codice — la struttura dati è minima, ma l'ordine esatto delle operazioni è l'intero problema. Budget stimato: 100–130 minuti.
