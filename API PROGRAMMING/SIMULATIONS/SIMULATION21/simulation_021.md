# Simulazione 021 — TtlCache (capstone)

*Prima delle due simulazioni "capstone" richieste: non un nuovo asse isolato, ma la fusione deliberata di tre meccanismi già padroneggiati singolarmente — scadenza temporale (`DelayQueue`, 008), deduplicazione del calcolo concorrente (`SingleFlightCache`, 015), espulsione per capacità (nuovo) — in un'unica struttura dati dove devono coesistere e interagire correttamente. La difficoltà non sta in nessuno dei tre singolarmente, ma nel fatto che una decisione presa per uno di essi ha conseguenze sugli altri due.*

---

## TtlCache

Una cache di risposte (ad esempio per un resolver DNS, o per risultati di query costose) deve: evitare di ricalcolare un valore già noto e ancora valido; evitare che più richieste concorrenti per la stessa chiave mancante scatenino calcoli duplicati; scadere automaticamente le voci più vecchie di un termine dato; e, quando la memoria disponibile per la cache è esaurita, fare spazio rimuovendo prima le voci già scadute e, se non bastasse, quella usata meno di recente.

Si scriva in Rust una struttura che implementi il tratto generico `TtlCache<K, V>` definito di seguito.

### API richiesta

```rust
use std::time::Duration;
use std::hash::Hash;

pub trait TtlCache<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone {
    // Restituisce il valore associato a `key`.
    //
    // - Se `key` è presente in cache e non è scaduta (sono trascorsi meno
    //   di `ttl` dal termine con cui è stata inserita), la restituisce
    //   immediatamente, senza bloccare, e la marca come "usata ora" ai
    //   fini dell'espulsione LRU.
    // - Se `key` è assente, o presente ma scaduta, e nessun altro thread
    //   sta già calcolando un nuovo valore per la stessa chiave, il
    //   chiamante stesso diventa responsabile del calcolo: invoca
    //   `compute` SENZA mantenere alcun lock durante la sua esecuzione,
    //   poi memorizza il risultato con un nuovo termine di scadenza pari a
    //   `ttl` a partire da questo momento.
    // - Se invece un altro thread sta già calcolando un nuovo valore per
    //   la stessa chiave, attende senza consumare cicli di CPU che quel
    //   calcolo termini, quindi restituisce il valore da esso prodotto
    //   (con il `ttl` fornito da *quel* thread, non necessariamente il
    //   proprio).
    //
    // Se, al momento di memorizzare un nuovo valore calcolato, la cache ha
    // già raggiunto `capacity` voci (contando solo quelle effettivamente
    // presenti, non i calcoli in corso), viene fatto spazio rimuovendo
    // prima una voce già scaduta, se ne esiste almeno una; altrimenti
    // viene rimossa la voce non scaduta usata meno di recente.
    fn get_or_compute(&self, key: K, ttl: Duration, compute: impl FnOnce() -> V) -> V;

    // Numero di voci effettivamente presenti in cache in questo momento
    // (non conta i calcoli in corso non ancora completati).
    fn len(&self) -> usize;
}

pub fn make_ttl_cache<K: Eq + Hash + Clone + Send, V: Clone + Send>(capacity: usize) -> impl TtlCache<K, V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- `compute` deve essere invocata al più una volta per ciascun episodio di "chiave mancante o scaduta" condiviso da più chiamate concorrenti — non una volta per chiamata.
- Il lock interno non deve mai essere mantenuto durante l'esecuzione di `compute`.
- L'espulsione per capacità (quando necessaria) deve sempre preferire una voce scaduta a una non scaduta, indipendentemente da quale delle due sia meno recente.
- Una lettura che trova la cache con una voce non scaduta non deve mai bloccare, anche se in quel momento è in corso un calcolo per una chiave diversa.
- Nessuna attesa attiva in nessun punto in cui un thread è in attesa del calcolo altrui.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ogni voce con uno stato che può essere `InCorso` (un calcolo è in atto, con un `Condvar` su cui altri thread possono attendere il suo completamento) oppure `Pronta { valore, scade_il: Instant, ultimo_accesso: Instant }`, tutto dentro una `HashMap<K, StatoVoce>` protetta da `Mutex` + `Condvar` dentro un `Arc`. L'espulsione, quando necessaria, scorre le voci `Pronta` per trovarne una scaduta (`scade_il` nel passato) o, in assenza, quella con `ultimo_accesso` più vecchio — una scansione lineare è accettabile per questo esercizio.

---

## Meta-commentario

**Dove le tre parti si scontrano, e perché è lì la vera difficoltà:** l'insidia non è implementare scadenza, single-flight o LRU singolarmente — sono tutte già state affrontate separatamente nella serie — ma le decisioni di design che uno di essi impone sugli altri. Ad esempio: una voce "in corso di calcolo" non ha ancora un `ultimo_accesso` significativo né un `scade_il` — deve comunque essere rappresentabile nella stessa struttura dati delle voci pronte, il che spinge naturalmente verso un tipo enum a due varianti invece di due mappe separate (una scelta architetturale, non solo sintattica). Oppure: una richiesta che arriva mentre una voce è scaduta ma un'altra richiesta sta già ricalcolandola deve attendere come nel caso "in corso", non trattare la voce scaduta come "assente e da ricalcolare essa stessa" — altrimenti due thread calcolerebbero lo stesso valore scaduto contemporaneamente, violando il requisito di single-flight.

**Perché non è semplicemente `SingleFlightCache` con due funzionalità extra incollate sopra:** in `SingleFlightCache` lo stato di una voce ha due sole possibilità (pronta o in corso). Qui, "pronta" si biforca ulteriormente in "valida" e "scaduta", e la scelta di quale delle due innesca un nuovo calcolo — e quale può ancora contribuire come vittima di espulsione — richiede di ragionare sull'intero ciclo di vita di una voce, non su un singolo bit di stato.

**Perché conta esplicitamente come esercizio "capstone":** supera probabilmente il tempo di un singolo appello reale — è pensato per allenare il ragionamento su strutture che interagiscono, non come previsione puntuale del prossimo testo d'esame. Budget stimato: 120–150 minuti.
