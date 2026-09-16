# Simulazione 045 — ShardedCache (capstone)

*Ricalibrata rispetto alle ultime simulazioni: non una singola struttura condivisa con una corsa sottile da risolvere, ma un numero variabile di strutture indipendenti — gli shard — più uno strato di coordinazione che deve governarle tutte insieme durante un'operazione rara e globale, senza sacrificare il parallelismo di cui gode l'uso ordinario. Il numero e la relazione tra le strutture è qui il problema, non un singolo meccanismo isolato al suo interno.*

---

## ShardedCache

Una cache condivisa da molti thread paga un costo di contesa se un solo lock protegge l'intera mappa: partizionarla in più shard indipendenti, ciascuno con il proprio lock, elimina la contesa tra chiavi che finiscono in shard diversi. Il numero di shard, però, non è sempre quello giusto per sempre — un carico che cresce può richiedere di ridistribuire l'intera cache su più shard, un'operazione che deve avvenire in modo sicuro rispetto a letture e scritture ordinarie in corso.

Si scriva in Rust una struttura che implementi il tratto generico `ShardedCache<K, V>` definito di seguito.

### API richiesta

```rust
use std::hash::Hash;

pub trait ShardedCache<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone {
    // Legge il valore associato a key, instradando verso lo shard
    // corretto in base al numero di shard attualmente in uso. Se un
    // rehash() è in corso, blocca il chiamante, senza consumare cicli di
    // CPU, finché non termina.
    fn get(&self, key: &K) -> Option<V>;

    // Come get(), ma scrive.
    fn put(&self, key: K, value: V);

    // Numero di shard attualmente in uso.
    fn shard_count(&self) -> usize;

    // Ridistribuisce ogni voce esistente secondo un nuovo numero di
    // shard. Ogni get()/put() concorrente deve attendere, senza
    // consumare cicli di CPU, il completamento di questa operazione
    // prima di procedere con il layout aggiornato. Si assuma che
    // rehash() non venga mai chiamato concorrentemente a se stesso.
    fn rehash(&self, new_shard_count: usize);
}

pub fn make_sharded_cache<K: Eq + Hash + Clone + Send, V: Clone + Send>(
    initial_shard_count: usize,
) -> impl ShardedCache<K, V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- Operazioni `get`/`put` su chiavi che, secondo il layout corrente, finiscono in shard diversi non devono mai contendersi a vicenda in alcun modo.
- `rehash()` deve ridistribuire correttamente ogni voce esistente secondo il nuovo `shard_count`, senza perdite né duplicati, e deve escludere in modo pulito qualunque `get`/`put` dall'operare su uno shard mentre la ridistribuzione è in corso — senza che ciò richieda di bloccare un intero shard per sempre, solo per la durata di `rehash()` stessa.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Non esiste un'unica struttura dati "giusta" qui: il problema chiede di decidere quante strutture separate servono, cosa ciascuna protegge, e come si relazionano tra loro — in particolare, come un'operazione che riguarda *tutti* gli shard può coordinarsi con operazioni che ne riguardano *uno solo* senza che le due si blocchino a vicenda più del necessario.

---

## Meta-commentario

**Cosa distingue questo problema da ogni capstone precedente:** `VirtualMemory` (046), `TransactionalPair` (048), `CancelScope` (049) e `InternPool` (050) hanno tutti, nel nucleo, *una* struttura di stato condiviso (per quanto internamente articolata) con una disciplina di lock sottile da rispettare. Qui il numero stesso delle strutture di stato è una variabile del problema — dipende da `initial_shard_count`, può cambiare a runtime — e la vera domanda architetturale è: quale lock protegge *cosa*, e come si evita che l'operazione rara che tocca tutto (`rehash`) e le operazioni frequenti che toccano poco (`get`/`put`) finiscano per serializzarsi a vicenda più del necessario, o peggio, per contraddirsi (una `put` che scrive nel vecchio layout mentre `rehash` sta già ridistribuendo verso il nuovo).

**Perché acquisire tutti gli shard per `rehash()` riguarda di nuovo un ordine di acquisizione, ma in un contesto diverso da `MultiResourceManager` (011):** lì il rischio di stallo nasceva da *più chiamanti indipendenti* che competevano per insiemi di risorse specifiche. Qui c'è un solo chiamante (per assunzione, `rehash()` non è mai concorrente a se stesso) che ha bisogno di *tutte* le risorse contemporaneamente — un problema più vicino a coordinare un'operazione "ferma il mondo" rispetto a operazioni ordinarie che continuano a bussare, che a evitare uno stallo tra pari.

**Difficoltà stimata:** il volume di codice non è necessariamente superiore a problemi precedenti; la difficoltà è quasi interamente nella scelta di decomposizione prima di scrivere una sola riga. Budget stimato: 110–140 minuti.
