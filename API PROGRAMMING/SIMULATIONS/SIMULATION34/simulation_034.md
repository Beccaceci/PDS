# Simulazione 034 — MvccStore

*L'altra direzione che avevo segnalato come più promettente: controllo di concorrenza multi-versione (MVCC), la tecnica reale dietro PostgreSQL e molti database moderni. Diversa da `VersionedCell` (014) non per grado ma per natura: lì un solo valore, senza bisogno di conservare versioni superate. Qui più chiavi devono condividere un'unica nozione coerente di "adesso", e le versioni superate non possono essere scartate finché anche un solo snapshot ancora vivo potrebbe averne bisogno — un problema di conteggio dei riferimenti attraverso l'intero store, non su un singolo valore.*

---

## MvccStore

Un archivio letto molto più spesso di quanto venga scritto trae vantaggio dal non far mai attendere i lettori — nemmeno per la durata di una scrittura — dando a ciascun lettore una fotografia coerente dell'intero stato in un istante preciso, mentre le scritture continuano a creare nuove versioni senza toccare quelle già osservate da uno snapshot in corso. Le versioni superate, però, non sono spazzatura immediata: vanno conservate finché anche un solo snapshot ancora vivo potrebbe ancora averne bisogno.

Si scrivano in Rust le strutture che implementano i tratti `Snapshot<K, V>` e `MvccStore<K, V>` definiti di seguito.

### API richiesta

```rust
use std::hash::Hash;

pub trait Snapshot<K, V: Clone + Send> {
    // Legge il valore associato a `key` così come appariva nell'istante in
    // cui questo snapshot è stato catturato, indipendentemente da
    // qualunque scrittura avvenuta successivamente. None se la chiave non
    // esisteva ancora in quel momento.
    fn get(&self, key: &K) -> Option<V>;
}

pub trait MvccStore<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone {
    // Scrive un nuovo valore per key, creando una nuova versione. Non
    // altera in alcun modo ciò che gli snapshot già catturati
    // restituiscono per quella chiave, e non blocca mai il chiamante.
    fn write(&self, key: K, value: V);

    // Cattura uno snapshot coerente dell'intero store in questo istante:
    // ogni get() su di esso, per qualunque chiave, vede lo stato esatto di
    // questo momento, anche se scritture successive modificano lo store
    // nel frattempo. Non blocca mai il chiamante.
    fn snapshot(&self) -> impl Snapshot<K, V>;

    // Numero di versioni ancora conservate in memoria per key, incluse
    // quelle non più visibili da alcuno snapshot corrente ma non ancora
    // riscattate. Pensato per l'ispezione nei test, senza altro effetto.
    fn version_count(&self, key: &K) -> usize;
}

pub fn make_mvcc_store<K: Eq + Hash + Clone + Send, V: Clone + Send>() -> impl MvccStore<K, V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- Ogni `get()` su uno `Snapshot` restituisce, per l'intera vita di quello snapshot, esattamente il valore che la chiave aveva nell'istante in cui `snapshot()` è stato chiamato — mai un valore scritto dopo, mai un valore scritto prima ma già superato a quel momento.
- Una versione di una chiave può essere effettivamente rimossa dalla memoria solo quando **nessuno** snapshot ancora vivo (né esistente al momento della rimozione, né che potrebbe già esistere per essere stato catturato prima) potrebbe più averne bisogno — anche se quello snapshot non ha mai chiamato `get()` su quella chiave specifica.
- Quando l'ultimo snapshot che rendeva necessaria una versione superata esce dallo scope, quella versione deve, prima o poi, essere effettivamente rimossa — osservabile tramite una diminuzione di `version_count`.
- `write()` e `snapshot()` non devono mai bloccare il chiamante.
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con: un contatore di versione globale (`u64`, incrementato ad ogni `write()`, indipendentemente dalla chiave scritta — un unico orologio logico condiviso da tutte le chiavi), una mappa `HashMap<K, Vec<(u64, V)>>` con le versioni di ciascuna chiave in ordine crescente, e un registro degli snapshot attivi, ad esempio un `BTreeMap<u64, usize>` che conta quanti snapshot vivi condividono ciascun numero di versione — protetti da un unico `Mutex` dentro un `Arc`. `snapshot()` legge la versione globale corrente, ne incrementa il conteggio nel registro, e restituisce uno `Snapshot` che la memorizza; `get(key)` cerca, tra le versioni di quella chiave, la più recente con numero ≤ alla propria. Il `Drop` di `Snapshot` decrementa il proprio conteggio nel registro (rimuovendo la voce se arriva a zero), poi ricalcola la versione minima ancora attiva — la chiave più piccola rimasta nel `BTreeMap`, o "nessun limite" se vuoto — e per ciascuna chiave nella mappa dati rimuove tutte le versioni superate che nessuna versione minima attiva potrebbe più richiedere, mantenendo comunque l'ultima versione ≤ a quel limite.

---

## Meta-commentario

**Perché serve un registro degli snapshot attivi, e non basta "tenere le ultime N versioni":** un limite fisso sul numero di versioni conservate per chiave non ha alcuna relazione con quanti snapshot sono effettivamente vivi in un dato momento — uno snapshot catturato molto tempo fa e ancora vivo potrebbe richiedere una versione ben più vecchia di quante un limite arbitrario ne conserverebbe, mentre in assenza di snapshot vivi anche una sola versione per chiave (l'ultima) sarebbe sufficiente. La correttezza dipende da *quali* snapshot esistono in questo momento, non da un conteggio fisso.

**Perché una versione va mantenuta anche se lo snapshot che la richiede non ha mai chiamato `get()` su quella chiave:** lo `Snapshot` promette una vista coerente sull'*intero* store, non solo sulle chiavi che finora ha effettivamente interrogato — un'implementazione che tracciasse solo le chiavi già lette da ciascuno snapshot (invece del solo numero di versione) risparmierebbe memoria in alcuni casi, ma romperebbe la correttezza nel momento in cui quello snapshot chiamasse `get()` su una chiave non ancora consultata, trovando una versione già rimossa.

**Perché nessun `Condvar` è necessario, a differenza di quasi ogni altro problema della serie:** né `write()` né `snapshot()` hanno mai un motivo per attendere — è la stessa proprietà di `VersionedCell` (014), qui estesa a più chiavi contemporaneamente e combinata, per la prima volta, con una vera raccolta di memoria innescata da `Drop` (in `VersionedCell` non c'era nulla da riscattare: un solo valore, sempre sovrascritto sul posto).

**Difficoltà stimata:** tra le più alte della serie per densità concettuale, nonostante nessuna primitiva di sincronizzazione oltre a un singolo `Mutex`. Budget stimato: 100–130 minuti.
