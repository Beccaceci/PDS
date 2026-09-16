# Simulazione 012 — MetricsBoard

*Deliberatamente diversa nella presentazione dalle simulazioni precedenti: intestazioni di sezione diverse, vincoli parafrasati invece delle frasi canoniche già viste, nessuna sezione di suggerimenti implementativi, e due idiomi Rust mai usati finora (`Result` con un tipo di errore proprio, granularità di lock mista). L'architettura di fondo — tratti + funzione factory opaca — resta quella osservata nei campioni reali: quella non cambia mai, è la firma del professore.*

---

## MetricsBoard

Un pannello di controllo per un sistema embedded riceve in continuazione aggiornamenti numerici da più sottosistemi (temperatura, tensione, carico di CPU...) e deve, allo stesso tempo, permettere a un osservatore di leggere lo stato corrente o di attendere che una particolare grandezza superi una soglia critica.

Si scrivano in Rust le strutture che implementano i tratti generici `MetricHandle` e `MetricsBoard` definiti di seguito.

### API richiesta

```rust
pub struct NameAlreadyUsed;

pub trait MetricHandle {
    // Aggiorna il valore corrente della metrica associata a questo handle.
    // Pensato per essere invocato con altissima frequenza da più thread,
    // anche su metriche diverse contemporaneamente.
    fn record(&self, value: f64);
}

pub trait MetricsBoard {
    // Registra una nuova metrica identificata da `name`. Se il nome è già
    // in uso da una registrazione ancora attiva, l'operazione fallisce e
    // restituisce `NameAlreadyUsed`, senza alterare lo stato del board. Il
    // nome torna disponibile per una nuova registrazione quando l'handle
    // restituito da questa chiamata viene distrutto.
    fn register(&self, name: &str) -> Result<impl MetricHandle, NameAlreadyUsed>;

    // Restituisce una fotografia interamente coerente di tutte le metriche
    // correntemente registrate, come coppie nome-valore — non un insieme
    // di letture prese in istanti diversi le une dalle altre.
    fn snapshot(&self) -> Vec<(String, f64)>;

    // Resta sospeso finché il valore della metrica `name` non è maggiore o
    // uguale a `threshold`. Si assuma che venga invocato solo su metriche
    // già registrate.
    fn wait_for_threshold(&self, name: &str, threshold: f64);
}

pub fn make_metrics_board() -> impl MetricsBoard {
    ...
}
```

### Vincoli

- Il board va condiviso tra più thread contemporaneamente, sia per aggiornare sia per leggere le metriche.
- `record()` è pensata per un tasso di chiamate molto alto: aggiornamenti su metriche diverse non devono serializzarsi a vicenda più del necessario.
- Il thread chiamante di `wait_for_threshold()` non deve restare occupato in cicli di verifica ripetuta (polling) mentre attende.
- Ogni nome può essere registrato una sola volta alla volta; alla distruzione del `MetricHandle` corrispondente, il nome torna disponibile per una nuova `register()`.
- `snapshot()` deve riflettere uno stato realmente esistito in un singolo istante, non una combinazione di letture prese in momenti diversi.
- La consegna che non compila non viene presa in considerazione ai fini della valutazione.
- `cargo test` deve completare con successo su tutti i test forniti, senza alcuna modifica al file di test.

---

## Meta-commentario

**Le variazioni deliberate, da rimuovere prima di usarlo come vera simulazione a tempo:**

- Sezione "Vincoli" invece di "Requisiti"; frasi parafrasate invece delle formule già viste (*"non deve restare occupato in cicli di verifica ripetuta"* invece di *"senza consumare cicli di CPU"*; *"la consegna che non compila non viene presa in considerazione"* invece di *"se il codice non compila, non verrà valutato"*) — stesso significato, lessico diverso, per non abituarsi a riconoscere solo le frasi esatte già incontrate.
- Nessuna sezione di suggerimenti implementativi (come in `ResourcePool`): l'intero disegno architetturale va dedotto autonomamente, senza scaletta.
- `Result<impl MetricHandle, NameAlreadyUsed>` — primo caso della serie in cui il fallimento richiede un tipo di errore proprio invece di `Option`, perché qui il fallimento ha una causa specifica e nominabile (nome duplicato), non una semplice assenza.
- Granularità del lock mista, ed è la vera insidia: `record()` chiede esplicitamente di non serializzare inutilmente metriche diverse tra loro — il che spinge verso un lock per metrica, non un unico lock globale sull'intero board — mentre `snapshot()` chiede uno stato coerente su *tutte* le metriche simultaneamente, il che richiede di acquisire più lock contemporaneamente. Chi ha già affrontato `MultiResourceManager` (simulazione 011) riconoscerà l'insidia: acquisire i lock delle singole metriche in un ordine qualunque durante `snapshot()`, mentre `register()`/`record()` li acquisiscono in un ordine diverso, riapre esattamente lo stesso rischio di stallo — la soluzione è la stessa lezione già vista lì, applicata a un contesto diverso: un ordine totale fisso di acquisizione (ad esempio alfabetico sui nomi) rispettato ovunque nel codice.

**Perché la firma architetturale (tratti + funzione factory con `impl Trait`) non è tra le cose variate:** è l'elemento più stabile osservato in entrambi i campioni reali; variarlo insegnerebbe ad aspettarsi uno stile che quasi certamente non cambierà, mentre variare lessico, struttura delle sezioni e scelta tra `Option`/`Result` insegna a riconoscere la sostanza sotto formulazioni diverse — che è esattamente il rischio reale se il testo del prossimo esame differisce anche solo nella forma da quelli già visti.

**Difficoltà stimata:** paragonabile a `TransactionalQueue`/`RwCell`; la parte concettualmente più difficile non è la sincronizzazione in sé ma riconoscere la tensione tra granularità fine (per `record()`) e coerenza globale (per `snapshot()`) senza che venga suggerita esplicitamente. Budget consigliato: 90–110 minuti.
