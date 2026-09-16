# Simulazione 046 — TaskGraph (capstone)

*Esattamente due tratti pubblici, come `ResourcePool` — ma soddisfarli richiede probabilmente il maggior numero di strutture interne cooperanti di tutta la serie. La difficoltà è tutta a monte del codice: quali informazioni servono, per ciascun task, per rispondere correttamente sia "posso partire?" sia "chi devo avvisare quando finisco?" — e perché la seconda domanda richiede una relazione che nessuna delle due firme dei tratti nomina esplicitamente.*

---

## TaskGraph

Un insieme di task con dipendenze reciproche — questo deve terminare prima che quello possa iniziare — è la struttura naturale di una compilazione, di una pipeline di build, di un grafo di elaborazione dati. Un task diventa eseguibile solo quando tutte le sue dipendenze hanno avuto successo; se anche una sola fallisce, ogni task che ne dipende, direttamente o transitivamente, deve essere considerato fallito senza mai essere eseguito — a cascata, per quanto lontano si estenda quella catena di dipendenze.

Si scrivano in Rust le strutture che implementano i tratti `TaskHandle` e `TaskGraph` definiti di seguito.

### API richiesta

```rust
pub type TaskId = u64;

pub trait TaskHandle {
    // Annulla il task se non è ancora iniziato (in coda, o in attesa
    // delle proprie dipendenze). Non ha alcun effetto — e restituisce
    // false — se il task è già in esecuzione o già terminato. Un
    // annullamento riuscito si propaga come un fallimento a tutti i
    // dipendenti transitivi di questo task, esattamente come farebbe un
    // fallimento della sua esecuzione.
    fn cancel(&self) -> bool;

    // Consuma l'handle, bloccando il chiamante, senza consumare cicli di
    // CPU, finché il task non raggiunge un esito definitivo — successo,
    // fallimento proprio, annullamento, o fallimento ereditato da una
    // dipendenza. Restituisce true solo se il task ha effettivamente
    // eseguito con successo.
    fn join(self) -> bool;
}

pub trait TaskGraph: Clone + Send + Sync {
    // Sottomette un nuovo task che dipende dal completamento con successo
    // di tutti i task identificati in depends_on (id restituiti da
    // precedenti submit sullo stesso grafo — si assuma che ogni id
    // riferisca sempre un task già sottomesso in precedenza). Restituisce
    // l'id assegnato al nuovo task insieme al suo handle.
    //
    // Il task viene eseguito da uno dei thread di lavoro interni non
    // appena tutte le sue dipendenze terminano con successo. Se anche una
    // sola dipendenza fallisce, viene annullata, o è a sua volta saltata
    // per lo stesso motivo, questo task viene saltato senza mai essere
    // eseguito — e questo esito si propaga a sua volta a ogni suo
    // dipendente, ricorsivamente.
    fn submit(
        &self,
        depends_on: &[TaskId],
        task: impl FnOnce() -> bool + Send + 'static,
    ) -> (TaskId, impl TaskHandle);
}

pub fn make_task_graph(worker_count: usize) -> impl TaskGraph {
    ...
}
```

### Requisiti

- Alla creazione, `worker_count` thread di lavoro devono essere avviati, ciascuno capace di eseguire qualunque task pronto trovi.
- Un task con `depends_on` vuoto è pronto immediatamente.
- Un `depends_on` che referenzia un task già terminato con successo prima della chiamata a `submit` conta come dipendenza già soddisfatta; uno che referenzia un task già fallito/annullato/saltato rende il nuovo task immediatamente saltato.
- La propagazione di un fallimento (per qualunque causa) deve raggiungere ogni dipendente transitivo, non solo quelli diretti, indipendentemente da quanto è profonda la catena.
- `cancel()` e la propagazione naturale di un fallimento devono produrre esattamente lo stesso esito osservabile per i dipendenti — non due percorsi che potrebbero divergere.
- Thread-safe, condivisibile (`Clone + Send + Sync`).
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Ogni task registrato deve poter rispondere a due domande distinte: "quante delle mie dipendenze mancano ancora?" e "chi dipende da me?" — la propagazione, sia del successo sia del fallimento, attraversa la seconda relazione, che `depends_on` non fornisce direttamente.

---

## Meta-commentario

**Perché la relazione "chi dipende da me" è la vera scoperta architetturale del problema:** l'API pubblica offre solo `depends_on` — ogni task dichiara ciò da cui dipende, mai chi dipenderà da lui. Ma per notificare correttamente i dipendenti al termine di un task (per farli avanzare, o per propagare loro un fallimento), serve la relazione inversa, costruita e mantenuta internamente man mano che `submit` viene chiamato: quando il task X dichiara di dipendere da Y, è Y che deve registrare "anche X dipende da me", non il contrario. Un'implementazione che tentasse di rispondere "chi dipende da Y?" scorrendo ogni volta l'intero insieme dei task alla ricerca di chi lo nomina in `depends_on` sarebbe corretta ma tradirebbe l'assenza di questa struttura dedicata — il tipo di scelta che distingue un'architettura pensata da una assemblata a posteriori.

**Perché `cancel()` e il fallimento naturale devono convergere sullo stesso percorso di propagazione:** è la stessa disciplina già raccomandata in `Phaser` (038) e `PoisonableBarrier` (045) — due inneschi diversi per lo stesso evento non devono avere due implementazioni parallele del suo effetto, o rischiano di divergere silenziosamente nel tempo (ad esempio, un fallimento naturale che aggiorna correttamente lo stato di un dipendente, ma un `cancel()` a monte che dimentica di farlo).

**Quante strutture servono, indicativamente, senza prescriverle:** uno stato per task (il proprio esito, se determinato), un contatore di dipendenze non ancora soddisfatte, l'elenco dei propri dipendenti diretti, una coda dei task pronti non ancora avviati, e un meccanismo per cui `join()` possa attendere l'esito di uno specifico task — cinque responsabilità distinte, che possono convivere in un'unica struttura per task o essere separate diversamente; la scelta è parte della valutazione.

**Difficoltà stimata:** paragonabile a `VirtualMemory` (046) per densità architetturale, con la propagazione ricorsiva come dimensione aggiuntiva. Budget stimato: 120–150 minuti.
