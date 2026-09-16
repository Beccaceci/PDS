# Simulazione 027 — SagaOrchestrator

*Dominio nuovo: il pattern *saga* (transazioni distribuite senza lock a due fasi, comuni nei sistemi a microservizi) — una sequenza di passi ciascuno con una propria azione di compensazione, da annullare in ordine inverso se un passo successivo fallisce. Diverso da `TwoPhaseCommitCoordinator` (032) nello spirito: lì i partecipanti agivano in parallelo dietro un'unica decisione; qui l'ordine è parte della semantica, e ogni passo dipende dagli effetti di quello prima. La vera difficoltà: un passo troppo lento non può essere interrotto in Rust, e l'orchestratore deve decidere onestamente cosa fare di un effetto che potrebbe ancora materializzarsi dopo che ha già rinunciato ad aspettarlo.*

---

## SagaOrchestrator

Una sequenza di operazioni su sistemi diversi (ad esempio: riservare inventario, addebitare un pagamento, pianificare una spedizione) non può essere resa atomica con un lock distribuito se i sistemi coinvolti non lo supportano — ma se un passo a metà sequenza fallisce, gli effetti dei passi già riusciti vanno annullati esplicitamente, in ordine inverso rispetto a quello con cui sono stati applicati.

Si scrivano in Rust le strutture che implementano i tratti `Step` e `SagaOrchestrator` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Step: Send {
    // Esegue il passo. Restituisce true in caso di successo, false in
    // caso di fallimento esplicito.
    fn execute(&self) -> bool;

    // Annulla gli effetti di questo passo. Invocato al più una volta, e
    // solo se execute() aveva già avuto successo in precedenza.
    fn compensate(&self);
}

pub trait SagaOrchestrator: Clone {
    // Esegue in ordine i passi forniti, uno alla volta. Ciascun passo ha
    // al più step_timeout per completare execute(): se non termina entro
    // quel tempo, o se termina restituendo false, è considerato fallito.
    //
    // Al primo passo fallito, l'esecuzione della saga si interrompe:
    // vengono invocati compensate() sui passi precedenti già riusciti, in
    // ordine STRETTAMENTE INVERSO rispetto a quello di esecuzione, e il
    // metodo restituisce false. Se tutti i passi hanno successo entro il
    // proprio timeout, restituisce true senza invocare alcuna
    // compensate().
    //
    // Un passo il cui execute() supera step_timeout continua comunque ad
    // essere eseguito sullo sfondo (Rust non offre un modo per
    // interromperlo forzatamente): se in seguito termina comunque con
    // successo, il suo effetto non deve essere in alcun modo considerato
    // parte della saga già interrotta, né compensato.
    fn run_saga(&self, steps: Vec<Box<dyn Step>>, step_timeout: Duration) -> bool;
}

pub fn make_saga_orchestrator() -> impl SagaOrchestrator {
    ...
}
```

### Requisiti

- I passi vanno eseguiti in ordine sequenziale, mai in parallelo tra loro: il passo N+1 può presupporre gli effetti del passo N.
- `SagaOrchestrator` deve essere condivisibile (`Clone`) e utilizzabile per saghe concorrenti indipendenti: chiamate simultanee a `run_saga` su cloni diversi, con liste di passi diverse, non devono interferire tra loro in alcun modo.
- `compensate()` va invocato esclusivamente sui passi il cui `execute()` ha già restituito `true` prima del fallimento, mai sul passo fallito stesso né su passi non ancora raggiunti.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Per applicare un timeout a un singolo `execute()` senza poter interrompere il thread che lo esegue, l'unica via è eseguirlo su un thread separato e attendere il suo risultato con un limite di tempo — ad esempio tramite un `Mutex` + `Condvar` locale a quel singolo passo (creato e distrutto ad ogni passo, non condiviso con l'intera saga), su cui il thread principale attende con `wait_timeout_while` mentre il thread spawnato per il passo scrive il proprio risultato al termine e notifica. Se il timeout scade prima che il risultato arrivi, il thread principale procede a compensare senza attendere oltre; il thread del passo, se ancora vivo, continuerà ad eseguire fino alla propria conclusione naturale, scrivendo un risultato che a quel punto nessuno leggerà più.

---

## Meta-commentario

**Perché questo problema non va confuso con un esercizio di "solo controllo di flusso":** la sequenza "esegui in avanti, tieni traccia dei successi, compensa all'indietro in caso di fallimento" è, presa da sola, poco più che un ciclo con una pila — non è lì la difficoltà. La difficoltà è che ogni singolo passo di quel ciclo, per rispettare il timeout, richiede la stessa architettura di sincronizzazione (thread separato, attesa con limite, gestione dell'esito tardivo) già vista altrove nella serie, ripetuta e incastonata dentro una logica sequenziale più ampia — è la composizione delle due cose, non nessuna delle due da sola, a rendere il problema difficile.

**Perché l'orfano "non compensato" è la scelta corretta, non una scappatoia:** compensare un passo il cui `execute()` potrebbe ancora essere in esecuzione in background significherebbe rischiare di invocare `compensate()` prima che gli effetti da annullare esistano ancora, o mentre `execute()` li sta ancora producendo — una corsa senza soluzione pulita, dato che non c'è modo di sapere con certezza se e quando quel thread orfano terminerà. Documentare onestamente il limite (l'effetto puramente ipotetico di un successo tardivo non è gestito) è la stessa scelta di design già vista nel `Coordinator` di `TwoPhaseCommitCoordinator` (032) davanti allo stesso vincolo di Rust.

**Difficoltà stimata:** eccede probabilmente il tempo di un singolo appello, soprattutto per la disciplina richiesta nel isolare correttamente lo stato di sincronizzazione di ciascun passo da quello degli altri passi e delle altre saghe concorrenti. Budget stimato: 120–150 minuti.
