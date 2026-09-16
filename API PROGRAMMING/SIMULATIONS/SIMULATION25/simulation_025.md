# Simulazione 025 — TransferPipeline (capstone)

*Dominio nuovo: un gestore di trasferimenti file in background, con ritentativi e cancellazione il cui effetto dipende da quale stato coglie. La difficoltà non è in nessuna singola operazione, ma nel fatto che una `cancel()` e la naturale progressione di stato di un worker possono scontrarsi nello stesso istante, e solo l'ordine in cui vengono osservate sotto lock decide chi vince — un'invariante che va progettata, non aggiunta a posteriori.*

---

## TransferPipeline

Un servizio che copia file in background su richiesta deve gestire fallimenti transitori ritentando automaticamente, verificare l'integrità del risultato prima di dichiararlo completo, e permettere l'annullamento di un trasferimento — ma solo finché non è già entrato nella fase di verifica, dopo la quale annullarlo lascerebbe il sistema in uno stato ambiguo (il file trasferito ma non ancora dichiarato né completo né scartato).

Si scrivano in Rust le strutture che implementano i tratti generici `TransferHandle` e `TransferPipeline` definiti di seguito.

### API richiesta

```rust
pub enum TransferStatus {
    Queued,
    Transferring,
    Verifying,
    Complete,
    Failed,
    Cancelled,
}

pub trait TransferHandle {
    // Stato osservabile corrente del trasferimento.
    fn status(&self) -> TransferStatus;

    // Tenta di annullare il trasferimento. Ha effetto — e restituisce true
    // — solo se lo stato osservato al momento della chiamata è Queued o
    // Transferring. Se lo stato è già Verifying o uno stato terminale, non
    // ha alcun effetto e restituisce false.
    fn cancel(&self) -> bool;

    // Blocca il chiamante, senza consumare cicli di CPU, finché il
    // trasferimento non raggiunge uno stato terminale (Complete, Failed o
    // Cancelled), e lo restituisce.
    fn join(&self) -> TransferStatus;
}

pub trait TransferPipeline: Clone {
    // Sottomette un nuovo trasferimento. `do_transfer` e `do_verify` sono
    // le operazioni effettive, fornite dal chiamante ed eseguite da un
    // thread di lavoro interno (ciascuna restituisce true in caso di
    // successo, false in caso di fallimento). In caso di fallimento di
    // `do_transfer`, il trasferimento viene ritentato automaticamente fino
    // a un totale di `max_attempts` tentativi prima di passare
    // definitivamente a Failed. `do_verify`, se raggiunta, non viene mai
    // ritentata: un suo fallimento porta direttamente a Failed.
    fn submit(
        &self,
        max_attempts: u32,
        do_transfer: impl Fn() -> bool + Send + Sync + 'static,
        do_verify: impl Fn() -> bool + Send + Sync + 'static,
    ) -> impl TransferHandle;
}

pub fn make_transfer_pipeline(worker_count: usize) -> impl TransferPipeline {
    ...
}
```

### Requisiti

- Alla creazione, `worker_count` thread di lavoro devono essere avviati, ciascuno capace di eseguire qualunque trasferimento pendente trovi.
- Transizioni valide: `Queued` → `Transferring` (quando un worker lo prende in carico) → **o** `Verifying` (se `do_transfer` ha successo) **o** di nuovo `Queued` (se `do_transfer` fallisce e restano tentativi) **o** `Failed` (se fallisce e i tentativi sono esauriti); `Verifying` → `Complete` (se `do_verify` ha successo) **o** `Failed` (se fallisce).
- Se `cancel()` ha successo mentre lo stato è `Transferring`, il worker che lo sta eseguendo deve, non appena la chiamata a `do_transfer` in corso termina (non è richiesta l'interruzione a metà della chiamata stessa, opaca al pipeline), fermarsi a `Cancelled` invece di procedere al tentativo successivo o a `Verifying` — indipendentemente dall'esito che `do_transfer` aveva appena prodotto.
- Una `cancel()` che arriva mentre un worker sta già transitando verso `Verifying` non deve avere effetto: la corsa tra le due va risolta in modo che l'esito sia sempre uno dei due stati coerentemente, mai uno stato intermedio o contraddittorio.
- Thread-safe, condivisibile (`Clone`).
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ogni trasferimento con un record contenente lo stato (l'`enum` `TransferStatus`, o un `enum` interno equivalente con eventuali campi aggiuntivi), i tentativi rimasti, e le due chiusure fornite dal chiamante — conservabili come `Arc<dyn Fn() -> bool + Send + Sync>` per poterle richiamare più volte attraverso i tentativi senza spostarle fuori dalla struttura condivisa. Il worker, prima di ogni transizione di stato (dopo aver eseguito `do_transfer` o `do_verify`), deve riacquisire il lock e verificare se nel frattempo lo stato è diventato `Cancelled`: se sì, deve arrestarsi lì senza sovrascriverlo con l'esito appena calcolato, qualunque esso sia — la verifica e la transizione devono avvenire sotto lo stesso lock, mai in due passi separati.

---

## Meta-commentario

**Perché `Fn` (non `FnMut` né `FnOnce`) è la scelta corretta qui, in contrasto con `TickerService` (026):** `do_transfer` può essere richiamata più volte (una per tentativo), quindi `FnOnce` è escluso; non ha però bisogno di mutare uno stato catturato tra un tentativo e l'altro (a differenza dei callback di `on_tick`), quindi `FnMut` sarebbe una restrizione inutile che complicherebbe la condivisione tra i worker senza alcun beneficio — `Fn` è sufficiente e permette di conservare la chiusura dietro un semplice riferimento condiviso, coerente con l'uso di `Arc`.

**Perché la corsa tra `cancel()` e la naturale transizione di stato è il vero cuore del problema:** un'implementazione che legge lo stato, decide "posso annullare", e *poi* scrive `Cancelled` in un passo separato dal worker che legge lo stato, decide "sono ancora valido", e *poi* scrive il proprio esito — anche se ciascuna delle due letture-e-scritture è a sua volta corretta in isolamento — può comunque produrre l'ordine sbagliato se le due operazioni si intrecciano tra loro senza condividere lo stesso lock nello stesso istante di decisione. Non è un problema di *quale* primitiva di sincronizzazione usare (sempre lo stesso `Mutex` di tutta la serie), ma di *dove esattamente* cade il confine di ciascuna sezione critica.

**Difficoltà stimata:** eccede probabilmente il tempo di un singolo appello. Budget stimato: 120–140 minuti.
