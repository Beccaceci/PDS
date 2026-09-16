# Simulazione 015 — SingleFlightCache

*Colma il vuoto a priorità più alta identificato in `coverage_plan.md`: nessun problema precedente richiedeva che un singolo metodo scegliesse, caso per caso, se bloccare o meno — ognuno era interamente bloccante o interamente non bloccante. Qui `get_or_compute` deve fare entrambe le cose, a seconda di cosa trova.*

---

## SingleFlightCache

Quando molte richieste concorrenti domandano lo stesso valore costoso da calcolare (una query lenta, una chiamata di rete) e quel valore non è ancora in cache, calcolarlo una volta per ciascuna richiesta è uno spreco: è sufficiente che **una sola** richiesta esegua il calcolo, mentre le altre che chiedono la stessa chiave nello stesso momento ne attendono il risultato invece di duplicarlo. È lo schema noto come *single-flight* o *request coalescing*.

Si scriva in Rust una struttura che implementi il tratto generico `SingleFlightCache<K, V>` definito di seguito.

### API richiesta

```rust
pub trait SingleFlightCache<K, V: Clone + Send>: Clone {
    // Restituisce il valore associato a `key`.
    //
    // - Se il valore è già presente in cache, ritorna immediatamente,
    //   senza bloccare.
    // - Se nessun altro thread sta già calcolando il valore per questa
    //   chiave, il chiamante stesso diventa responsabile del calcolo:
    //   invoca `compute()` — che può richiedere un tempo arbitrario —
    //   SENZA mantenere alcun lock durante la sua esecuzione, così da non
    //   bloccare richieste concorrenti su chiavi diverse. Al termine,
    //   memorizza il risultato in cache e risveglia chiunque fosse in
    //   attesa dello stesso calcolo.
    // - Se invece un altro thread sta già calcolando il valore per la
    //   stessa chiave, il chiamante attende, senza consumare cicli di CPU,
    //   che quel calcolo termini, quindi restituisce il risultato prodotto
    //   da quell'altro thread, senza ricalcolarlo.
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> V) -> V;
}

pub fn make_single_flight_cache<K, V>() -> impl SingleFlightCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send,
    V: Clone + Send,
{
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread (da cui `Clone` sul tratto `SingleFlightCache`).
- Per una data chiave, se `n` thread chiamano `get_or_compute` concorrentemente mentre il valore non è ancora in cache, `compute` deve essere invocata **esattamente una volta**, non `n` volte.
- Il lock protetto interno non deve mai essere mantenuto durante l'esecuzione di `compute`: una chiamata lenta per la chiave `"a"` non deve impedire a `get_or_compute("b", ...)` di procedere nel frattempo (se `"b"` è già in cache, o se nessun altro sta calcolando `"b"`).
- Tutti i thread in attesa dello stesso calcolo devono ricevere lo stesso valore prodotto da chi lo ha effettivamente calcolato.
- Nessuna attesa attiva in nessun punto in cui un thread è in attesa del risultato altrui.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso come una mappa da chiave a stato della voce (`Pronto(V)`, oppure `InCorso` per una chiave il cui calcolo è già stato assegnato a un altro thread), protetta da `Mutex` + `Condvar` dentro un `Arc`. In `get_or_compute`: acquisire il lock, controllare lo stato della chiave — se `Pronto`, clonare e restituire subito; se `InCorso`, attendere (`wait_while`) che lo stato cambi, poi restituire il valore ormai pronto; se la chiave non è presente affatto, inserirla come `InCorso`, **rilasciare il lock**, eseguire `compute()`, poi riacquisire il lock, sostituire lo stato con `Pronto(risultato)`, notificare (`notify_all`), e restituire il risultato.

---

## Meta-commentario

**Cosa rende questo problema strutturalmente diverso da tutti i precedenti:** ogni simulazione fin qui era o interamente bloccante (quasi tutte) o interamente non bloccante (`VersionedCell`). Qui la stessa chiamata a `get_or_compute` può prendere tre strade diverse — nessuna attesa, attesa su `Condvar`, oppure diventare essa stessa la fonte del risveglio altrui — decise dinamicamente in base allo stato osservato, non dal tipo di chiamata. Un'implementazione che tiene il lock acquisito per l'intera durata di `compute()` supera i test con una sola chiave alla volta, ma introduce una falsa serializzazione tra chiavi completamente indipendenti non appena il test nasconde una `compute()` lenta su una chiave e una richiesta concorrente su un'altra — un problema di prestazioni piuttosto che di correttezza in senso stretto, ma il tipo di errore che un test a tempo (che verifica che la seconda richiesta ritorni entro una soglia breve mentre la prima è ancora in corso) individua senza ambiguità.

**Perché è il completamento naturale di `VersionedCell` (simulazione 014), non solo un'altra variazione:** lì la lezione era "riconoscere quando `Condvar` non serve affatto"; qui la lezione è "riconoscere che `Condvar` serve solo su *alcuni* percorsi di uno stesso metodo, e capire esattamente quali" — la stessa competenza di fondo, applicata al caso in cui non è possibile scegliere un solo stile per l'intero problema.

**Difficoltà stimata:** tra le più alte della serie, non per il numero di tratti (uno solo) ma per la disciplina di scope del lock richiesta — rilasciarlo prima di `compute()` e non semplicemente "prima possibile" ma esattamente nel punto giusto. Budget consigliato: 75–90 minuti.
