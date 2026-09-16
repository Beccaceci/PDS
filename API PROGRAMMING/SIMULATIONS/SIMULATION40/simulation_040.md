# Simulazione 046 — VirtualMemory (capstone)

*Il punto più denso della serie finora: non un nuovo meccanismo isolato, ma la fusione deliberata di cinque lezioni già viste separatamente — deduplicazione su richiesta concorrente (`SingleFlightCache`, 015), espulsione LRU (`TtlCache`, 027), blocco per capacità (`ResourcePool`), un vincolo di esclusione dall'espulsione mai visto (il "pin"), e uno scrittura differita su espulsione (mai vista) — in un solo sistema coerente: un gestore di memoria virtuale con page fault, esattamente il cuore di un sistema operativo reale.*

---

## VirtualMemory

Un sistema di memoria virtuale mantiene in memoria fisica solo un sottoinsieme delle pagine di cui un programma potrebbe aver bisogno: quando se ne richiede una non residente (un *page fault*), va caricata — facendo spazio, se necessario, espellendo la pagina residente usata meno di recente tra quelle non attualmente in uso, scrivendo prima su supporto persistente quelle modificate. Una pagina attivamente in uso da un thread non può mai essere scelta per l'espulsione.

Si scrivano in Rust le strutture che implementano i tratti `PageGuard<V>` e `VirtualMemory<K, V>` definiti di seguito.

### API richiesta

```rust
pub trait PageGuard<V: Send> {
    // Accesso condiviso al contenuto della pagina, pinnata (non
    // espellibile) per l'intera durata di vita di questo oggetto.
    fn get(&self) -> &V;

    // Accesso esclusivo e mutabile. Ogni chiamata marca la pagina come
    // "sporca": dovrà essere scritta sul supporto sottostante prima di
    // poter essere espulsa in futuro.
    fn get_mut(&mut self) -> &mut V;
}

pub trait VirtualMemory<K: Eq + std::hash::Hash + Clone + Send, V: Send>: Clone {
    // Accede alla pagina key, pinnandola (non potrà essere scelta per
    // l'espulsione finché il PageGuard restituito non esce dallo scope).
    //
    // Se key è già residente e non in fase di caricamento, la pinna e la
    // restituisce immediatamente, senza bloccare.
    //
    // Se key non è residente e nessun altro thread la sta già caricando,
    // invoca load per ottenerne il contenuto: se ci sono meno di
    // frame_count pagine residenti, la installa direttamente; altrimenti
    // sceglie per l'espulsione la pagina residente NON pinnata usata meno
    // di recente, la scrive su supporto (se sporca) tramite la funzione
    // fornita alla creazione, quindi la rimuove per fare spazio. Se in
    // quel momento tutte le pagine residenti sono pinnate, blocca — senza
    // consumare cicli di CPU — finché almeno una non lo diventa.
    //
    // Se key non è residente ma un altro thread la sta già caricando,
    // attende — senza consumare cicli di CPU — che quel caricamento
    // termini, poi pinna la stessa pagina appena installata, senza
    // richiamare load una seconda volta.
    fn fault_in(&self, key: K, load: impl FnOnce() -> V) -> impl PageGuard<V>;
}

pub fn make_virtual_memory<K: Eq + std::hash::Hash + Clone + Send, V: Send>(
    frame_count: usize,
    write_back: impl Fn(&K, &V) + Send + Sync + 'static,
) -> impl VirtualMemory<K, V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`); al più `frame_count` pagine residenti contemporaneamente.
- Più thread possono pinnare la stessa pagina simultaneamente (il conteggio dei pin può superare 1); una pagina è candidata all'espulsione solo quando il suo conteggio di pin è tornato a zero.
- `load` e `write_back` non devono mai essere invocate mentre è mantenuto un lock che bloccherebbe operazioni su chiavi non correlate — solo l'aggiornamento della struttura condivisa va protetto, non l'esecuzione di queste chiusure fornite dal chiamante.
- Una pagina marcata sporca da almeno una `get_mut()` deve essere scritta tramite `write_back` prima di essere rimossa dalla memoria per fare spazio; una pagina mai modificata (mai acceduta tramite `get_mut()`) non richiede `write_back` alla propria espulsione.
- Se, al momento di un `fault_in` che richiede spazio, nessuna pagina residente è espellibile (tutte pinnate), il chiamante deve attendere, senza consumare cicli di CPU, finché una non lo diventa.
- Nessuna attesa attiva in nessun altro punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ciascuna chiave nota con uno stato — `InCaricamento` (con un modo per notificare chi attende il caricamento) oppure `Residente { valore, conteggio_pin, sporca, ultimo_accesso }` — dentro una mappa condivisa protetta da `Mutex` + `Condvar` in un `Arc`. La sequenza per un fault su una chiave assente, priva di caricamenti concorrenti, richiede più fasi distinte, ciascuna sotto il proprio breve possesso del lock, mai un lock unico mantenuto per l'intera durata: (1) sotto lock, verificare se serve fare spazio e, in caso affermativo, scegliere la vittima LRU non pinnata (attendendo se nessuna esiste), marcarla come "in fase di espulsione" e uscire dal lock; (2) fuori dal lock, invocare `write_back` se la vittima era sporca; (3) rientrare nel lock per rimuoverla definitivamente e marcare la chiave richiesta come `InCaricamento`, poi uscire di nuovo dal lock; (4) fuori dal lock, invocare `load`; (5) rientrare nel lock per installare il risultato come `Residente` con `conteggio_pin: 1`, notificando (`notify_all`) chiunque fosse in attesa — sia di quella pagina (caricamento concorrente) sia di spazio libero (thread bloccati per assenza di vittime). Il `PageGuard` deve tenere traccia, con un flag interno, se `get_mut()` è mai stata chiamata: il suo `Drop` decrementa sempre il conteggio dei pin e, se quel flag era impostato, marca la pagina come sporca nello stato condiviso.

---

## Meta-commentario

**Le cinque lezioni che convergono qui, esplicitamente:** la deduplicazione del caricamento per chiavi richieste concorrentemente è la stessa disciplina di `SingleFlightCache` (015); la scelta della vittima LRU è la stessa di `TtlCache` (027); il blocco quando la capacità è esaurita è la stessa di `ResourcePool`; il rilascio del lock prima di invocare una chiusura fornita dal chiamante (`load`, `write_back`) — qui applicato *due volte* nello stesso flusso, non una — è la disciplina di scope del lock già centrale in `CircuitBreaker` (029) e `TransferPipeline` (031). L'unico ingrediente genuinamente nuovo è il **pin**: un'esclusione dall'espulsione che non è né RAII "restituisci al pool" né "cancella esplicitamente", ma una condizione che la selezione della vittima deve rispettare attivamente, scegliendo *tra* le pagine residenti invece di agire su un singolo oggetto.

**Perché la sequenza a fasi (lock–fuori dal lock–lock di nuovo, due volte) è il vero banco di prova:** un'implementazione che, per semplicità, mantenesse il lock durante `write_back` o `load` supererebbe comunque i test con un solo thread attivo — e fallirebbe silenziosamente (non un errore di compilazione, non un panico: solo un collo di bottiglia totale) non appena un test verificasse che un fault su una chiave *non correlata* può procedere mentre un `load`/`write_back` lento è in corso su un'altra. È lo stesso genere di fallimento silenzioso, non rilevabile senza un test progettato apposta, già discusso a proposito della correzione reale di `ResourcePool`.

**Perché conta come il capstone più denso finora:** non introduce una singola idea nuova di grande portata (come il termine condiviso senza lock centrale di `Elezione del leader`), ma richiede di tenere in testa contemporaneamente cinque discipline già apprese separatamente, applicandole correttamente nello stesso frammento di codice senza farle interferire tra loro. Budget stimato: 140–180 minuti.
