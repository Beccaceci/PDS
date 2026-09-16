# Simulazione 051 — CacheBus (capstone)

*Esattamente due tratti pubblici, come richiesto — e una sfida che porta la concorrenza al livello dei protocolli hardware a basso livello (architettura dei calcolatori e modelli di memoria): un bus di memoria con protocollo di coerenza MESI (Modified, Exclusive, Shared, Invalid), snooping concorrente tra core, store buffer asincroni e barriere di memoria (`smp_mb`). La vera difficoltà architetturale è coordinare il transito delle linee di cache tra più core indipendenti: una scrittura su un core deve invalidare atomicamente le copie condivise negli altri core senza causare stalli di bus, mentre la barriera di memoria deve sospendere il chiamante senza consumare cicli di CPU finché tutti gli store buffer pendenti non sono stati drenati e confermati.*

---

## CacheBus

Nelle architetture multi-core simmetriche (SMP), ciascun core della CPU dispone di una propria cache L1 privata per ridurre la latenza degli accessi e la congestione verso la memoria centrale (RAM). Tuttavia, quando più core leggono e scrivono concorrentemente le medesime locazioni di memoria, sorge il problema fondamentale della **coerenza di cache**: modifiche apportate da un core non devono risultare invisibili o contraddittorie per gli altri core.

Il protocollo standard de facto per garantire la coerenza a livello hardware è il **protocollo MESI** (noto anche come protocollo dell'Illinois). Ogni linea di cache (associata a un indirizzo di memoria) può trovarsi in uno dei quattro stati fondamentali:

1. **`Modified (M)`**: la linea è presente unicamente nella cache locale ed è "sporca" (il suo valore è più recente di quello presente nella memoria principale). Il core possiede l'autorizzazione esclusiva di lettura e scrittura.
2. **`Exclusive (E)`**: la linea è presente unicamente nella cache locale, ma è "pulita" (identica alla memoria principale). Il core può leggerla immediatamente o promuoverla silenziosamente a `Modified` al momento della scrittura, senza generare traffico di bus.
3. **`Shared (S)`**: la linea è presente nella cache locale e potenzialmente in una o più cache di altri core; il dato è pulito. Il core può leggerla, ma non può scriverla senza prima invalidare le copie degli altri.
4. **`Invalid (I)`**: la linea non contiene dati validi. Qualsiasi operazione di lettura o scrittura costituisce un *cache miss*.

Tutti i core sono collegati a un canale di trasmissione condiviso (il **Bus di Memoria**). Ciascun core pratica costantemente lo **snooping** (ascolto passivo) dei messaggi di broadcast inviati sul bus:
- **`BusRd` (Read Miss)**: emesso quando un core ha un miss in lettura. Gli altri core che detengono la linea in `Modified` devono effettuare il flush del dato sporco verso il bus/memoria e degradare il proprio stato a `Shared`; se la detenevano in `Exclusive`, degradano a `Shared`. Il core richiedente installa la linea: se altri core avevano la linea, transita in `Shared`; se nessun altro core la possedeva, transita direttamente in `Exclusive`.
- **`BusUpgr` (Upgrade Write)**: emesso quando un core possiede la linea in `Shared` e desidera scriverla. Tutti gli altri core che detengono la linea in `Shared` la invalidano immediatamente (`S -> I`). Il core richiedente passa da `Shared` a `Modified`.
- **`BusRdX` (Read-with-Intent-to-Modify)**: emesso quando un core ha un miss in scrittura (`Invalid`). Gli altri core invalidano le loro copie (se un core l'aveva in `Modified`, effettua prima il flush) e il richiedente ottiene l'accesso esclusivo in stato `Modified`.

Inoltre, per non bloccare la pipeline di esecuzione ad ogni scrittura mentre si attendono le conferme di invalidazione dagli altri core, ciascun core è dotato di uno **Store Buffer**: le scritture vengono inserite in coda FIFO ed elaborate in background. Tuttavia, quando un thread necessita di garantire la sequenzialità delle operazioni (ad esempio prima di rilasciare un lock), invoca una **barriera di memoria (`memory_barrier`)**, la quale sospende il chiamante **senza consumare cicli di CPU** finché lo store buffer locale non è completamente vuoto e tutte le invalidazioni non sono state applicate.

Si scrivano in Rust le strutture che implementano i tratti `CoreCache` e `MemoryBus` definiti di seguito.

---

### API richiesta

```rust
pub type CoreId = u64;
pub type Address = u64;
pub type Value = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MesiState {
    Modified,
    Exclusive,
    Shared,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusError {
    /// Il core è stato disconnesso o il bus è stato arrestato.
    CoreDisconnected,
}

pub trait CoreCache: Send + Sync {
    // Legge il valore associato all'indirizzo specificato.
    // - Cache Hit (M, E, S): restituisce immediatamente il valore locale (nessun messaggio di bus).
    // - Cache Miss (I): emette BusRd sul bus; snoop degli altri core (flush se in M, downgrade di E/S a S);
    //   installa la linea come Shared (se condivisa con altri core) oppure come Exclusive (se assente altrove).
    fn read(&self, addr: Address) -> Result<Value, BusError>;

    // Scrive il valore all'indirizzo specificato.
    // - Se la linea è in stato M: sovrascrive il dato localmente.
    // - Se la linea è in stato E: transita a M e sovrascrive il dato localmente.
    // - Se la linea è in stato S: emette BusUpgr, invalidando le copie negli altri core, e transita a M.
    // - Se la linea è in stato I: emette BusRdX, invalidando le copie altrui (con flush se M), e ottiene la linea in stato M.
    // La scrittura può transitare nello store buffer del core prima di essere consolidata nella linea.
    fn write(&self, addr: Address, val: Value) -> Result<(), BusError>;

    // Blocca il chiamante, senza consumare cicli di CPU, finché tutte le scritture
    // pendenti nello store buffer di questo core non sono state completamente
    // consolidate nella cache ed propagate sul bus (smp_mb).
    fn memory_barrier(&self);

    // Restituisce lo stato MESI attuale della linea di cache contenente l'indirizzo.
    fn line_state(&self, addr: Address) -> MesiState;

    // Restituisce il numero di scritture attualmente pendenti nello store buffer locale.
    fn pending_store_count(&self) -> usize;
}

pub trait MemoryBus: Clone + Send + Sync {
    // Connette un nuovo core al bus di memoria condiviso, restituendo l'ID univoco assegnato e il rispettivo handle CoreCache.
    fn attach_core(&self) -> (CoreId, impl CoreCache + 'static);

    // Restituisce il valore attualmente memorizzato nella memoria principale (RAM)
    // per quell'indirizzo (il valore di base o quello risultante dall'ultimo flush di una linea M).
    fn read_main_memory(&self, addr: Address) -> Value;

    // Restituisce il conteggio cumulativo di transazioni di broadcast trasmesse sul bus (BusRd, BusRdX, BusUpgr).
    fn total_bus_transactions(&self) -> usize;

    // Restituisce il conteggio cumulativo di invalidazioni di linee avvenute per effetto dello snooping.
    fn total_invalidations(&self) -> usize;

    // Restituisce il numero di core attualmente connessi e attivi.
    fn active_core_count(&self) -> usize;
}

pub fn make_memory_bus() -> impl MemoryBus {
    ...
}
```

---

### Requisiti

- **Invarianti di Coerenza MESI**:
  - In ogni istante, se una linea di cache per un dato indirizzo si trova in stato `Modified` su un core, nessun altro core può possedere quella linea in stato diverso da `Invalid`.
  - Se una linea è in stato `Exclusive` su un core, nessun altro core può possederla in stato diverso da `Invalid`.
  - Se una linea è in stato `Shared` su un core, può essere presente solo in stato `Shared` o `Invalid` negli altri core.
- **Snooping Reattivo**:
  - Quando il core $A$ emette `BusUpgr` o `BusRdX` per l'indirizzo $X$, **tutti** gli altri core che detengono la linea per $X$ devono transire il proprio stato a `Invalid`.
  - Ogni linea invalidata in questo modo incrementa il contatore globale `total_invalidations()`.
  - Ogni operazione che richiede il bus (`BusRd`, `BusRdX`, `BusUpgr`) incrementa il contatore `total_bus_transactions()`.
- **Store Buffer & Barriera di Memoria**:
  - Il metodo `memory_barrier()` non deve consumare cicli di CPU: deve attendere su una `Condvar` che il contatore `pending_store_count()` scenda a 0.
- **Gestione RAII (`Drop`) del `CoreCache`**:
  - Quando un handle `CoreCache` viene distrutto (`Drop`), tutte le sue linee ancora nello stato `Modified` devono essere trascritte (flush) nella memoria principale (`main_memory`), e il core deve essere deregistrato dal bus decrementando `active_core_count()`.
- **Thread-Safety & Assenza di Deadlock**:
  - Condivisibile tra thread (`Clone + Send + Sync`).
  - Nessuna attesa attiva.
  - Nessun deadlock di bus: se due core tentano simultaneamente una transazione di bus, l'arbitro del bus deve serializzarle deterministamente.
- **I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).**
- **Se il codice consegnato non compila, non verrà valutato.**

---

### Suggerimenti implementativi

1. Ciascun core può essere rappresentato da una struttura interna contenente: una tabella delle linee locali (`HashMap<Address, (Value, MesiState)>`), una coda di store buffer FIFO e una `Condvar` locale su cui sospendere il thread invocante di `memory_barrier()`.
2. Il `MemoryBus` centrale coordina lo stato globale protetto da `Mutex`: la memoria principale (RAM), l'elenco dei core registrati per lo snooping e i contatori di diagnostica (transazioni e invalidazioni).
3. Quando un core emette una transazione di bus:
   - Sotto il lock del bus, itera sugli altri core registrati interrogando il loro stato locale per quell'indirizzo.
   - Se trova una linea `Modified`, ne copia il valore aggiornato nella memoria principale ed effettua il downgrade/invalidation nel core proprietario.
   - Se emette un'invalidazione (`BusUpgr` o `BusRdX`), forza lo stato locale degli altri core a `MesiState::Invalid` e incrementa il contatore delle invalidazioni.
4. Quando uno store buffer completa il drenaggio di un'operazione, decrementa `pending_store_count` e, se raggiunge zero, invoca `notify_all()` sulla `Condvar` della barriera.

---

## Meta-commentario

**Perché `CacheBus` è una pietra miliare architetturale:**
Negli esercizi precedenti, la comunicazione avveniva sempre per via diretta (code MPMC, canali, grafi di dipendenze). In `CacheBus`, invece, si implementa il pattern del **canale di broadcast con snooping distribuito**: ciascun core opera in modo indipendente sulla propria memoria locale veloce, ma è costantemente soggetto a mutazioni di stato esogene innescate dalle azioni dei suoi pari. L'invariante di coerenza non è localizzata in un singolo punto, ma è una proprietà emergente del sistema.

**La scoperta architetturale chiave: la serializzazione delle transazioni concorrenti di bus:**
Se il Core 1 ha la linea $X$ in `Shared` e chiama `write()`, e simultaneamente il Core 2 ha la linea $X$ in `Shared` e chiama `write()`, entrambi vorrebbero emettere `BusUpgr`. Se non vi fosse una rigorosa serializzazione dell'arbitro del bus, entrambi penserebbero di aver invalidato l'altro e passerebbero entrambi allo stato `Modified`, violando catastroficamente il protocollo MESI (due scrittori esclusivi contemporanei sulla stessa locazione). L'arbitro deve concedere il bus a uno dei due: il vincitore emette l'invalidazione e passa a `Modified`; il perdente vede arrivare l'invalidazione prima di ottenere il bus, la propria linea diventa `Invalid`, e la sua scrittura deve automaticamente convertirsi da `BusUpgr` a `BusRdX`.

**Store Buffer e memoria debolmente ordinata:**
L'introduzione dello store buffer simula fedelmente l'origine del disallineamento della memoria nei processori moderni (x86 TSO, ARM weak ordering): le scritture non sono istantanee sul bus. La barriera di memoria (`smp_mb`) rappresenta l'unico punto di rendezvous in cui il programmatore forza il riallineamento temporale, rendendo l'attesa su `Condvar` la perfetta traduzione software del blocco della pipeline hardware.

**Strutture cooperanti stimate (senza prescriverle):**
1. `BusState`: contiene la RAM principale, la mappa dei core connessi, i contatori statistici del bus.
2. `CoreInner`: contiene la mappa delle linee cache locali (`Address -> (Value, MesiState)`), la coda dello store buffer e la relativa Condvar di barriera.
3. `MyCoreCache`: handle del core che implementa `CoreCache`, con `Drop` per il flush delle linee sporche.
4. `BusManager`: struttura pubblica che implementa `MemoryBus`.

**Difficoltà stimata:** 5.0+ / 5.0. Budget stimato: 130–160 minuti.
