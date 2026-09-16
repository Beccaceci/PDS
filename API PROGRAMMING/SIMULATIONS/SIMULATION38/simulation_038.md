# Simulazione 038 — HierarchicalPool (capstone)

*Direzione rimasta in sospeso da diverse simulazioni fa: un pool a due livelli, come le cache per-CPU degli allocatori di memoria reali (slab allocator del kernel Linux, `tcmalloc`, `jemalloc`) — ogni worker ha una propria riserva locale, rapida e senza contesa, e un pool condiviso di riserva a cui si ricorre solo quando quella locale è vuota. La difficoltà: un elemento rilasciato a volte resta invisibile agli altri worker (torna nella riserva locale) e a volte diventa visibile a tutti (finisce nella riserva condivisa) — e quale dei due accade dipende dallo stato del pool locale nel preciso istante del rilascio, non da dove l'elemento era stato originariamente prelevato.*

---

## HierarchicalPool

Un pool di risorse condiviso da più worker paga un costo di contesa ogni volta che un worker deve attendere il lock usato da tutti gli altri, anche quando la maggior parte delle richieste potrebbe essere soddisfatta localmente. Dare a ciascun worker una piccola riserva propria, rifornita da un pool condiviso di riserva solo quando necessario, riduce drasticamente quella contesa — a patto che le riserve locali non crescano senza limite, altrimenti gli elementi restano intrappolati presso un worker inattivo mentre altri ne restano privi.

Si scrivano in Rust le strutture che implementano i tratti `Resource<T>`, `LocalPool<T>` e `HierarchicalPool<T>` definiti di seguito.

### API richiesta

```rust
pub trait Resource<T: Send> {
    fn get(&self) -> &T;
}

pub trait LocalPool<T: Send> {
    // Preleva un elemento: dalla riserva locale di questo worker se non
    // vuota (accesso rapido, senza contendere alcun lock condiviso con
    // altri worker); altrimenti dal pool condiviso di overflow, bloccando
    // il chiamante, senza consumare cicli di CPU, se anch'esso è vuoto,
    // finché un elemento non torna disponibile nell'uno o nell'altro.
    fn acquire(&self) -> impl Resource<T>;
}

pub trait HierarchicalPool<T: Send>: Clone + Send + Sync {
    // Restituisce il pool locale del worker worker_id (0..worker_count).
    fn local(&self, worker_id: usize) -> impl LocalPool<T>;

    // Numero totale di elementi gestiti — somma di quelli nelle riserve
    // locali di tutti i worker e di quelli nel pool condiviso di overflow
    // — invariante in ogni istante rispetto a quelli forniti alla
    // creazione.
    fn total_capacity(&self) -> usize;
}

pub fn make_hierarchical_pool<T: Send + 'static>(
    items: Vec<T>,
    worker_count: usize,
    local_capacity: usize,
) -> impl HierarchicalPool<T> {
    ...
}
```

### Requisiti

- Alla creazione, tutti gli elementi forniti iniziano nel pool condiviso di overflow; le riserve locali partono vuote.
- Quando un `Resource<T>` ottenuto tramite `local(worker_id).acquire()` esce dallo scope (RAII, `Drop`): se la riserva locale di `worker_id` ha in quel momento **meno di** `local_capacity` elementi, l'elemento vi ritorna; altrimenti va nel pool condiviso di overflow.
- Un elemento che ritorna nella riserva locale di `worker_id` (perché sotto `local_capacity`) non deve diventare visibile né disponibile per `acquire()` chiamato su un pool locale diverso, né deve necessariamente sbloccare un'attesa in corso sul pool condiviso di overflow.
- L'accesso alla riserva locale di un worker non deve mai contendere, né essere bloccato da, un'operazione in corso sulla riserva locale di un worker diverso — solo l'accesso al pool condiviso di overflow è, necessariamente, un punto di possibile contesa comune a tutti.
- Thread-safe, condivisibile (`Clone + Send + Sync`).
- Nessuna attesa attiva.
- I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ciascuna riserva locale come un `Mutex<Vec<T>>` indipendente (senza `Condvar` proprio: se vuota, `acquire()` ricorre semplicemente al pool condiviso, non attende sulla riserva locale in sé), e il pool condiviso di overflow come un `Mutex<Vec<T>>` + `Condvar` separato, tutti dentro lo stesso `Arc`. `acquire()` su `local(worker_id)`: prova prima il lock della riserva locale (breve, senza attesa); se vuota, rilascia quel lock e prova il pool condiviso, attendendo lì (`wait_while`) se necessario — l'unico punto della struttura in cui un'attesa reale può avvenire. Il `Resource<T>` restituito deve ricordare da quale `worker_id` proviene, per sapere a quale riserva locale tentare di tornare al momento del `Drop`.

---

## Meta-commentario

**Perché non ogni rilascio deve notificare il pool condiviso:** un'implementazione che, per prudenza, notificasse sempre il `Condvar` del pool condiviso ad ogni rilascio — anche quando l'elemento resta nella riserva locale — non sarebbe scorretta in senso stretto (un risveglio spurio che ricontrolla la propria condizione e si riaddormenta è sempre sicuro), ma tradirebbe l'assunzione centrale del problema: un rilascio locale è, per definizione, un evento che *non* riguarda gli altri worker. Il vero requisito da rispettare è quello inverso — un worker in attesa sul pool condiviso non deve *dipendere* da notifiche locali per essere prima o poi risvegliato correttamente quando un elemento finisce davvero nel pool condiviso.

**Perché il `Resource<T>` deve ricordare il proprio `worker_id` invece di limitarsi a "tornare da dove è stato preso":** un elemento prelevato dal pool condiviso di overflow (perché la riserva locale era vuota al momento dell'`acquire`) va comunque restituito, al rilascio, tentando prima la riserva locale del worker che lo ha richiesto — non il pool condiviso da cui proveniva originariamente — per dargli la possibilità di "diventare" locale a quel worker, esattamente lo scopo del pattern (un elemento tende a stabilizzarsi presso il worker che lo usa più spesso, finché la capacità locale non lo impedisce).

**Perché conta come esercizio capstone:** la combinazione di locking a due livelli con visibilità selettiva (alcuni rilasci restano invisibili agli altri worker, altri no) e la necessità che il pool condiviso resti comunque una fonte affidabile per chi vi attende, eccede probabilmente il tempo di un singolo appello. Budget stimato: 100–130 minuti.
