# Simulazione 028 — QuotaManager

*Dominio nuovo: limitazione multi-tenant, come quella usata da un'API condivisa tra più clienti — ciascuno con un proprio limite individuale, più un limite complessivo condiviso da tutti. La difficoltà è nell'interazione tra due vincoli di capacità di livello diverso (per-tenant e globale) che devono essere verificati e aggiornati insieme, atomicamente, senza che l'uno possa essere soddisfatto mentre l'altro no.*

---

## QuotaManager

Un servizio condiviso tra più clienti (*tenant*) deve impedire che uno solo di essi esaurisca l'intera capacità del sistema, pur non conoscendo in anticipo l'insieme completo dei tenant che lo useranno: ogni tenant ha una propria quota individuale di operazioni concorrenti, ma esiste anche un tetto complessivo che si applica alla somma di tutte le operazioni di tutti i tenant insieme, qualunque essi siano.

Si scrivano in Rust le strutture che implementano i tratti `Permit` e `QuotaManager` definiti di seguito.

### API richiesta

```rust
pub trait Permit {}

pub trait QuotaManager: Clone {
    // Acquisisce un permesso per il tenant identificato da `tenant_id`.
    // Blocca il chiamante, senza consumare cicli di CPU, finché non è
    // possibile concederlo rispettando SIA la quota individuale di quel
    // tenant SIA il limite globale condiviso tra tutti i tenant insieme.
    // Quando l'oggetto restituito esce dallo scope, il permesso viene
    // rilasciato (RAII, tramite Drop), liberando capacità sia per quel
    // tenant sia globalmente.
    fn acquire(&self, tenant_id: &str) -> impl Permit;
}

pub fn make_quota_manager(per_tenant_limit: usize, global_limit: usize) -> impl QuotaManager {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`); i tenant non sono noti in anticipo — un `tenant_id` mai visto prima è implicitamente ammesso, con quota individuale piena disponibile, alla prima `acquire()` che lo nomina.
- `acquire(tenant_id)` deve attendere se il tenant richiedente ha già `per_tenant_limit` permessi attivi, **oppure** se il totale di permessi attivi su tutti i tenant insieme ha già raggiunto `global_limit` — anche se il tenant richiedente non ha ancora raggiunto la propria quota individuale.
- Quando capacità globale si libera mentre più tenant diversi sono in attesa, l'implementazione non deve favorire sistematicamente un tenant specifico (ad esempio, un tenant con molte richieste già in coda non deve poter monopolizzare ogni nuova unità di capacità globale a scapito di un tenant con una singola richiesta in attesa da più tempo) — non è richiesta una politica di equità formalmente dimostrabile, ma nessuna scelta implementativa deve introdurre un vantaggio strutturale per un tenant rispetto a un altro.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con una mappa dell'utilizzo corrente per tenant (`HashMap<String, usize>`) più un contatore globale, protetti da `Mutex` + `Condvar` dentro un `Arc`; `acquire()` attende con `wait_while` finché `utilizzo_globale >= global_limit || utilizzo_del_tenant >= per_tenant_limit`, poi incrementa entrambi i contatori in un'unica operazione sotto lo stesso lock. Il `Permit` restituito deve conservare il proprio `tenant_id` (una stringa posseduta, non un riferimento preso in prestito) insieme a un riferimento allo stato condiviso, in modo che `Drop` possa decrementare correttamente sia il contatore del tenant giusto sia quello globale.

---

## Meta-commentario

**Perché `notify_all`, non `notify_one`, è di nuovo la scelta corretta — la stessa lezione di `TicketLock` (019), in un contesto del tutto diverso:** ogni thread in attesa qui verifica una condizione che dipende dal *proprio* `tenant_id`, non da una condizione uniforme condivisa da tutti gli attendenti come in `ResourcePool`. Un `notify_one` alla liberazione di un permesso potrebbe risvegliare un thread in attesa per un tenant già alla propria quota individuale (che tornerebbe a dormire senza aver consumato la capacità appena liberata), lasciando addormentato — potenzialmente per sempre, se nessun altro evento sopraggiunge — un thread di un tenant diverso che quella capacità avrebbe potuto usarla.

**Perché la mappa per-tenant non può essere semplicemente "tanti `ResourcePool` indipendenti, uno per tenant":** se ogni tenant avesse il proprio contatore protetto da un proprio lock separato, il vincolo *globale* — che attraversa tutti i tenant insieme — non sarebbe esprimibile senza un lock aggiuntivo che li avvolge tutti, reintroducendo comunque un unico punto di sincronizzazione condiviso. La struttura corretta ha un solo lock che protegge sia la mappa sia il contatore globale insieme, proprio perché la decisione "posso procedere?" dipende da entrambi contemporaneamente e deve essere presa atomicamente rispetto a entrambi.

**Difficoltà stimata:** paragonabile a `TicketLock`/`MultiResourceManager` nella sostanza tecnica, ma con una struttura dati per-chiave (`HashMap`) che nessuno dei due usava — combinare correttamente una mappa dinamica con un vincolo globale è la parte non immediata. Budget consigliato: 90–110 minuti.
