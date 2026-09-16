# Simulazione 018 — TicketLock

*Chiude l'ultimo vuoto di media priorità: in ogni problema precedente, `notify_one` vs `notify_all` era al più una questione di efficienza — entrambi funzionavano correttamente perché ogni thread risvegliato ricontrollava la propria condizione (`wait_while`) e tornava eventualmente a dormire se non era ancora il suo turno. Qui, per la prima volta, `notify_one` è un vero bug di correttezza: può causare uno stallo permanente.*

---

## TicketLock

`std::sync::Condvar` non garantisce alcun ordine tra i thread risvegliati da `notify_one`: non è detto che sia il thread in attesa da più tempo a essere scelto. Un mutex "equo" (*fair*), che garantisca l'accesso in stretto ordine di arrivo delle richieste, richiede quindi un meccanismo che non si affidi all'ordine di risveglio del sistema operativo, ma lo verifichi esplicitamente — è lo schema del *ticket lock*: ogni richiedente estrae un numero progressivo e attende che sia il suo turno, in modo analogo a uno sportello con biglietto numerato.

Si scrivano in Rust le strutture che implementano i tratti generici `LockGuard<T>` e `TicketLock<T>` definiti di seguito.

### API richiesta

```rust
pub trait LockGuard<T: Send> {
    fn get(&self) -> &T;
    fn get_mut(&mut self) -> &mut T;
}

pub trait TicketLock<T: Send> {
    // Acquisisce l'accesso esclusivo al valore protetto, garantendo che le
    // richieste vengano soddisfatte rigorosamente nell'ordine in cui sono
    // state effettuate: se il thread A chiama questo metodo prima del
    // thread B (ed entrambi finiscono per attendere), A deve ottenere
    // l'accesso prima di B — indipendentemente da quale dei due il sistema
    // operativo risveglierebbe per primo. Blocca senza consumare cicli di
    // CPU.
    fn lock(&self) -> impl LockGuard<T>;
}

pub fn make_ticket_lock<T: Send>(value: T) -> impl TicketLock<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread.
- L'ordine di accesso concesso deve rispettare rigorosamente l'ordine di chiamata a `lock()`, non un ordine arbitrario né l'ordine di risveglio scelto dal sistema operativo.
- Al più un `LockGuard<T>` può essere attivo alla volta.
- Quando il `LockGuard<T>` esce dallo scope, l'accesso viene rilasciato (RAII, tramite `Drop`), permettendo alla richiesta successiva in ordine di procedere.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con due contatori — `prossimo_biglietto` e `in_servizio` — protetti da `Mutex` + `Condvar` dentro un `Arc`; `lock()` estrae il proprio numero incrementando `prossimo_biglietto`, poi attende (`wait_while`) finché `in_servizio` non è uguale al proprio numero; il `Drop` del `LockGuard<T>` incrementa `in_servizio` e notifica.

---

## Meta-commentario

**Perché `notify_one` è qui un bug vero, non solo uno spreco:** ogni thread in attesa ricontrolla una condizione — "è il mio numero?" — diversa da quella di ogni altro thread in attesa. `notify_one` risveglia un solo thread scelto dal runtime, senza alcuna garanzia che sia proprio quello il cui numero è ora `in_servizio`. Se risveglia il thread sbagliato, questo ricontrolla, vede che non è ancora il suo turno, e torna a dormire — ma la notifica è stata "consumata" da un thread che non ne aveva bisogno, mentre il thread realmente in attesa del turno corrente può restare addormentato indefinitamente se nessun altro evento lo risveglia in seguito. È uno stallo silenzioso, dipendente dall'interleaving, che può non manifestarsi affatto con pochi thread in un test rapido e comparire solo sotto carico concorrente più sostenuto — esattamente il tipo di bug intermittente più difficile da diagnosticare, e per questo motivo tra i più istruttivi da incontrare per la prima volta in un esercizio piuttosto che in produzione.

**La regola generale che questo esercizio isola:** `notify_one` è sicuro solo quando **qualunque** thread in attesa risvegliato sarebbe corretto da servire (come in `ResourcePool`, dove un elemento libero va bene per qualunque richiedente). Non appena i thread in attesa hanno condizioni di risveglio **individualmente diverse** — come qui, o come in `Rendezvous`/`TurnManager` — serve `notify_all`, perché nessuno dei thread addormentati sa se il risveglio è "per lui".

**Difficoltà stimata:** implementazione semplice; la difficoltà è interamente concettuale — riconoscere perché la scelta tra le due notifiche non è qui una questione di stile. Budget consigliato: 45–60 minuti.
