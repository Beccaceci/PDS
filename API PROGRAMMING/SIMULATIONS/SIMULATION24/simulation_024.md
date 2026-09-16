# Simulazione 024 — LeaseLockManager (capstone)

*Dominio nuovo, tier capstone: un gestore di lock a lease, come quelli usati nei sistemi distribuiti (etcd, ZooKeeper) per evitare che un possessore bloccato o terminato in modo anomalo tenga un lock per sempre. Fonde un thread di background che agisce autonomamente (`TaskExecutor`, 004) con un contatore di generazione per evitare corse ABA (`Rendezvous`/`TurnManager`, 002/017) e introduce una terza via di risoluzione mai vista prima: non RAII *oppure* azione esplicita come in ogni problema precedente, ma **tre** meccanismi di rilascio che devono coesistere senza contraddirsi — rinnovo esplicito, rilascio anticipato via `Drop`, e scadenza automatica.*

---

## LeaseLockManager

Un servizio di lock distribuito non può permettere che un lock resti bloccato per sempre se chi lo detiene si blocca o dimentica di rilasciarlo: ogni acquisizione ha una scadenza (*lease*), che il possessore deve rinnovare esplicitamente per continuare a detenere il lock; se non lo fa in tempo, il lock si libera automaticamente, anche senza alcuna azione da parte sua.

Si scrivano in Rust le strutture che implementano i tratti generici `Lease` e `LeaseLockManager` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Lease {
    // Rinnova la lease corrente, estendendola di `duration` a partire da
    // questo momento. Restituisce false se la lease era già scaduta (e
    // quindi il lock già rilasciato automaticamente) prima di questa
    // chiamata — in tal caso il rinnovo non ha alcun effetto.
    fn renew(&self, duration: Duration) -> bool;
}

pub trait LeaseLockManager: Clone {
    // Acquisisce il lock identificato da `name`, con una lease iniziale
    // della durata data. Blocca il chiamante, senza consumare cicli di
    // CPU, finché il lock non è libero — perché non è mai stato
    // acquisito, perché la lease precedente è scaduta senza essere stata
    // rinnovata, oppure perché il precedente `Lease` è uscito dallo scope
    // prima della scadenza.
    fn acquire(&self, name: &str, lease_duration: Duration) -> impl Lease;
}

pub fn make_lease_lock_manager() -> impl LeaseLockManager {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`); ogni nome di lock è indipendente dagli altri.
- Se l'oggetto `Lease` esce dallo scope prima della propria scadenza, il lock corrispondente si libera immediatamente (RAII, tramite `Drop`), risvegliando eventuali thread in attesa di acquisire lo stesso nome.
- Se nessuno rinnova né lascia uscire dallo scope l'oggetto `Lease` in tempo, il lock deve liberarsi comunque, entro un tempo ragionevole dopo la scadenza, senza che nessuno lo richieda esplicitamente.
- `renew()` chiamato dopo che il lock è già stato liberato (per scadenza) non deve avere alcun effetto, e deve restituire `false` — in particolare, non deve mai rinnovare per errore una lease che nel frattempo è stata riassegnata a un altro chiamante che ha acquisito lo stesso nome.
- `Drop` di un `Lease` la cui lease era già scaduta al momento della distruzione (e quindi il lock già riassegnato ad altri) non deve liberare il lock del *nuovo* possessore.
- Nessuna attesa attiva in nessun punto, incluso il meccanismo che rileva le scadenze.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato di ciascun lock con un `enum` — `Libero` oppure `Occupato { scade_il: Instant, generazione: u64 }` — dentro una mappa condivisa protetta da `Mutex` + `Condvar` in un `Arc`. Ogni oggetto `Lease` conserva la `generazione` che aveva al momento dell'acquisizione: `renew()` e `Drop` devono entrambi verificare che la generazione memorizzata nel lock coincida ancora con la propria prima di agire — se non coincide, il lock è già stato riassegnato (per scadenza o per un rilascio precedente), e l'operazione non deve avere alcun effetto. Un thread di sottofondo ("reaper"), avviato dalla funzione factory, si risveglia periodicamente — o meglio, attende su un `Condvar` con un timeout calcolato dinamicamente sulla scadenza più vicina tra tutti i lock occupati, con la stessa tecnica già vista in `DelayQueue` (008) — e libera (incrementando la generazione, per invalidare ogni `Lease` ancora in giro) qualunque lock la cui scadenza sia trascorsa, notificando poi gli eventuali attendenti.

---

## Meta-commentario

**Perché serve una generazione, e non basta un semplice flag "scaduto":** senza di essa, una `renew()` che arriva esattamente mentre il reaper sta liberando lo stesso lock rischierebbe una corsa critica: se il reaper libera il lock e un nuovo chiamante lo riacquisisce prima che la `renew()` del vecchio possessore venga eseguita, quella `renew()` — se verificasse solo "il lock è occupato?" — troverebbe "sì" e rinnoverebbe per errore la lease del *nuovo* possessore, prolungandola a sua insaputa. Confrontare la generazione anziché il solo stato "occupato/libero" rende visibile la differenza tra "questo è ancora il mio possesso" e "questo lock è stato riassegnato da quando l'ho acquisito", esattamente come il contatore di generazione in `Rendezvous` evitava che un thread rientrasse nel round sbagliato.

**Perché tre meccanismi di risoluzione (rinnovo, `Drop`, scadenza automatica) devono coesistere senza contraddirsi:** ogni problema precedente della serie sceglieva un solo meccanismo di risoluzione per handle (RAII *oppure* azione esplicita, mai più di un tipo). Qui il possessore può rinnovare esplicitamente (estendendo la vita), può uscire dallo scope prima della scadenza (terminandola in anticipo), oppure può semplicemente non fare nulla e lasciare che il reaper agisca al suo posto — tre percorsi che devono tutti convergere sullo stesso stato coerente, verificato dalla stessa generazione, senza che uno "sorprenda" gli altri.

**Perché conta come esercizio capstone:** il thread di reaper con attesa a scadenza dinamica, combinato con la generazione per prevenire corse, combinato con tre vie di risoluzione indipendenti, eccede quasi certamente il tempo di un singolo appello. Budget stimato: 120–150 minuti.
