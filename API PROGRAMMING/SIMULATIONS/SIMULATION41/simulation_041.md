# Simulazione 047 — RpcClient

*Dominio nuovo: la tabella di correlazione richiesta/risposta al cuore di ogni client RPC — inviare una richiesta con un identificatore, ricevere risposte in arrivo su un canale separato (magari da un altro thread, magari fuori ordine), e far incontrare le due cose. La tensione centrale: una richiesta pendente ha due modi di concludersi — l'attesa esplicita della risposta, o l'abbandono silenzioso — e devono convergere sullo stesso stato senza contraddirsi, anche quando una risposta arriva nell'istante esatto in cui il chiamante rinuncia ad attenderla.*

---

## RpcClient

Un client che comunica con un servizio remoto invia richieste identificate da un numero di correlazione, e riceve le risposte corrispondenti in un momento successivo, possibilmente fuori ordine, possibilmente consegnate da un thread diverso da quello che ha inviato la richiesta originale (ad esempio un thread dedicato alla lettura della connessione di rete). Il client deve far corrispondere ogni risposta in arrivo alla richiesta pendente giusta, senza che chi ha inviato la richiesta debba occuparsi direttamente della demultiplazione.

Si scrivano in Rust le strutture che implementano i tratti `PendingRequest<Resp>` e `RpcClient<Req, Resp>` definiti di seguito.

### API richiesta

```rust
pub trait PendingRequest<Resp: Send> {
    // Consuma questo oggetto, bloccando il chiamante, senza consumare
    // cicli di CPU, finché la risposta correlata non arriva tramite
    // deliver_response sullo stesso identificatore, oppure finché il
    // client non viene chiuso — in tal caso restituisce None.
    fn wait_response(self) -> Option<Resp>;
}

pub trait RpcClient<Req: Send, Resp: Send>: Clone {
    // Invia una richiesta, assegnandole un identificatore di correlazione
    // univoco generato internamente, e la consegna tramite `transport`
    // (fornita dal chiamante, che si assume la recapiti al servizio
    // remoto insieme all'identificatore, in un modo non specificato da
    // questo esercizio). Restituisce un oggetto che permette di attendere
    // la risposta corrispondente.
    fn send(
        &self,
        request: Req,
        transport: impl FnOnce(u64, Req) + Send + 'static,
    ) -> impl PendingRequest<Resp>;

    // Consegna la risposta ricevuta per l'identificatore di correlazione
    // dato, risvegliando chi è in attesa di quella specifica richiesta,
    // se esiste ancora. Se nessuna richiesta con quell'id è pendente (già
    // risolta, già abbandonata, o mai esistita), non ha alcun effetto. Non
    // blocca mai.
    fn deliver_response(&self, id: u64, response: Resp);

    // Chiude il client: tutte le richieste attualmente pendenti (e quelle
    // future, se send() venisse ancora chiamato) ricevono None da
    // wait_response invece di una risposta.
    fn close(&self);
}

pub fn make_rpc_client<Req: Send, Resp: Send>() -> impl RpcClient<Req, Resp> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- `send()` non deve mai mantenere alcun lock condiviso durante l'esecuzione di `transport`: l'inserimento della richiesta nello stato pendente e l'invocazione di `transport` sono operazioni distinte, la seconda mai eseguita sotto lock.
- Se un `PendingRequest<Resp>` viene abbandonato (esce dallo scope senza che `wait_response` sia mai stato chiamato), il proprio identificatore di correlazione deve essere rimosso dallo stato pendente — una `deliver_response` successiva per quello stesso id non deve avere alcun effetto osservabile né causare panico.
- Se `deliver_response` arriva per un id ancora pendente esattamente mentre il corrispondente `PendingRequest` sta per essere abbandonato (uscita dallo scope concorrente alla consegna), l'esito deve essere uno dei due coerentemente — mai entrambi, mai nessuno dei due, mai uno stato intermedio.
- `deliver_response` non blocca mai.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con una mappa `id -> stato` (`InAttesa` oppure `Risolto(Resp)`), un flag di chiusura, protetti da `Mutex` + `Condvar` dentro un `Arc`. `send()`: sotto lock, genera un nuovo id e inserisce `InAttesa`; fuori dal lock, invoca `transport`; restituisce un `PendingRequest` che ricorda l'id. `wait_response(self)`: attende (`wait_while`) finché lo stato per il proprio id non è `Risolto` o il client non è chiuso, poi rimuove la voce dalla mappa (che essa stessa abbia risolto l'attesa o meno) e restituisce il risultato — la rimozione dalla mappa deve avvenire qui, non nel `Drop`, dato che `wait_response` consuma `self` e quindi lo previene. Il `Drop` di `PendingRequest` (eseguito solo se `wait_response` non è mai stato chiamato) deve invece rimuovere la propria voce dalla mappa se ancora presente — usando la stessa tecnica `Option`/flag già vista più volte per distinguere "già gestito da `wait_response`" da "ancora da gestire secondo il default dell'abbandono".

---

## Meta-commentario

**Perché `wait_response` consuma `self` invece di prendere `&self`:** a differenza di quasi ogni altro handle della serie, qui l'azione "attendi la risposta" ha senso al più una volta — attenderla una seconda volta non avrebbe alcun valore osservabile aggiuntivo, e consumare `self` lo rende impossibile per costruzione, invece di doverlo prevenire con un controllo a runtime. È lo stesso principio già visto in `TransactionalQueue::commit` (006) e `Token::done` (009): un'azione che si offre come alternativa a `Drop` funziona meglio se il tipo stesso ne impedisce la ripetizione.

**Perché la corsa tra `deliver_response` e l'abbandono è il vero cuore del problema, non un caso limite:** in un client reale, una risposta può arrivare in un momento arbitrario rispetto a quando il chiamante decide di non aspettarla più (timeout applicato altrove, errore che porta a un early return, panic durante l'elaborazione di un'altra richiesta che fa terminare lo scope). Un'implementazione che rimuove la voce dalla mappa nel `Drop` senza verificare sotto lock se nel frattempo è già arrivata una risposta rischia di perderla silenziosamente proprio nell'istante in cui sarebbe arrivata comunque troppo tardi per essere utile — comportamento accettabile se documentato, ma solo se è una scelta di design deliberata, non un effetto collaterale di un ordine di operazioni non pensato.

**Difficoltà stimata:** paragonabile a `TransactionalQueue` per l'onere concettuale della doppia via di risoluzione, con l'ulteriore attenzione richiesta dal disaccoppiamento tra chi invia (`send`) e chi consegna la risposta (`deliver_response`), tipicamente thread diversi. Budget consigliato: 90–110 minuti.
