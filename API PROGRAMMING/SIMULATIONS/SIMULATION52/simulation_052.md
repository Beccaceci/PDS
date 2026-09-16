# Simulazione 052 — SwarmCoordinator (capstone)

*Esattamente due tratti pubblici, come `TaskGraph` (046), `LockGraph` (048), `SupervisorTree` (049), `SegmentedWal` (050) e `CacheBus` (051) — ma qui il problema architetturale esplora le reti P2P e la distribuzione decentralizzata di artefatti (stile BitTorrent, IPFS e i cluster di distribuzione per immagini container come Uber Kraken e Twitter Murder): un coordinatore di sciame (Swarm) che gestisce il download concorrente di pezzi frammentati in blocchi, la prioritizzazione "Rarest-First", la modalità speculativa "Endgame Mode" con cancellazione incrociata istantanea dei blocchi ridondanti, il failover dinamico su choke/unchoke e la verifica crittografica di integrità con recupero da blocchi corrotti. La sfida è governare la competizione e la cooperazione di decine di peer concorrenti senza stalli né attese attive.*

---

## SwarmCoordinator

Nei sistemi peer-to-peer (P2P) ad alta efficienza per la distribuzione di file o artefatti di grandi dimensioni (ad esempio immagini container o modelli di machine learning distribuiti a migliaia di nodi), il file complessivo viene suddiviso in un numero prefissato di **pezzi** (`PieceId`), ciascuno dei quali è a sua volta frammentato in $K$ **blocchi** (`BlockId`).

Ogni nodo che partecipa allo sciame (un **peer**) possiede solo un sottoinsieme dei pezzi disponibili e comunica la propria disponibilità (bitfield) al coordinatore locale. Per massimizzare il throughput e garantire il completamento del download nel minor tempo possibile, il coordinatore deve orchestrare quattro meccanismi cooperanti:

1. **Schedulazione Rarest-First**:
   - Per evitare che pezzi rari vadano perduti quando un peer abbandona la rete (il classico problema dell'"ultimo pezzo"), il coordinatore deve tenere traccia di quanti peer possiedono ciascun pezzo.
   - Quando un peer richiede il prossimo blocco da scaricare (`schedule_next_block`), il coordinatore sceglie prioritariamente i blocchi appartenenti al pezzo **più raro** (posseduto dal minor numero di peer, purché maggiore di zero) tra quelli ancora non completati.
2. **Pipelining & Gestione In-Flight**:
   - Ciascun peer può avere più richieste di blocchi "in volo" contemporaneamente per saturare la banda di rete.
   - Se un peer subisce un blocco temporaneo (**Choke**), tutte le sue richieste di blocchi in volo vengono revocate e riassegnate immediatamente ad altri peer non bloccati (**Unchoked**).
3. **Endgame Mode & Cancellazione Speculativa**:
   - Quando i blocchi mancanti dell'intero torrent scendono al di sotto di una soglia prefissata (`endgame_threshold`), il tempo di attesa dell'ultimo blocco lento (straggler) dominerebbe il tempo totale.
   - Il coordinatore entra automaticamente in **Endgame Mode**: in questa fase è consentito richiedere contemporaneamente lo stesso blocco a **tutti** i peer disponibili che possiedono quel pezzo.
   - Nel momento esatto in cui il *primo* peer consegna il blocco (`deliver_block`), il blocco viene registrato come completato e il coordinatore **cancella istantaneamente** tutte le richieste ridondanti in volo per quel blocco verso gli altri peer (`cancel_request`).
4. **Assemblaggio, Verifica di Integrità & Ripristino da Corruzione**:
   - Quando tutti i blocchi di un pezzo sono stati consegnati, il pezzo viene assemblato concatenando i payload dei blocchi e sottoposto a una funzione di verifica (`validator`).
   - Se la verifica ha successo, il pezzo transita nello stato `Verified` e i thread bloccati in attesa del pezzo o del torrent completo vengono risvegliati.
   - Se la verifica fallisce (pezzo corrotto o manomesso), il pezzo viene scartato, tutti i suoi blocchi vengono riportati allo stato `Missing` per essere riscaricati, e la consegna fallita restituisce `Err(SwarmError::CorruptedPiece)`.

Si scrivano in Rust le strutture che implementano i tratti `PeerSession` e `SwarmCoordinator` definiti di seguito.

---

### API richiesta

```rust
pub type PeerId = u64;
pub type PieceId = u64;
pub type BlockId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceState {
    Missing,
    InProgress,
    Verified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmError {
    /// Il peer è attualmente 'choked' (bloccato); non può scaricare né consegnare blocchi.
    PeerChoked,
    /// Il blocco consegnato è duplicato, obsoleto o già integrato da un altro peer.
    DuplicateBlock,
    /// Il pezzo completato non ha superato la verifica crittografica di integrità.
    CorruptedPiece,
    /// Parametro o identificatore non valido o fuori dai limiti.
    InvalidParameter,
}

pub trait PeerSession: Send + Sync {
    // Sblocca questo peer (unchoke): il peer diventa idoneo a ricevere richieste di blocchi.
    fn unchoke(&self);

    // Blocca questo peer (choke): revoca immediatamente tutte le richieste di blocchi
    // attualmente in volo verso questo peer, rendendole nuovamente disponibili per altri peer.
    fn choke(&self);

    // Restituisce true se questo peer si trova attualmente nello stato choked.
    fn is_choked(&self) -> bool;

    // Consegna al coordinatore un blocco scaricato da questo peer.
    // - In modalità ordinaria: memorizza il blocco per il pezzo di appartenenza.
    // - In Endgame Mode: il primo blocco che arriva vince; le richieste ridondanti in volo
    //   dello stesso blocco verso tutti gli altri peer vengono cancellate immediatamente.
    // Se la consegna completa l'ultimo blocco mancante del pezzo, viene invocata la funzione
    // di verifica dell'integrità: se ha successo, il pezzo transita a Verified; se fallisce,
    // il pezzo viene azzerato e la chiamata restituisce Err(SwarmError::CorruptedPiece).
    fn deliver_block(&self, piece: PieceId, block: BlockId, data: Vec<u8>) -> Result<(), SwarmError>;

    // Restituisce l'elenco dei blocchi attualmente in volo (richiesti e non ancora consegnati) assegnati a questo peer.
    fn inflight_requests(&self) -> Vec<(PieceId, BlockId)>;

    // Annulla esplicitamente una specifica richiesta di blocco in volo verso questo peer.
    fn cancel_request(&self, piece: PieceId, block: BlockId) -> bool;
}

pub trait SwarmCoordinator: Clone + Send + Sync {
    // Registra un nuovo peer nello sciame, specificando l'elenco dei pezzi che esso possiede.
    // Incrementa i contatori di disponibilità dei pezzi per la prioritizzazione Rarest-First.
    // Restituisce il PeerId univoco assegnato e il rispettivo handle PeerSession (inizialmente unchoked).
    fn register_peer(&self, available_pieces: &[PieceId]) -> (PeerId, impl PeerSession + 'static);

    // Schedula e riserva il prossimo blocco da richiedere per il peer specificato.
    // - Applica la priorità Rarest-First: sceglie prioritariamente i pezzi posseduti da meno peer.
    // - In Endgame Mode: consente di richiedere blocchi già in volo verso altri peer per hedging speculativo.
    // Restituisce None se il peer è choked, se non possiede pezzi con blocchi mancanti, o se non ci sono blocchi disponibili.
    fn schedule_next_block(&self, peer: PeerId) -> Option<(PieceId, BlockId)>;

    // Blocca il chiamante, senza consumare cicli di CPU, finché lo specifico pezzo non è
    // completamente scaricato e verificato con successo, restituendo i dati concatenati del pezzo.
    fn wait_for_piece(&self, piece: PieceId) -> Result<Vec<u8>, SwarmError>;

    // Blocca il chiamante, senza consumare cicli di CPU, finché tutti i pezzi del torrent
    // non sono stati completamente scaricati e verificati.
    fn wait_completed(&self);

    // Restituisce lo stato attuale del pezzo (Missing, InProgress, Verified).
    fn piece_state(&self, piece: PieceId) -> PieceState;

    // Restituisce true se lo sciame si trova attualmente in Endgame Mode.
    fn in_endgame_mode(&self) -> bool;

    // Restituisce il numero di blocchi totali ancora mancanti per completare il torrent.
    fn remaining_blocks_count(&self) -> usize;

    // Restituisce il numero di peer attualmente registrati e attivi.
    fn active_peer_count(&self) -> usize;
}

pub fn make_swarm_coordinator(
    piece_count: usize,
    blocks_per_piece: usize,
    endgame_threshold: usize,
    validator: impl Fn(PieceId, &[u8]) -> bool + Send + Sync + 'static,
) -> impl SwarmCoordinator {
    ...
}
```

---

### Requisiti

- **Priorità Rarest-First**:
  - Ogni pezzo $P \in [0, \text{piece\_count})$ possiede un conteggio di disponibilità corrispondente a quanti peer registrati lo possiedono.
  - La funzione `schedule_next_block(peer)` deve selezionare tra i pezzi posseduti da quel peer quello con il **minore conteggio di disponibilità strettamente positivo**, tra quelli non ancora completati.
- **Endgame Mode**:
  - La modalità Endgame si attiva non appena `remaining_blocks_count() <= endgame_threshold`.
  - In Endgame Mode, un blocco può essere richiesto concorrentemente a più peer che possiedono il relativo pezzo.
  - Non appena uno qualunque dei peer consegna il blocco tramite `deliver_block()`, il blocco viene salvato, e le richieste corrispondenti in volo su tutti gli altri peer vengono cancellate istantaneamente.
- **Failover su Choke**:
  - Quando viene invocato `choke()` su una `PeerSession`, tutti i blocchi che erano in volo verso quel peer vengono revocati e tornano immediatamente disponibili per essere schedulati ad altri peer idonei.
- **Verifica di Integrità & Ripristino Fuori dal Lock**:
  - Quando l'ultimo blocco di un pezzo viene consegnato, i dati dei blocchi vengono assemblati nell'ordine $0, 1, \dots, K-1$.
  - Il `validator` deve essere invocato **rilasciando preventivamente il lock dello stato**, per non congelare lo sciame durante calcoli crittografici intensivi.
  - Se il validatore restituisce `false`, il pezzo torna allo stato `Missing`, tutti i suoi blocchi tornano disponibili e `deliver_block` restituisce `Err(SwarmError::CorruptedPiece)`.
- **Gestione RAII (`Drop`) del `PeerSession`**:
  - Quando una `PeerSession` viene distrutta (`Drop`), essa viene deregistrata dallo sciame:
    1. Tutti i suoi blocchi in volo vengono revocati;
    2. I contatori di disponibilità dei pezzi posseduti da quel peer vengono decrementati;
    3. `active_peer_count()` viene decrementato.
- **Thread-Safety & Assenza di Busy-Waiting**:
  - Thread-safe, condivisibile (`Clone + Send + Sync`).
  - `wait_for_piece` e `wait_completed` devono sospendere il chiamante su `Condvar` senza consumare cicli di CPU finché la condizione attesa non si verifica.
  - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
  - Se il codice consegnato non compila, non verrà valutato.

---

### Suggerimenti implementativi

1. Rappresentare ciascun pezzo con una struttura interna contenente: lo stato (`Missing`, `InProgress`, `Verified`), il conteggio dei peer che lo possiedono (`availability`), una tabella dei blocchi (ciascuno con il proprio buffer di byte opzionale e lo stato di in-flight) e una `Condvar` per svegliare chi attende quel pezzo.
2. Rappresentare ciascun peer con una struttura interna contenente: il flag `is_choked`, il set o lista dei pezzi posseduti, e l'insieme dei blocchi attualmente in volo `HashSet<(PieceId, BlockId)>`.
3. Nella funzione `deliver_block`, se il blocco era già stato ricevuto (ad esempio in Endgame Mode da un altro peer più veloce), restituire `Err(SwarmError::DuplicateBlock)` senza corrompere il dato.
4. Quando un blocco arriva in Endgame Mode, scorrere tutti i peer attivi: se hanno quel `(piece, block)` nel loro insieme di richieste in volo, rimuoverlo.

---

## Meta-commentario

**Perché `SwarmCoordinator` raggiunge la massima complessità architetturale (Score 5.0+):**
I capstone precedenti hanno esplorato grafi di dipendenze aciclici (`TaskGraph`), grafi ciclici con deadlock (`LockGraph`), alberi gerarchici con crash a cascata (`SupervisorTree`) e log sequenziali con contropressione (`SegmentedWal`). `SwarmCoordinator` introduce un paradigma ancora diverso: la **coordinazione speculativa multi-peer con cancellazione incrociata**:
- L'allocazione del lavoro non è deterministica né statica: dipende dinamicamente dalla disponibilità distribuita (Rarest-First) e dallo stato dinamico dei canali (Choke/Unchoke).
- L'Endgame Mode richiede che una singola mutazione di stato (la consegna del primo blocco vincente) si propaghi all'indietro cancellando le intenzioni di scheduling su tutti gli altri thread peer in concorrenza, prevenendo lo spreco di banda e lo stallo dell'intero sistema.

**La scoperta architetturale chiave: la dissociazione tra richiesta di blocco e possesso definitivo:**
Un'implementazione ingenua assegnerebbe un blocco a un peer in modo esclusivo e irrevocabile. Nel mondo reale delle reti decentralizzate, un peer può rallentare, subire uno stallo di rete o inviare dati corrotti. La struttura deve mantenere la richiesta come una prenotazione revocabile (*in-flight lease*), capace di essere disdetta da tre eventi distinti:
1. Il verificarsi di un evento di Choke;
2. La vittoria di un peer concorrente in Endgame Mode;
3. La disconnessione o distruzione RAII (`Drop`) dell'handle del peer.
Tutti e tre i percorsi devono convergere sulla stessa logica di bonifica e riaccodamento.

**Strutture cooperanti stimate (senza prescriverle):**
1. `SwarmState`: contiene la tabella dei pezzi, la mappa dei peer registrati, il contatore dei blocchi mancanti, la soglia di endgame e la Condvar di completamento globale.
2. `PieceData`: contiene lo stato del pezzo, i blocchi memorizzati, la mappa dei blocchi in volo e la Condvar per i lettori di quel pezzo.
3. `PeerData`: contiene lo stato `choked`, i pezzi posseduti e le richieste in volo del peer.
4. `MyPeerSession`: handle del peer che implementa `PeerSession`, con `Drop` per la deregistrazione pulita.

**Difficoltà stimata:** 5.0+ / 5.0. Budget stimato: 130–160 minuti.
