//! # Simulazione 052 — SwarmCoordinator (capstone)
//!
//! Nei sistemi peer-to-peer (P2P) ad alta efficienza per la distribuzione di file o artefatti
//! di grandi dimensioni (ad esempio immagini container o modelli di machine learning distribuiti
//! a migliaia di nodi), il file complessivo viene suddiviso in un numero prefissato di pezzi
//! (`PieceId`), ciascuno dei quali è a sua volta frammentato in K blocchi (`BlockId`).
//!
//! Ogni nodo che partecipa allo sciame (un peer) possiede solo un sottoinsieme dei pezzi disponibili
//! e comunica la propria disponibilità (bitfield) al coordinatore locale. Per massimizzare il throughput
//! e garantire il completamento del download nel minor tempo possibile, il coordinatore deve orchestrare
//! quattro meccanismi cooperanti:
//! 1. Schedulazione Rarest-First: prioritizzazione dei pezzi posseduti da meno peer.
//! 2. Pipelining & Gestione In-Flight: tracciamento dei blocchi in volo e revoca su Choke.
//! 3. Endgame Mode & Cancellazione Speculativa: duplicazione hedging dei blocchi contesi e
//!    cancellazione incrociata istantanea alla prima consegna.
//! 4. Assemblaggio, Verifica di Integrità & Ripristino: convalida fuori dal lock e rollback su corruzione.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `PeerSession` e `SwarmCoordinator`.

use std::{collections::{HashMap, VecDeque}, sync::{Arc, Condvar, Mutex, atomic::AtomicBool}};

use crate::{PieceState::{Missing, Verified}, SwarmError::InvalidParameter};

pub type PeerId = u64;
pub type PieceId = u64;
pub type BlockId = u32;

/// Stato di avanzamento e integrità di un pezzo del torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceState {
    Missing,
    InProgress,
    Verified,
}

/// Errori operativi restituiti durante le transazioni dello sciame P2P.
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

/// Tratto che rappresenta la sessione attiva di un peer connesso allo sciame.
pub trait PeerSession: Send + Sync {
    /// Sblocca questo peer (unchoke): il peer diventa idoneo a ricevere richieste di blocchi.
    fn unchoke(&self);

    /// Blocca questo peer (choke): revoca immediatamente tutte le richieste di blocchi
    /// attualmente in volo verso questo peer, rendendole nuovamente disponibili per altri peer.
    fn choke(&self);

    /// Restituisce true se questo peer si trova attualmente nello stato choked.
    fn is_choked(&self) -> bool;

    /// Consegna al coordinatore un blocco scaricato da questo peer.
    /// - In modalità ordinaria: memorizza il blocco per il pezzo di appartenenza.
    /// - In Endgame Mode: il primo blocco che arriva vince; le richieste ridondanti in volo
    ///   dello stesso blocco verso tutti gli altri peer vengono cancellate immediatamente.
    /// Se la consegna completa l'ultimo blocco mancante del pezzo, viene invocata la funzione
    /// di verifica dell'integrità: se ha successo, il pezzo transita a Verified; se fallisce,
    /// il pezzo viene azzerato e la chiamata restituisce Err(SwarmError::CorruptedPiece).
    fn deliver_block(&self, piece: PieceId, block: BlockId, data: Vec<u8>) -> Result<(), SwarmError>;

    /// Restituisce l'elenco dei blocchi attualmente in volo (richiesti e non ancora consegnati)
    /// assegnati a questo peer.
    fn inflight_requests(&self) -> Vec<(PieceId, BlockId)>;

    /// Annulla esplicitamente una specifica richiesta di blocco in volo verso questo peer.
    fn cancel_request(&self, piece: PieceId, block: BlockId) -> bool;
}

/// Tratto che rappresenta il coordinatore centrale dello sciame P2P.
pub trait SwarmCoordinator: Clone + Send + Sync {
    /// Registra un nuovo peer nello sciame, specificando l'elenco dei pezzi che esso possiede.
    /// Incrementa i contatori di disponibilità dei pezzi per la prioritizzazione Rarest-First.
    /// Restituisce il PeerId univoco assegnato e il rispettivo handle PeerSession (inizialmente unchoked).
    fn register_peer(&self, available_pieces: &[PieceId]) -> (PeerId, impl PeerSession + 'static);

    /// Schedula e riserva il prossimo blocco da richiedere per il peer specificato.
    /// - Applica la priorità Rarest-First: sceglie prioritariamente i pezzi posseduti da meno peer.
    /// - In Endgame Mode: consente di richiedere blocchi già in volo verso altri peer per hedging speculativo.
    /// Restituisce None se il peer è choked, se non possiede pezzi con blocchi mancanti, o se non ci
    /// sono blocchi disponibili.
    fn schedule_next_block(&self, peer: PeerId) -> Option<(PieceId, BlockId)>;

    /// Blocca il chiamante, senza consumare cicli di CPU, finché lo specifico pezzo non è
    /// completamente scaricato e verificato con successo, restituendo i dati concatenati del pezzo.
    fn wait_for_piece(&self, piece: PieceId) -> Result<Vec<u8>, SwarmError>;

    /// Blocca il chiamante, senza consumare cicli di CPU, finché tutti i pezzi del torrent
    /// non sono stati completamente scaricati e verificati.
    fn wait_completed(&self);

    /// Restituisce lo stato attuale del pezzo (Missing, InProgress, Verified).
    fn piece_state(&self, piece: PieceId) -> PieceState;

    /// Restituisce true se lo sciame si trova attualmente in Endgame Mode.
    fn in_endgame_mode(&self) -> bool;

    /// Restituisce il numero di blocchi totali ancora mancanti per completare il torrent.
    fn remaining_blocks_count(&self) -> usize;

    /// Restituisce il numero di peer attualmente registrati e attivi.
    fn active_peer_count(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct PeerState {
    is_choked: bool,
    requests_on_fly: VecDeque<(PieceId, BlockId)>
}

impl PeerState {
    pub fn new () -> Self {
        Self {
            is_choked: false,
            requests_on_fly: VecDeque::new()
        }
    }
}

pub struct MyPeer {
    peer_id: PeerId,
    available_pieces: Vec<PieceId>,
    peer_state: Arc<(Mutex<PeerState>, Condvar)>,
    shared_coordinator: MySwarmCoordinator
}

impl PeerSession for MyPeer {
    fn unchoke(&self) {
        let mut guard = self.peer_state.0.lock().unwrap();
        guard.is_choked = false;
    }

    fn choke(&self) {
        let mut guard = self.peer_state.0.lock().unwrap();
        guard.is_choked = true;
        guard.requests_on_fly.clear();
    }

    fn is_choked(&self) -> bool {
        let guard = self.peer_state.0.lock().unwrap();
        guard.is_choked
    }

    fn deliver_block(&self, piece: PieceId, block: BlockId, data: Vec<u8>) -> Result<(), SwarmError> {
        todo!()
    }

    fn inflight_requests(&self) -> Vec<(PieceId, BlockId)> {
        let guard = self.peer_state.0.lock().unwrap();
    
        let mut inflight_requests = Vec::new();
        for (piece_id, block_id) in guard.requests_on_fly.iter() {
            inflight_requests.push((piece_id.clone(), block_id.clone()));
        }
        inflight_requests
    }

    fn cancel_request(&self, piece: PieceId, block: BlockId) -> bool {
        let mut guard = self.peer_state.0.lock().unwrap();
    
        for (index, (piece_id, block_id)) in guard.requests_on_fly.iter().enumerate() {
            if *piece_id == piece && *block_id == block {
                guard.requests_on_fly.remove(index);
                return true;
            }
        }
        false
    }
}

impl Clone for MyPeer {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            available_pieces: self.available_pieces.clone(),
            peer_state: self.peer_state.clone(),
            shared_coordinator: self.shared_coordinator.clone()
        }
    }
}

impl Drop for MyPeer {
    fn drop(&mut self) {
        todo!()
    }
}

pub struct CoordinatorState {
    next_peer_id: PeerId,
    peers: HashMap<PeerId, MyPeer>,
    pieces: HashMap<PieceId, (PieceState, Vec<u8>)>, // (piece_state, received_blocks)
    in_endgame_mode: bool
}

impl CoordinatorState {
    pub fn with_number_of_pieces (number_of_pieces: usize) -> Self {
        let mut pieces = HashMap::new();
        for piece_id in 0..number_of_pieces as PieceId {
            pieces.insert(piece_id, (Missing, Vec::<u8>::new()));
        }
        
        Self {
            next_peer_id: 0,
            peers: HashMap::new(),
            pieces,
            in_endgame_mode: false
        }
    }

    pub fn is_verified (&self, piece_id: PieceId) -> bool {
        if let Some((status, _)) = self.pieces.get(&piece_id) {
            if matches!(*status, Verified) {
                return true;
            }
        }
        false
    }

    pub fn is_completed (&self, piece_count: usize) -> bool {
        for piece_id in 0..piece_count as PieceId {
            if !self.is_verified(piece_id) {
                return false
            }
        }
        true
    }
}

pub struct MySwarmCoordinator {
    inner: Arc<(Mutex<CoordinatorState>, Condvar)>,
    piece_count: usize,
    blocks_per_piece: usize,
    endgame_threshold: usize,
    validator: Arc<Box<dyn Fn(PieceId, &[u8]) -> bool + Send + Sync + 'static>>
}

impl SwarmCoordinator for MySwarmCoordinator {
    fn register_peer(&self, available_pieces: &[PieceId]) -> (PeerId, impl PeerSession + 'static) {
        let (mutex, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        let actual_peer_id = guard.next_peer_id;
        guard.next_peer_id += 1;

        let new_peer = MyPeer {
            peer_id: actual_peer_id,
            available_pieces: available_pieces.to_vec(),
            peer_state: Arc::new((Mutex::new(PeerState::new()), Condvar::new())),
            shared_coordinator: self.clone()
        };
        guard.peers.insert(actual_peer_id, new_peer.clone());
        (actual_peer_id, new_peer)
    }

    fn schedule_next_block(&self, peer: PeerId) -> Option<(PieceId, BlockId)> {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        let target_peer = guard.peers.get(&peer).unwrap();

        if target_peer.peer_state.0.lock().unwrap().is_choked {
            return None;
        }

        let available_pieces = target_peer.available_pieces.clone();

        let mut min_blocks_received = self.blocks_per_piece;
        let mut target_piece_block = None;

        for piece_id in available_pieces {
            if let Some((status, received_blocks)) = guard.pieces.get(&piece_id) {
                if matches!(*status, Missing) {
                    let num_received_blocks = received_blocks.len();
                    if num_received_blocks < min_blocks_received {
                        min_blocks_received = num_received_blocks;
                        target_piece_block = Some((piece_id, num_received_blocks as BlockId));
                    }
                }
            }
        }

        target_piece_block
    }

    fn wait_for_piece(&self, piece: PieceId) -> Result<Vec<u8>, SwarmError> {
        if (piece as usize) >= self.piece_count {
            return Err(InvalidParameter);
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard = cvar.wait_while(guard, |c| {
            !c.is_verified(piece)
        }).unwrap();

        let (_, received_blocks) = guard.pieces.get(&piece).unwrap();
        Ok(received_blocks.clone())
    }

    fn wait_completed(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut _guard = mutex.lock().unwrap();
        _guard = cvar.wait_while(_guard, |c| {
            !c.is_completed(self.piece_count)
        }).unwrap();
    }

    fn piece_state(&self, piece: PieceId) -> PieceState {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        let (state, _) = guard.pieces.get(&piece).unwrap();
        *state
    }

    fn in_endgame_mode(&self) -> bool {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.in_endgame_mode
    }

    fn remaining_blocks_count(&self) -> usize {
        let mut remaining_blocks = 0usize;
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();

        for piece_id in 0..self.piece_count as PieceId {
            if guard.is_verified(piece_id) {
                remaining_blocks += 1;
            }
        }

        remaining_blocks
    }

    fn active_peer_count(&self) -> usize {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.peers.len()
    }
}

impl Clone for MySwarmCoordinator {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            piece_count: self.piece_count,
            blocks_per_piece: self.blocks_per_piece,
            endgame_threshold: self.endgame_threshold,
            validator: self.validator.clone()
        }
    }
}

/// Inizializza un nuovo coordinatore di sciame per un file suddiviso in `piece_count` pezzi,
/// con ciascun pezzo frammentato in `blocks_per_piece` blocchi.
/// La modalità Endgame si attiva non appena i blocchi mancanti totali scendono sotto `endgame_threshold`.
/// `validator` è la funzione crittografica invocata per convalidare ciascun pezzo al completamento.
pub fn make_swarm_coordinator(
    piece_count: usize,
    blocks_per_piece: usize,
    endgame_threshold: usize,
    validator: impl Fn(PieceId, &[u8]) -> bool + Send + Sync + 'static,
) -> impl SwarmCoordinator {
    MySwarmCoordinator {
        inner: Arc::new((Mutex::new(CoordinatorState::with_number_of_pieces(piece_count)), Condvar::new())),
        piece_count,
        blocks_per_piece,
        endgame_threshold,
        validator: Arc::new(Box::new(validator))
    }
}
