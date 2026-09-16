use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use simulation_052::{
    make_swarm_coordinator, PeerSession, PieceId, PieceState, SwarmCoordinator,
    SwarmError,
};

/// 1. Verifica statica dei bounds Send e Sync per i tratti.
#[test]
fn test_send_sync_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_clone<T: Clone>() {}

    fn check<C: SwarmCoordinator, S: PeerSession>() {
        assert_send::<C>();
        assert_sync::<C>();
        assert_clone::<C>();
        assert_send::<S>();
        assert_sync::<S>();
    }

    let coord = make_swarm_coordinator(1, 1, 0, |_p, _d| true);
    let (_pid, session) = coord.register_peer(&[0]);
    check_val(&coord, &session);
    fn check_val<C: SwarmCoordinator, S: PeerSession>(_c: &C, _s: &S) {
        check::<C, S>();
    }
}

/// 2. Ciclo di vita base: download di un pezzo a 2 blocchi e verifica con successo.
#[test]
fn test_single_peer_single_piece_download_and_verify() {
    // 1 pezzo, 2 blocchi, soglia endgame 0 (disabilitata)
    let coord = make_swarm_coordinator(1, 2, 0, |p, data| {
        p == 0 && data == [10, 20, 30, 40]
    });

    assert_eq!(coord.active_peer_count(), 0);
    assert_eq!(coord.remaining_blocks_count(), 2);
    assert_eq!(coord.piece_state(0), PieceState::Missing);

    let (pid, peer) = coord.register_peer(&[0]);
    assert_eq!(coord.active_peer_count(), 1);
    assert!(!peer.is_choked());

    // Schedula primo blocco
    let b1 = coord.schedule_next_block(pid);
    assert_eq!(b1, Some((0, 0)));
    assert_eq!(peer.inflight_requests(), vec![(0, 0)]);
    assert_eq!(coord.piece_state(0), PieceState::InProgress);

    // Schedula secondo blocco
    let b2 = coord.schedule_next_block(pid);
    assert_eq!(b2, Some((0, 1)));
    assert_eq!(peer.inflight_requests().len(), 2);

    // Nessun altro blocco da schedulare
    assert_eq!(coord.schedule_next_block(pid), None);

    // Consegna blocco 0
    let res0 = peer.deliver_block(0, 0, vec![10, 20]);
    assert_eq!(res0, Ok(()));
    assert_eq!(peer.inflight_requests(), vec![(0, 1)]);
    assert_eq!(coord.remaining_blocks_count(), 1);

    // Consegna blocco 1 (completa il pezzo e scatena la verifica)
    let res1 = peer.deliver_block(0, 1, vec![30, 40]);
    assert_eq!(res1, Ok(()));
    assert_eq!(peer.inflight_requests(), vec![]);
    assert_eq!(coord.remaining_blocks_count(), 0);
    assert_eq!(coord.piece_state(0), PieceState::Verified);

    // Verifica estrazione dati
    let data = coord.wait_for_piece(0).unwrap();
    assert_eq!(data, vec![10, 20, 30, 40]);
}

/// 3. Prioritizzazione Rarest-First:
/// Pezzo 0 posseduto solo da Peer A (rarità 1)
/// Pezzo 1 posseduto da Peer A e Peer B (rarità 2)
/// Pezzo 2 posseduto solo da Peer B (rarità 1)
/// Peer A deve ricevere prioritariamente Pezzo 0; Peer B Pezzo 2.
#[test]
fn test_rarest_first_scheduling_priority() {
    let coord = make_swarm_coordinator(3, 1, 0, |_p, _d| true);

    let (peer_a_id, _peer_a) = coord.register_peer(&[0, 1]);
    let (peer_b_id, _peer_b) = coord.register_peer(&[1, 2]);

    // Peer A possiede pezzo 0 (avail 1) e pezzo 1 (avail 2). Deve scegliere pezzo 0!
    let sched_a = coord.schedule_next_block(peer_a_id);
    assert_eq!(sched_a, Some((0, 0)));

    // Peer B possiede pezzo 1 (avail 2) e pezzo 2 (avail 1). Deve scegliere pezzo 2!
    let sched_b = coord.schedule_next_block(peer_b_id);
    assert_eq!(sched_b, Some((2, 0)));

    // Se Peer A chiede di nuovo, Pezzo 0 è già in-flight, quindi sceglie Pezzo 1
    let sched_a2 = coord.schedule_next_block(peer_a_id);
    assert_eq!(sched_a2, Some((1, 0)));
}

/// 4. Choke revoca le richieste in volo e ne impedisce la consegna.
#[test]
fn test_choke_revokes_inflight_requests() {
    let coord = make_swarm_coordinator(1, 2, 0, |_p, _d| true);

    let (peer_a_id, peer_a) = coord.register_peer(&[0]);
    let (peer_b_id, peer_b) = coord.register_peer(&[0]);

    // Schedula blocco a Peer A
    let block = coord.schedule_next_block(peer_a_id);
    assert_eq!(block, Some((0, 0)));
    assert_eq!(peer_a.inflight_requests(), vec![(0, 0)]);

    // Choke su Peer A
    peer_a.choke();
    assert!(peer_a.is_choked());
    assert_eq!(peer_a.inflight_requests(), vec![]); // Revocate!

    // Impossibile schedulare mentre choked
    assert_eq!(coord.schedule_next_block(peer_a_id), None);

    // Impossibile consegnare mentre choked
    let err = peer_a.deliver_block(0, 0, vec![1, 2]);
    assert_eq!(err, Err(SwarmError::PeerChoked));

    // Il blocco 0 è tornato disponibile ed è ora assegnabile a Peer B
    let block_b = coord.schedule_next_block(peer_b_id);
    assert_eq!(block_b, Some((0, 0)));
    assert_eq!(peer_b.inflight_requests(), vec![(0, 0)]);

    // Unchoke di Peer A
    peer_a.unchoke();
    assert!(!peer_a.is_choked());
    // Ora Peer A può ricevere il blocco rimanente (0, 1)
    let block_a2 = coord.schedule_next_block(peer_a_id);
    assert_eq!(block_a2, Some((0, 1)));
}

/// 5. Endgame Mode e duplicazione speculativa (hedging).
#[test]
fn test_endgame_mode_activation_and_hedging() {
    // 2 pezzi, 2 blocchi ciascuno = 4 blocchi totali. Soglia endgame = 2.
    let coord = make_swarm_coordinator(2, 2, 2, |_p, _d| true);

    let (p1, peer1) = coord.register_peer(&[0, 1]);
    let (p2, peer2) = coord.register_peer(&[0, 1]);

    assert!(!coord.in_endgame_mode());
    assert_eq!(coord.remaining_blocks_count(), 4);

    // Schedula e scarica i primi 2 blocchi del pezzo 0
    coord.schedule_next_block(p1); // (0, 0)
    peer1.deliver_block(0, 0, vec![1]).unwrap();
    coord.schedule_next_block(p1); // (0, 1)
    peer1.deliver_block(0, 1, vec![2]).unwrap();

    // Ora restano 2 blocchi mancanti: scatta la soglia endgame!
    assert_eq!(coord.remaining_blocks_count(), 2);
    assert!(coord.in_endgame_mode());

    // In Endgame Mode, Peer 1 richiede il blocco (1, 0)
    let req1 = coord.schedule_next_block(p1);
    assert!(req1 == Some((1, 0)) || req1 == Some((1, 1)));
    let assigned = req1.unwrap();

    // Anche Peer 2 può richiedere lo STESSO blocco per hedging speculativo!
    let req2 = coord.schedule_next_block(p2);
    assert_eq!(req2, Some(assigned));

    assert!(peer1.inflight_requests().contains(&assigned));
    assert!(peer2.inflight_requests().contains(&assigned));
}

/// 6. Cancellazione speculativa istantanea in Endgame Mode:
/// Quando il primo peer consegna il blocco, la richiesta in volo sul peer concorrente viene cancellata.
#[test]
fn test_endgame_first_arrival_cancels_redundant_inflight_requests() {
    let coord = make_swarm_coordinator(1, 1, 1, |_p, _d| true);

    let (p1, peer1) = coord.register_peer(&[0]);
    let (p2, peer2) = coord.register_peer(&[0]);

    assert!(coord.in_endgame_mode());

    let req1 = coord.schedule_next_block(p1);
    let req2 = coord.schedule_next_block(p2);
    assert_eq!(req1, Some((0, 0)));
    assert_eq!(req2, Some((0, 0)));

    // Peer 1 consegna il blocco per primo!
    let res1 = peer1.deliver_block(0, 0, vec![42]);
    assert_eq!(res1, Ok(()));

    // La richiesta in volo verso Peer 2 deve essere stata cancellata istantaneamente dal coordinatore!
    assert_eq!(peer2.inflight_requests(), vec![]);

    // Se Peer 2 tenta tardivamente di consegnare il blocco ridondante, riceve DuplicateBlock
    let res2 = peer2.deliver_block(0, 0, vec![42]);
    assert_eq!(res2, Err(SwarmError::DuplicateBlock));
}

/// 7. Rilevamento di blocchi corrotti e ripristino dei blocchi a Missing.
#[test]
fn test_corrupted_piece_validation_failure_resets_blocks() {
    // Il validatore rigetta qualsiasi pezzo che contenga il byte 0xFF
    let coord = make_swarm_coordinator(1, 2, 0, |_p, data| !data.contains(&0xFF));

    let (p1, peer1) = coord.register_peer(&[0]);

    coord.schedule_next_block(p1); // (0, 0)
    peer1.deliver_block(0, 0, vec![1, 2]).unwrap();

    coord.schedule_next_block(p1); // (0, 1)
    // Consegna blocco corrotto con 0xFF
    let res_corrupted = peer1.deliver_block(0, 1, vec![0xFF, 4]);
    assert_eq!(res_corrupted, Err(SwarmError::CorruptedPiece));

    // Il pezzo deve essere tornato a Missing e i blocchi mancanti devono essere tornati a 2!
    assert_eq!(coord.piece_state(0), PieceState::Missing);
    assert_eq!(coord.remaining_blocks_count(), 2);

    // I blocchi possono ora essere rischedulati e riscaricati correttamente
    let r0 = coord.schedule_next_block(p1);
    assert_eq!(r0, Some((0, 0)));
    peer1.deliver_block(0, 0, vec![1, 2]).unwrap();

    let r1 = coord.schedule_next_block(p1);
    assert_eq!(r1, Some((0, 1)));
    let res_valid = peer1.deliver_block(0, 1, vec![3, 4]);
    assert_eq!(res_valid, Ok(()));

    assert_eq!(coord.piece_state(0), PieceState::Verified);
    assert_eq!(coord.remaining_blocks_count(), 0);
}

/// 8. Attesa passiva non attiva: `wait_for_piece` e `wait_completed` si bloccano
/// su Condvar e vengono risvegliati prontamente alla verifica.
#[test]
fn test_wait_for_piece_and_wait_completed_unblocks() {
    let coord = make_swarm_coordinator(2, 1, 0, |_p, _d| true);
    let (pid, peer) = coord.register_peer(&[0, 1]);

    let piece_done = Arc::new(AtomicBool::new(false));
    let torrent_done = Arc::new(AtomicBool::new(false));

    let c1 = coord.clone();
    let pd = piece_done.clone();
    let h1 = thread::spawn(move || {
        let res = c1.wait_for_piece(0);
        assert!(res.is_ok());
        pd.store(true, Ordering::SeqCst);
    });

    let c2 = coord.clone();
    let td = torrent_done.clone();
    let h2 = thread::spawn(move || {
        c2.wait_completed();
        td.store(true, Ordering::SeqCst);
    });

    // Assicuriamo che entrambi i thread siano addormentati
    thread::sleep(Duration::from_millis(50));
    assert!(!piece_done.load(Ordering::SeqCst));
    assert!(!torrent_done.load(Ordering::SeqCst));

    // Scarichiamo pezzo 0
    coord.schedule_next_block(pid);
    peer.deliver_block(0, 0, vec![10]).unwrap();

    // Il thread che attende il pezzo 0 deve sbloccarsi
    h1.join().unwrap();
    assert!(piece_done.load(Ordering::SeqCst));
    // Ma il torrent non è ancora completo
    assert!(!torrent_done.load(Ordering::SeqCst));

    // Scarichiamo pezzo 1
    coord.schedule_next_block(pid);
    peer.deliver_block(1, 0, vec![20]).unwrap();

    // Il thread che attende il completamento completo deve sbloccarsi
    h2.join().unwrap();
    assert!(torrent_done.load(Ordering::SeqCst));
}

/// 9. Gestione RAII: Drop di una PeerSession revoca i blocchi in volo
/// e decrementa l'availability dei pezzi e il conteggio dei peer.
#[test]
fn test_raii_drop_peer_session_decrements_availability() {
    let coord = make_swarm_coordinator(1, 2, 0, |_p, _d| true);

    let (p1, peer1) = coord.register_peer(&[0]);
    assert_eq!(coord.active_peer_count(), 1);

    // Schedula blocco a peer1
    let b = coord.schedule_next_block(p1);
    assert_eq!(b, Some((0, 0)));

    // Droppa peer1
    drop(peer1);
    assert_eq!(coord.active_peer_count(), 0);

    // Registra un nuovo peer: il blocco (0, 0) precedentemente assegnato a peer1
    // deve essere tornato disponibile perché revocato al drop!
    let (p2, _peer2) = coord.register_peer(&[0]);
    assert_eq!(coord.active_peer_count(), 1);

    let b_new = coord.schedule_next_block(p2);
    assert_eq!(b_new, Some((0, 0)));
}

/// 10. Stress test concorrente multi-peer con download simultaneo e cancellazione.
#[test]
fn test_stress_multi_peer_concurrent_swarm() {
    const NUM_PIECES: usize = 6;
    const BLOCKS_PER_PIECE: usize = 3;
    const NUM_PEERS: usize = 4;

    let coord = make_swarm_coordinator(
        NUM_PIECES,
        BLOCKS_PER_PIECE,
        4, // Endgame threshold
        |_p, data| data.len() == BLOCKS_PER_PIECE * 4,
    );

    let mut handles = Vec::new();

    for _ in 0..NUM_PEERS {
        let coord_clone = coord.clone();
        // Ogni peer possiede tutti i pezzi per massimizzare la contesa
        let all_pieces: Vec<PieceId> = (0..NUM_PIECES as PieceId).collect();
        let (pid, peer) = coord_clone.register_peer(&all_pieces);
        let peer = Arc::new(peer);

        let h = thread::spawn(move || {
            let mut delivered = 0;
            while coord_clone.remaining_blocks_count() > 0 {
                if let Some((piece, block)) = coord_clone.schedule_next_block(pid) {
                    thread::sleep(Duration::from_millis(1));
                    let dummy_payload = vec![1, 2, 3, 4];
                    let _ = peer.deliver_block(piece, block, dummy_payload);
                    delivered += 1;
                } else {
                    thread::sleep(Duration::from_millis(2));
                }
            }
            delivered
        });
        handles.push(h);
    }

    coord.wait_completed();

    for h in handles {
        let _ = h.join().unwrap();
    }

    assert_eq!(coord.remaining_blocks_count(), 0);
    for p in 0..NUM_PIECES as PieceId {
        assert_eq!(coord.piece_state(p), PieceState::Verified);
    }
}
