use simulation_035::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

/// Mock di un nodo remoto per testare risposte lente, dinieghi e ritorni anticipati.
struct MockNode {
    term: AtomicU64,
    voted_in_term: AtomicU64,
    delay: Duration,
    vote_override: Option<VoteResult>,
    requests_received: Arc<AtomicUsize>,
}

impl MockNode {
    fn new(initial_term: u64, delay: Duration) -> Self {
        Self {
            term: AtomicU64::new(initial_term),
            voted_in_term: AtomicU64::new(0),
            delay,
            vote_override: None,
            requests_received: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_override(initial_term: u64, delay: Duration, outcome: VoteResult) -> Self {
        Self {
            term: AtomicU64::new(initial_term),
            voted_in_term: AtomicU64::new(0),
            delay,
            vote_override: Some(outcome),
            requests_received: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Node for MockNode {
    fn current_term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    fn is_leader(&self) -> bool {
        false
    }

    fn request_vote(&self, term: u64) -> VoteResult {
        self.requests_received.fetch_add(1, Ordering::SeqCst);
        if self.delay > Duration::ZERO {
            thread::sleep(self.delay);
        }

        if let Some(res) = self.vote_override {
            return res;
        }

        let curr = self.term.load(Ordering::SeqCst);
        if term > curr {
            self.term.store(term, Ordering::SeqCst);
            self.voted_in_term.store(term, Ordering::SeqCst);
            VoteResult::Granted
        } else if term == curr {
            let voted = self.voted_in_term.load(Ordering::SeqCst);
            if voted != term {
                self.voted_in_term.store(term, Ordering::SeqCst);
                VoteResult::Granted
            } else {
                VoteResult::Denied
            }
        } else {
            VoteResult::Denied
        }
    }

    fn receive_heartbeat(&self, term: u64) {
        let curr = self.term.load(Ordering::SeqCst);
        if term > curr {
            self.term.store(term, Ordering::SeqCst);
        }
    }

    fn start_election(&self, _others: &[&dyn Node]) -> bool {
        false
    }
}

#[test]
fn test_send_sync_bounds() {
    let node = make_node();
    assert_send(&node);
    assert_sync(&node);
}

#[test]
fn test_initial_state() {
    let node = make_node();
    assert_eq!(node.current_term(), 0);
    assert!(!node.is_leader());
}

#[test]
fn test_request_vote_transitions() {
    let node = make_node();

    // 1. Termine superiore (1 > 0) -> concede e aggiorna termine a 1
    assert_eq!(node.request_vote(1), VoteResult::Granted);
    assert_eq!(node.current_term(), 1);

    // 2. Stesso termine (1 == 1) dopo aver già votato -> nega
    assert_eq!(node.request_vote(1), VoteResult::Denied);
    assert_eq!(node.current_term(), 1);

    // 3. Termine inferiore (0 < 1) -> nega
    assert_eq!(node.request_vote(0), VoteResult::Denied);
    assert_eq!(node.current_term(), 1);

    // 4. Salto a termine superiore (5 > 1) -> concede e aggiorna a 5
    assert_eq!(node.request_vote(5), VoteResult::Granted);
    assert_eq!(node.current_term(), 5);

    // 5. Un'altra richiesta per il termine 5 -> ora nega
    assert_eq!(node.request_vote(5), VoteResult::Denied);
}

#[test]
fn test_receive_heartbeat_steps_down_leader() {
    let node = make_node();

    // Diventa leader per il termine 1
    assert!(node.start_election(&[]));
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 1);

    // Riceve heartbeat per lo stesso termine (1) -> rinuncia alla leadership
    node.receive_heartbeat(1);
    assert!(!node.is_leader());
    assert_eq!(node.current_term(), 1);

    // Ridiventa leader per il termine 2
    assert!(node.start_election(&[]));
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 2);

    // Riceve heartbeat per un termine superiore (5) -> aggiorna termine a 5 e rinuncia
    node.receive_heartbeat(5);
    assert!(!node.is_leader());
    assert_eq!(node.current_term(), 5);

    // Heartbeat per termine inferiore (3 < 5) -> ignorato
    node.receive_heartbeat(3);
    assert_eq!(node.current_term(), 5);
}

#[test]
fn test_single_node_election_wins_immediately() {
    let node = make_node();
    // In un cluster di 1 solo nodo, il voto proprio costituisce maggioranza (1/1 >= 1)
    let won = node.start_election(&[]);
    assert!(won);
    assert!(node.is_leader());
    assert_eq!(node.current_term(), 1);
}

#[test]
fn test_three_nodes_election_success() {
    let n1 = make_node();
    let n2 = make_node();
    let n3 = make_node();

    let others: Vec<&dyn Node> = vec![&n2, &n3];
    let won = n1.start_election(&others);

    assert!(won, "n1 must win election with 3/3 votes");
    assert!(n1.is_leader());
    assert_eq!(n1.current_term(), 1);
    assert_eq!(n2.current_term(), 1);
    assert_eq!(n3.current_term(), 1);
}

#[test]
fn test_election_fails_when_majority_denies() {
    let n1 = make_node();
    let n2 = MockNode::with_override(10, Duration::ZERO, VoteResult::Denied);
    let n3 = MockNode::with_override(10, Duration::ZERO, VoteResult::Denied);

    let others: Vec<&dyn Node> = vec![&n2, &n3];
    let won = n1.start_election(&others);

    assert!(!won, "n1 cannot win when both peers deny");
    assert!(!n1.is_leader());
    assert_eq!(n1.current_term(), 1);
}

#[test]
fn test_early_return_on_majority_victory_without_waiting_slow_peer() {
    let n1 = make_node();
    let n2 = MockNode::new(0, Duration::ZERO); // Concede subito
    let n3 = MockNode::new(0, Duration::ZERO); // Concede subito
    let n4 = MockNode::new(0, Duration::from_millis(200)); // Lento
    let n5 = MockNode::new(0, Duration::from_millis(200)); // Lento

    let others: Vec<&dyn Node> = vec![&n2, &n3, &n4, &n5];
    let start = Instant::now();

    // N=5: Maggioranza = 3. Con n1 (self), n2 e n3 i voti concessi sono 3 -> VITTORIA IMMEDIATA!
    let won = n1.start_election(&others);
    let elapsed = start.elapsed();

    assert!(won);
    assert!(n1.is_leader());
    assert!(
        elapsed < Duration::from_millis(100),
        "start_election must return early on majority victory without waiting 200ms for slow peers (elapsed: {:?})",
        elapsed
    );
}

#[test]
fn test_early_return_on_guaranteed_defeat_without_waiting_slow_peer() {
    let n1 = make_node();
    // N=5: Maggioranza necessaria = 3.
    // Con 3 rifiuti (n2, n3, n4), il massimo ottenibile è 1 (self) + 1 (n5) = 2 < 3 -> SCONFITTA GARANTITA!
    let n2 = MockNode::with_override(0, Duration::ZERO, VoteResult::Denied);
    let n3 = MockNode::with_override(0, Duration::ZERO, VoteResult::Denied);
    let n4 = MockNode::with_override(0, Duration::ZERO, VoteResult::Denied);
    let n5 = MockNode::new(0, Duration::from_millis(200)); // Lento

    let others: Vec<&dyn Node> = vec![&n2, &n3, &n4, &n5];
    let start = Instant::now();

    let won = n1.start_election(&others);
    let elapsed = start.elapsed();

    assert!(!won);
    assert!(!n1.is_leader());
    assert!(
        elapsed < Duration::from_millis(100),
        "start_election must return early on guaranteed defeat without waiting 200ms for slow peer (elapsed: {:?})",
        elapsed
    );
}

#[test]
fn test_concurrent_split_vote_at_most_one_leader() {
    // 5 nodi reali
    let nodes: Vec<Arc<dyn Node>> = (0..5).map(|_| Arc::new(make_node()) as Arc<dyn Node>).collect();
    let leaders_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..5 {
        let nodes_clone = nodes.clone();
        let lc = Arc::clone(&leaders_count);

        handles.push(thread::spawn(move || {
            let self_node = &nodes_clone[i];
            let others: Vec<&dyn Node> = nodes_clone
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != i)
                .map(|(_, n)| &**n)
                .collect();

            if self_node.start_election(&others) {
                lc.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let leaders = leaders_count.load(Ordering::SeqCst);
    assert!(
        leaders <= 1,
        "In concurrent split-vote elections, AT MOST ONE node can be elected leader (got {})",
        leaders
    );
}
