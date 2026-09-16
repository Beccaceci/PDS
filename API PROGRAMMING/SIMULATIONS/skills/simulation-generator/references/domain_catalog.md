# Domain Catalog: Explored & Virgin Systems Domains

To satisfy the user's explicit requirement (**Look for a never seen domain**), the agent MUST consult this catalogue to ensure the candidate domain has **never appeared** in any of the 47 prior simulations.

---

## 1. Censused Domains (Simulations 01 — 47) — DO NOT REUSE!

The following 47 domains are strictly off-limits for new simulations:

1. **EventBus**: Pub-Sub channel with Drop-based unsubscribe.
2. **WorkProducer/Consumer**: MPMC worker queue with cancel token.
3. **Exchanger**: Pairwise thread rendezvous exchange.
4. **TaskExecutor**: Priority task queue with deadlines.
5. **BoundedQueue**: Bounded circular buffer backpressure.
6. **TransactionalQueue**: Staged two-phase queue with rollback.
7. **RwCell**: Reader-writer lock cell with custom guards.
8. **DelayQueue**: Dynamic timed queue with earliest-deadline wait.
9. **WaitGroup**: Go-style synchronization barrier with delta counters.
10. **AsyncResourcePool**: Tokio async resource loan pool.
11. **MultiResourceManager**: Multi-key resource acquisition with ordered locking.
12. **MetricsBoard**: Real-time metrics snapshot with dynamic label registration.
13. **AgingScheduler**: Priority aging scheduler preventing starvation.
14. **VersionedCell**: Optimistic Concurrency Control (OCC) cell with CAS.
15. **SingleFlightCache**: Request coalescing / thundering herd suppression.
16. **BatchPool**: Dynamic batching worker pool with capacity/time triggers.
17. **TurnManager**: Sequential round-robin turn coordination.
18. **TicketLock**: Fair FIFO ticket-and-turn spin/block lock.
19. **Broadcast**: Multi-consumer broadcast channel iterator.
20. **TickerService**: Periodic interval tick callback dispatcher.
21. **TtlCache**: Single-flight LRU cache with time-to-live per key.
22. **JobScheduler**: Multi-resource dependency pipeline with worker pool.
23. **CircuitBreaker**: Failure breaker with half-open single-winner trial.
24. **LeaseLockManager**: Distributed lease lock with reaper thread & generation.
25. **TransferPipeline**: Staged pipeline with atomic rollback and retries.
26. **TwoPhaseCommitCoordinator**: 2PC with `Box<dyn Participant>` Send-only bridging.
27. **SagaOrchestrator**: Saga pattern with reverse step compensation & timeout.
28. **QuotaManager**: Multi-tenant hierarchical quota & token bucket.
29. **AppendLog**: Epoch-synchronized write-ahead log with non-blocking checkpoints.
30. **LoadBalancer**: Health-checked backend routing with dynamic cooldown.
31. **CreditedChannel**: HTTP/2 stream credit-based flow control channel.
32. **Phaser**: Multi-phase cyclic barrier with dynamic participants.
33. **WorkStealingPool**: Decentralized per-worker deque work stealing.
34. **MvccStore**: Multi-version concurrency control with snapshot GC on Drop.
35. **LeaderElection**: Raft-style distributed leader election with quorums.
36. **Reactor**: I/O event demultiplexer with disjunctive event wait & urgent drain.
37. **PriorityLock**: Mutex with priority inheritance and dynamic recalculation.
38. **HierarchicalPool**: Thread-local cache with shared overflow & Drop routing.
39. **PoisonableBarrier**: Barrier with poison cascade and generation restart.
40. **VirtualMemory**: OS page table, page-fault handling & frame allocation.
41. **RpcClient**: Multiplexed asynchronous RPC client with correlation IDs.
42. **TransactionalPair**: Two-variable cross-lock atomic commit coordinator.
43. **CancelScope**: Hierarchical structured concurrency tree cancellation.
44. **InternPool**: Memory-deduplicating string interner with weak-ref GC.
45. **ShardedCache**: Multi-shard concurrent hash table with "stop-the-world" rehash.
46. **TaskGraph**: Directed Acyclic Graph (DAG) task runner with cascade failure/cancel.
47. **Signal**: Re-entrant signal-slot subscription with snapshot emit & self-disconnect.

---

## 2. Virgin Domains: High-Complexity Candidates for Simulation 48+

When generating new simulations, draw from these unvisited, authentic systems-programming domains:

### Candidate A: **P2P Gossip Epidemic Buffer**
- **Domain Story**: Epidemic broadcast protocol where nodes exchange versioned peer state via gossip rounds.
- **Traits Required**: `GossipPeer`, `EpidemicBuffer`.
- **Architectural Discovery**: Tracking vector-clock version per peer, selective anti-entropy diffing, and tombstone reconciliation when peers drop off.

### Candidate B: **Segmented Write-Ahead Log (WAL) Compactor with Segment Leases**
- **Domain Story**: High-performance storage engine where active writers append to an active segment while concurrent reader iterators hold segment read leases, and a background compactor merges inactive segments.
- **Traits Required**: `WalWriter`, `SegmentReader` (or `WalStore` + `SegmentLease`).
- **Architectural Discovery**: Managing dynamic segment lifecycle (Active $\to$ Sealed $\to$ Compacting $\to$ Reclaimed), preventing reclamation of sealed segments while any `SegmentLease` remains active, and atomic segment swap during compaction.

### Candidate C: **Hierarchical Credit Overdraft Manager**
- **Domain Story**: Cloud resource scheduler where child tenants have baseline credit limits but can borrow from parent or sibling pools subject to immediate recall or penalty when high-priority tenants request burst capacity.
- **Traits Required**: `CreditAccount`, `QuotaRegistry` (or `CreditLease` + `CreditTree`).
- **Architectural Discovery**: Bi-directional tree of accounts, cascading overdraft recalls down the tree, and non-blocking credit return via Drop.

### Candidate D: **Distributed Monotonic Fencing Token Service**
- **Domain Story**: Storage guard ensuring that stale (zombie) workers whose network partitions resolved cannot overwrite newer writes. Each client acquires a lease bound to a strictly monotonic fencing token; storage validates the token on every operation and rejects retrograde tokens.
- **Traits Required**: `FencingCoordinator`, `FencedLease` (or `StorageGuard`).
- **Architectural Discovery**: Maintaining highest observed token per storage partition, invalidating stale leases in flight, and handling concurrent renewal attempts.

### Candidate E: **Dynamic Flow-Graph Stream Demuxer**
- **Domain Story**: Reactive stream processing where an incoming typed stream is demultiplexed into dynamic subscriber filter trees with backpressure propagation back to the source.
- **Traits Required**: `StreamPublisher`, `SubscriptionFilter`.
- **Architectural Discovery**: Dynamic filter predicate evaluation, per-subscriber capacity monitoring, and throttling the publisher when the slowest non-dropped subscriber hits high water mark.
