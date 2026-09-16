# Complexity Rubric & Benchmark Calibration

To satisfy the user's explicit requirement (**Higher Complexity**, calibrated against **Simulation 046 `TaskGraph`**), every newly generated simulation must be scored and vetted against this rubric before acceptance.

---

## 1. The Gold Standard: Deconstructing Simulation 046 (`TaskGraph`)

In `simulation_046.md`, the complexity stems from **cooperating internal structures and non-obvious relationships**, not from tricky arithmetic or arbitrary boilerplate.

### Why Simulation 046 Has High Structural Complexity:
1. **The Discovered Inverse Relationship**:
   - The public API only specifies `depends_on: &[TaskId]` (who I depend on).
   - To make the system work efficiently, the internal architecture MUST discover and maintain the inverse relationship: `dependents: Vec<TaskId>` (who depends on me?). Without this, notifying downstream tasks or cascading failures would require scanning the entire task table on every event ($O(N)$ vs $O(1)$).
2. **Four Distinct Cooperating Structures**:
   - `MyTaskManager`: Public wrapper holding `Arc<(Mutex<ManagerState>, Condvar, Condvar)>`.
   - `ManagerState`: Global state holding the sequential vector of tasks and the FIFO `ready_queue`.
   - `MyTask`: Per-task tracking node holding `remaining_deps`, `dependents`, `job: Mutex<Option<Job>>`, and `outcome: Option<bool>`.
   - `MyHandle`: Unit handle returned to callers holding task ID and manager reference for `join()` and `cancel()`.
3. **Dual Condition Variables**:
   - `cvar_workers`: Awakens worker threads when a task is pushed to `ready_queue`.
   - `cvar_join`: Awakens client threads blocked in `join()` when a task reaches terminal outcome.
4. **Unified Failure & Cancellation Cascade**:
   - Calling `cancel()` and encountering a runtime failure (`job() == false`) both route through `fail_cascade()`, propagating terminal failure down the transitive DAG and unlocking waiting joiners.
5. **Lock-Dropping Around Foreign Closures**:
   - The worker drops the state lock before running `job()`, then re-acquires it to trigger `success_cascade` or `fail_cascade`.

---

## 2. Structural Complexity Evaluation Dimensions (Total: 5.0)

A newly generated simulation must score **at least 4.5 / 5.0** on this rubric:

### Dimension 1: Relational & Structural Topology ($\ge 4.5 / 5.0$)
- **Score 1-2 (Trivial)**: A single shared queue or vector inside a Mutex (e.g. `BoundedQueue`, `SharedCounter`).
- **Score 3 (Moderate)**: Two structures with straightforward relationship (e.g. `EventBus` holding `Vec<Sender>`).
- **Score 4-5 (High / Simulation 46 Tier)**: 3 to 5 distinct internal structures cooperating across threads. Requires maintaining non-trivial relationships:
  * Bi-directional graphs or reverse-dependency maps.
  * Hierarchical parent-child token relationships (e.g., overdraft borrowing or tree scopes).
  * Segmented epoch rings with multi-tier indices.

### Dimension 2: Lifecycle & State Invariant Richness ($\ge 4.5 / 5.0$)
- **Score 1-2**: Binary state (Open/Closed or Empty/Full).
- **Score 3**: Linear lifecycle (Pending $\to$ Running $\to$ Done).
- **Score 4-5**: Multi-path state transitions with re-entrancy, cancellation, or cascade:
  * Cancellation before run vs during run vs after completion.
  * Rollback of speculative operations.
  * Re-entrant self/cross modification without deadlocks.

### Dimension 3: Synchronization & Signaling Sophistication ($\ge 4.5 / 5.0$)
- **Score 1-2**: Single `Mutex` + single `Condvar`.
- **Score 3**: `Mutex` + `Condvar` with dynamic timeout.
- **Score 4-5**: Heterogeneous synchronization disciplines:
  * Multiple specialized `Condvar`s separating distinct classes of waiting threads.
  * Strict lock-acquisition ordering across multiple entities to guarantee deadlock freedom.
  * Generation counters / epoch fences guarding against ABA or stale iteration races.

### Dimension 4: Discovered Architectural Insight ($\ge 4.5 / 5.0$)
- The problem description and traits must NOT spoon-feed the internal topology.
- The student must realize on their own what helper index, queue, or indirection layer is required to satisfy the operational constraints without quadratic overhead or active spinning.
