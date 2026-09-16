# Scaffolding Protocol & Generation Workflow

This document defines the step-by-step technical standard for scaffolding a new simulation package within the `SIMULATIONS` directory.

---

## 1. Directory Structure Standards

Each new simulation must be placed in a directory formatted as `SIMULATION<NN>` (where `NN` is a zero-padded 2-digit integer for numbers under 100, e.g. `SIMULATION48`):

```text
SIMULATIONS/SIMULATION48/
├── Cargo.toml
├── simulation_048.md
├── src/
│   └── lib.rs
└── tests/
    └── shared_tests.rs
```

---

## 2. File Specifications

### 1. `Cargo.toml`
Must define a clean library package without third-party async dependencies (adhering strictly to `std::sync` / `std::thread`):
```toml
[package]
name = "simulation_048"
version = "0.1.0"
edition = "2021"

[dependencies]
```

### 2. `simulation_0XX.md` (Phase 1 — The Master Exam Document)
This is the **most important deliverable**. It must strictly adhere to the established exam document schema:
- **Title**: `# Simulazione 0XX — <DomainName> (capstone)`
- **Subtitle / Italic Preface**: `*Due tratti pubblici, come richiesto — ...*` setting the architectural challenge and comparison with prior simulations.
- **Narrative Section**: Real-world systems motivating context (database storage, network multiplexing, distributed coordination, etc.).
- **`### API richiesta`**:
  - Exact Rust code block defining the traits and factory function signature.
  - Requires **at least 2 traits**: e.g., a Service Coordinator Trait and a Unit Handle/Session Trait.
  - Factory function returning `impl ServiceTrait`.
- **`### Requisiti`**:
  - Thread safety (`Clone + Send + Sync`).
  - Zero busy-waiting ("nessuna attesa attiva").
  - Clean error propagation or cancellation semantics.
  - Tests in `tests/shared_tests.rs` must pass without modification (`cargo test`).
  - Non-compiling code receives 0 points.
- **`### Suggerimenti implementativi`**:
  - Conceptual guidance highlighting the key challenge (e.g. "each task needs to know who depends on it", "snapshots need a shared flag with the live list"), without spelling out the exact structs.
- **`## Meta-commentario`**:
  - In-depth architectural analysis explaining why this problem is non-trivial.
  - Comparison with previous simulations (e.g., how it differs from `TaskGraph` 046 or `Signal` 047).
  - Structural responsibilities breakdown (how many distinct structural roles are needed, without prescribing their exact names or layouts).
  - Estimated difficulty and time budget (e.g., 120–150 minutes).

---

### 3. `src/lib.rs` (Phase 2 — Authentic Blank-Slate Exam Starter)
> [!IMPORTANT]
> **NO AID — ZERO STRUCT SPOILERS**:
> In the real exam, Prof. Malnati provides ONLY the traits and the factory signature. The student must infer and design every single structure from scratch.
> 
> - **DO NOT provide any concrete struct definitions** (no `SharedState`, `Manager`, `Handle`, `Task`, `Inner`, etc.).
> - **DO NOT add struct fields or state container declarations**.
> - **DO NOT write architectural hints or structural diagrams in `src/lib.rs`**.
> - You MAY use `std::marker::PhantomData` or `todo!()` inside the factory function stub so the file represents a valid starting template.

#### Canonical Template for `src/lib.rs`:
```rust
//! # Simulazione 0XX — <DomainName>
//!
//! <Doc comments reproducing the problem narrative and requirements>

use std::sync::Arc;
use std::time::Duration;

pub type ItemId = u64;

/// Unit handle trait
pub trait MyHandle {
    fn cancel(&self) -> bool;
    fn join(self) -> bool;
}

/// Service coordinator trait
pub trait MyService: Clone + Send + Sync {
    fn submit(
        &self,
        deps: &[ItemId],
        action: impl FnOnce() -> bool + Send + 'static,
    ) -> (ItemId, impl MyHandle + 'static);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub fn make_service() -> impl MyService {
    todo!("Implementa le strutture e il costruttore")
}
```

---

### 4. `tests/shared_tests.rs` (Phase 2 — Maximum Coverage Suite)
Must provide an exhaustive, high-coverage test harness testing against the public traits and factory function:
1. `test_send_sync_bounds`: Verifying `Send` and `Sync` static trait bounds.
2. `test_basic_lifecycle`: Basic single-threaded happy path.
3. `test_concurrent_contention`: Heavy multi-threaded stress test with multiple competing worker and producer threads.
4. `test_cancellation_or_rollback`: Cancellation, RAII Drop cleanup, or explicit rollback.
5. `test_transitive_cascade_or_propagation`: Testing deep transitive cascades across the graph/structure.
6. `test_zero_busy_wait_and_timeouts`: Verifying threads block without CPU spin and unblock promptly on signal.
7. `test_reentrancy_and_interleaving`: Self-disconnection, concurrent mutation during callback execution, or re-entrant invocations without deadlock.
8. `test_edge_cases_and_stale_handles`: Post-completion operations, multiple cancellations, invalid tokens, zero-capacity boundaries.

---

### 5. `traceability_matrix.md` Update
Immediately upon generating the new simulation:
- Append a new row to `SIMULATIONS/traceability_matrix.md` with:
  - ID (e.g. `48`)
  - Name
  - Domain & Nature
  - Structural Complexity Score (0.0 – 5.0, must be $\ge 4.5$)
  - Novelty / Unique Tricks Score (0.0 – 5.0, must be $\ge 4.5$)
  - 2026 Exam Pattern Alignment (0.0 – 5.0)
  - Traits Count ($\ge 2$)
  - Key Architectural Traits & Structures
