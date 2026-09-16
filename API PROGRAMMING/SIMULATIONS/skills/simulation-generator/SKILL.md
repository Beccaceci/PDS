---
name: simulation-generator
description: >-
  Generates new, high-complexity traits-driven Rust exam simulations based on Prof. Malnati's reverse-engineered examination patterns. Use this skill whenever the user asks to generate a new simulation, create an exam problem, or add to the simulation series.
---

# Simulation Generator Skill

This skill governs the autonomous design, scaffolding, verification, and traceability tracking of new Rust exam simulations for *Programmazione di Sistema* (Prof. G. Malnati, Politecnico di Torino).

---

## 🏛️ Core Principles & Non-Negotiable Invariants

Every simulation produced by this skill must satisfy:

1. **Reverse-Engineered Pattern Grounding**:
   - Adhere to the canonical **Three-Tier Architecture** (`SharedState` $\to$ `ServiceStruct` $\to$ `HandleStruct`).
   - Pure traits-driven contract with opaque factory function returning `impl Trait`.
   - Explicit lifecycle governance via RAII (`Drop`) or explicit transaction methods (`cancel`, `rollback`, `disconnect`, `join`).
   - Zero busy-waiting (`Condvar::wait_while` / `wait_timeout_while`), thread safety (`Clone + Send + Sync`), and releasing locks before invoking foreign user callbacks.
   - Read details in [Reverse-Engineered Patterns Reference](./references/patterns.md).

2. **Multi-Trait Requirement ($\ge 2$ Traits)**:
   - Must require implementing **at least 2 public traits** (e.g. Service Coordinator + Unit Handle).

3. **Higher Structural Complexity (Calibrated to Simulation 46 `TaskGraph`)**:
   - Must score $\ge 4.5 / 5.0$ on the [Complexity Rubric](./references/complexity_rubric.md).
   - Must require at least 3–4 cooperating internal structures with non-obvious internal relationships (e.g. reverse edges, generational fencing, dual condition variables, or indirection layers).

4. **Never-Seen Domain**:
   - Must consult the [Domain Catalog](./references/domain_catalog.md) to ensure the domain is completely virgin and does not duplicate any of the 47 prior simulations.

5. **Traceability Matrix Maintenance**:
   - Must update `SIMULATIONS/traceability_matrix.md` with the new simulation's ID, complexity, novelty, pattern alignment score, and structural breakdown.

6. **Authentic Exam Blank Slate in `src/lib.rs` (NO STRUCT AID)**:
   - The user must start completely from scratch, exactly as in the real exam with Prof. Malnati.
   - **NEVER provide internal struct definitions** (no `SharedState`, no `Manager`, no `Task`, no `Handle`, etc.).
   - Only provide the public traits, method signatures, and the opaque factory function with `todo!()` or `PhantomData`.

---

## 🛠️ Two-Phase Generation Workflow

When the user requests to generate a new simulation:

### 🌟 Phase 1: Problem Text Generation (`simulation_0XX.md`) — The Master Deliverable
This is the central phase. Formulate the comprehensive exam markdown document in `SIMULATIONS/SIMULATION<NN>/simulation_<NN>.md` following [Scaffolding Protocol](./references/scaffolding_protocol.md):
1. **Title & Subtitle**: Domain name, capstone tag, and italic introductory subtitle explaining the structural challenge.
2. **Motivating Narrative**: Systems programming real-world context.
3. **API Richiesta**: Code block defining the $\ge 2$ traits and factory function signature.
4. **Requisiti**: Concurrency, thread safety, zero busy-wait, cascade rules, grading caveats.
5. **Suggerimenti Implementativi**: Conceptual hints pointing out the relationship to discover (without revealing struct layouts).
6. **Meta-commentario**: Architectural depth, comparison with prior simulations (e.g. Simulation 46 `TaskGraph`), structural responsibilities census, and estimated difficulty budget.

---

### 📦 Phase 2: Scaffolding Generation (`Cargo.toml`, `src/lib.rs`, `tests/shared_tests.rs`)

Scaffold the directory `SIMULATIONS/SIMULATION<NN>/`:

#### 1. `Cargo.toml`
Create a clean Cargo package `simulation_0XX` with edition 2021 and no external async crates.

#### 2. `src/lib.rs` (Exam Blank Slate — Zero Struct Spoilers)
- Add module doc comments matching the problem description.
- Define the public traits (`pub trait Trait1`, `pub trait Trait2`) with method docstrings.
- Add the official **Student Workspace** separator:
  ```rust
  // =================================================================================
  // 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
  // =================================================================================
  // Inserisci in questa sezione le tue strutture private e le relative implementazioni.
  // =================================================================================
  ```
- Define the factory function returning `impl Trait` containing `todo!()` (or `std::marker::PhantomData` stub if needed).
- **DO NOT define any internal structs, fields, or helper architectures!**

#### 3. `tests/shared_tests.rs` (Maximum Coverage Suite)
Create an exhaustive test suite targeting the highest possible coverage:
- `test_send_sync_bounds`
- `test_basic_lifecycle`
- `test_concurrent_contention` (high thread count)
- `test_cancellation_or_rollback`
- `test_cascade_or_propagation`
- `test_zero_busy_wait_and_timeouts`
- `test_reentrancy_and_interleaving`
- `test_edge_cases_and_stale_handles`

#### 4. Update `traceability_matrix.md`
Append the new simulation entry to `SIMULATIONS/traceability_matrix.md` with full metric scores ($\ge 4.5$ complexity) and structural notes.

---

### Step 3: Present to the User
Present a concise summary to the user:
- State the chosen new domain.
- Present the required traits.
- Highlight the architectural challenge (why it has high complexity comparable to Simulation 46).
- Provide clickable `file://` links to `simulation_<NN>.md`, `src/lib.rs`, and `tests/shared_tests.rs`.
