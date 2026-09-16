# Reverse-Engineered Exam Patterns & Malnati Architectural Standard

This reference synthesizes the core traits-driven architectural guidelines reverse-engineered from Prof. Malnati's exam sittings and reference implementations (*Programmazione di Sistema*, Politecnico di Torino).

---

## 1. Core Meta-Pattern: Traits-Driven Systems Design

Modern exams at Politecnico di Torino do not ask for concrete structs. Instead, the contract is strictly formulated around **public traits** and an opaque factory function:

```rust
pub trait HandleTrait {
    // Unit lifecycle methods: cancel, join, forget, disconnect, poll...
}

pub trait ServiceTrait: Clone + Send + Sync {
    // Service-level operations: submit, register, acquire, publish...
}

pub fn make_service(...) -> impl ServiceTrait {
    // Returns private concrete struct implementing ServiceTrait
}
```

### Key Pedagogical Constraints
1. **No Busy-Waiting**: Any blocking requirement ("senza consumare cicli di CPU") maps directly to `Condvar::wait_while` or `wait_timeout_while` (or wrapped channels).
2. **RPITIT (Return-Position `impl Trait` in Trait Definitions)**: Used to return opaque handle types (`fn submit(...) -> (Id, impl HandleTrait)`) without heap allocating trait objects (`Box<dyn Trait>`).
3. **`Send + Sync` Boundaries**: Types shared across threads must enforce `Send + Sync`. If closures or callbacks are accepted (`FnOnce() -> bool + Send + 'static`), the internal containers must handle them without violating thread-safety.

---

## 2. The Canonical Three-Tier Architecture

Prof. Malnati's expected internal decomposition consists of three decoupled layers:

```
┌────────────────────────────────────────────────────────────────────────┐
│ SharedState (or ManagerState)                                          │
│ - Pure data holder: collections, queues, state graphs.                 │
│ - Held behind Arc<Mutex<SharedState>> or Arc<(Mutex<S>, Condvar, ...)> │
│ - No public traits implemented on this struct.                         │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ ServiceCoordinator (e.g. MyTaskManager, MySignal, MyPool)             │
│ - Implements the aggregate public trait (TaskGraph, Signal, Pool).     │
│ - Implements Clone + Send + Sync.                                      │
│ - Holds Arc to the SharedState and Condvars.                          │
│ - Constructed and returned by the factory function.                    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ UnitHandle (e.g. MyHandle, MySubscription, MyConnection)               │
│ - Implements the unit/lifecycle trait (TaskHandle, Connection).        │
│ - Holds item identity/ID, Option<T> (for .take() on Drop),             │
│   and an Arc reference back to the coordinator or shared state.        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Mandatory Concurrency Disciplines

### A. Lock Dropping Before Execution
When invoking foreign code (user closures, callbacks, notification loops, or worker jobs):
- **ALWAYS release/drop the Mutex guard before calling the function!**
```rust
let job = task.job.lock().unwrap().take().unwrap();
drop(manager_guard); // Free lock so other threads/methods aren't starved!
let outcome = job();
```
Holding locks across foreign invocations causes deadlocks, serialization bottlenecks, and prevents re-entrancy (e.g., a callback calling `disconnect()` or `submit()` on the same coordinator).

### B. Single Convergence Path for Lifecycle Events
If a resource or task can be terminated via multiple triggers (e.g. explicit `.cancel()`, timeout, or natural failure propagation):
- **Never maintain duplicate teardown logic.**
- Both paths must converge on the exact same helper method (`fail_cascade()`, `retire_entry()`, etc.) ensuring consistent state transitions.

### C. Targeted Condvar Signaling
When coordinating multiple waiting groups with different wake-up conditions (e.g., workers waiting for ready tasks vs. clients waiting for a specific task to complete):
- Use **distinct `Condvar` instances** (e.g. `(Mutex<State>, Condvar /* workers */, Condvar /* completion */)`).
- This prevents severe thundering-herd issues and accidental spurious wakeups across heterogeneous waiting sets.

### D. Safe Resource Extraction in `Drop`
To return or transfer ownership of a field from inside `Drop::drop(&mut self)`:
```rust
if let Some(resource) = self.item.take() {
    let mut guard = self.shared.lock().unwrap();
    guard.pool.push(resource);
    self.cvar.notify_one();
}
```
Always use `Option<T>` and `.take()` to circumvent Rust's prohibition against moving out of types implementing `Drop`.
