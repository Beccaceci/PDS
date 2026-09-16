# Exam Reverse-Engineering — Traits-Driven Design Problems
### Prof. G. Malnati — Programmazione di Sistema (Politecnico di Torino)

*Agent 2 deliverable. Sample size: 2 exam problems. Treat every pattern below as a hypothesis, not a certainty — the confidence level is stated explicitly wherever it matters, and a third exam sample would sharpen almost every section here.*

**Sources analyzed**
1. **Exam A — `ResourcePool`**, Quiz "API Programming - Rust", 3 July 2026. Full header available: 6.0 points total, this attempt scored 2.0 (33%), 1h05m elapsed. Includes the professor's own graded feedback and a "solution vaguely based on your code."
2. **Exam B — `forgettable_channel`**, filename dated 15 June 2026. Only the question text is available (no header, no grading, no score) — so everything about *difficulty calibration* for this one is inferred, not measured.

---

## 1. What actually changed this year

Your own framework document already names the shift correctly: previous years tested *implementation* skills directly — write the algorithm, manage the ownership, get the enum right. This year's exams test something one level up: **can you infer a correct concurrent architecture from a contract you didn't write?**

Both samples confirm this precisely. Neither problem asks "implement a thread pool" or "implement a channel." Both say, in effect: *here are the trait signatures your code must satisfy — including a free function whose return type is `impl Trait` — go design whatever private types you need.* The traits themselves are short (2–4 methods). All the difficulty is hidden in the words around them: "senza consumare cicli di CPU," "concesso ad al più un thread alla volta," "scartati silenziosamente." Reading comprehension of the prose is now as heavily examined as Rust syntax.

This is a deliberate pedagogical move for a *systems programming* course: the professor isn't testing "do you know `Mutex`," he's testing "given a classical OS/systems concept (a connection pool, a cancellable message queue), can you pick the right synchronization primitive and wrap it behind a clean, opaque API?" That's the actual skill being examined.

## 2. The meta-pattern: one shape, two costumes

Strip away the domain story and both exams reduce to the *same template*:

> Take a standard-library concurrency primitive. Layer an extra piece of **lifecycle semantics** on top of it (return-to-pool-on-drop; cancel-before-delivery). Expose the result through **traits only** — never a concrete struct — via a **free factory function returning `impl Trait`**. There is always a **handle object** whose lifetime or an explicit method on it (`Drop`, or `forget()`) governs the extra semantics.

| | Exam A — `ResourcePool` | Exam B — `forgettable_channel` |
|---|---|---|
| Domain story | connection pool | cancellable mpsc message |
| "Unit" trait (single item) | `Resource<T: Send>` | `Forgettable` |
| "Service" trait (the aggregate) | `ResourcePool<T: Send>` | `ForgettableSender<T>` + `ForgettableReceiver<T>` |
| Factory function | `make_resource_pool<T: Send>(items: Vec<T>) -> impl ResourcePool<T>` | `forgettable_channel<T>() -> (impl ForgettableSender<T>, impl ForgettableReceiver<T>)` (signature implied, not shown verbatim) |
| Lifecycle mechanism | RAII — `Drop` returns the item automatically | explicit — the caller must call `forget()` |
| Blocking requirement | `acquire()` blocks, no busy-wait | `recv()` blocks, implicitly no busy-wait (inherited from the wrapped channel) |
| Bounded/variant method | `acquire_timeout(Duration) -> Option<impl Resource<T>>` | none shown in this sample |
| Multi-producer requirement | not applicable (symmetric access) | `ForgettableSender<T>: Clone` — explicit MPSC topology |
| "What happens to a stale/invalid entry" | n/a | "cancelled messages are discarded silently" — a lazy-cleanup clause |
| Real-world motivating paragraph | present, explicit (DB pools) | absent in the captured text |
| Implementation scaffold given | none | 3 explicit suggested steps |

The two right-hand differences (motivating paragraph, scaffold) are the most likely candidates for **exam-to-exam variance** rather than signal — don't over-read them as "this year's exams always give scaffolding." With n=2 we can't tell if that's a stylistic constant or just this particular question being more guided.

## 3. The professor's canonical internal architecture (inferred)

Exam A is the goldmine here, because we have the professor's own reference solution, not just the question. It reveals a **three-tier skeleton** that is almost certainly the template he expects for *every* problem in this family, including exam B and anything not yet seen:

```
SharedState<T>      — the raw, protected data (e.g. a Vec<T> of available items,
                       or a queue of pending messages). No trait implemented here;
                       it's a pure data holder, always behind Mutex (+ Condvar if
                       something needs to block on it).

ServiceStruct<T>     — implements the "aggregate" trait (ResourcePool<T>,
                       ForgettableSender/Receiver<T>). Holds an Arc<...SharedState...>
                       so it can be cloned/shared across threads. This is what the
                       factory function actually constructs and returns as `impl Trait`.

HandleStruct<T>      — implements the "unit" trait (Resource<T>, Forgettable). Holds
                       Option<T> (so ownership can be moved out later) plus a clone
                       of the same Arc. If the semantics are RAII (exam A), it also
                       implements Drop and uses `.take()` to extract T without
                       violating Rust's "can't move out of a type with Drop" rule.
                       If the semantics are explicit-cancel (exam B), it holds an id
                       or reference instead, and forget() does a locked lookup.
```

Concretely, exam A's reference solution is:

```rust
struct SharedState<T: Send> { resources: Vec<T> }

struct Manager<T: Send> {
    state: Arc<(Mutex<SharedState<T>>, Condvar)>,
    capacity: usize,
}

struct Element<T: Send> {
    element: Option<T>,
    state: Arc<(Mutex<SharedState<T>>, Condvar)>,
}
```

Two idioms here are worth internalizing because they will very likely recur:

- **`Arc<(Mutex<S>, Condvar)>`** — the professor bundles the mutex and its condvar into a single tuple inside one `Arc`, rather than two separately-shared fields. This is a specific, recognizable style choice, not the only correct one, but matching it reduces friction when your architecture is compared against his mental template.
- **`Option<T>` + `.take()` inside `Drop`** — the standard trick for extracting an owned value out of a struct that implements `Drop` (you cannot move out of `&mut self` directly in `drop()`, but you *can* `Option::take()` it). Any exercise combining "owns a `T`" with "must give it back on scope-exit" will need this same trick.

## 4. Recurring API-design idioms

These are phrases/signatures you should learn to treat as *direct instructions*, because in both exams the prose maps one-to-one onto a specific Rust mechanism:

| Phrase in the prompt | What it's telling you to use |
|---|---|
| "blocca... senza consumare cicli di CPU" / any "no busy-waiting" clause | `Condvar::wait_while` / `wait_timeout_while` (never a `loop { if ... } `spin) |
| "`-> impl Trait`" on a free function | You must design a private, unnamed struct — the public API is the trait, not a struct |
| "`: Clone`" bound on a producer-side trait | Multi-producer topology; the sender wraps `Arc` internally so cloning is cheap and shares state |
| "restituisce `Option<...>`" for a "did this succeed" method | The operation can legitimately fail without it being an error — model it as `Option`, not `Result` |
| "timeout... rinuncia e restituisce `None`" | Pairs with `Condvar::wait_timeout_while`, whose `WaitTimeoutResult::timed_out()` becomes your `None` branch |
| "scartati silenziosamente" / "silently discarded" | A lazy-cleanup pattern: don't eagerly remove cancelled/stale entries elsewhere — just skip them at the point of consumption |
| "esce dallo scope... torna nel pool" | `Drop`-based RAII, not an explicit `release()` method |

One deeper language point worth flagging for Agent 3 (Rust Concepts) to cover explicitly: `acquire(&self) -> impl Resource<T>` is **return-position `impl Trait` in a trait definition** (RPITIT), a relatively recent stabilization in Rust. Its necessity here isn't arbitrary — `ResourcePool<T>` couldn't be used as a `dyn Trait` if `acquire` returned a generic-in-`Self` associated type any other way, and the professor clearly wants students to hide the concrete handle type without resorting to `Box<dyn Resource<T>>` (which would add an allocation + dynamic dispatch the exercise doesn't ask for). If you haven't seen RPITIT explicitly in lecture, that's a gap worth closing before the next exam — this mechanism appears in *both* samples' factory-function signatures.

## 5. What the grading actually rewards and punishes (hard evidence)

This is the most valuable section, because for once we're not inferring philosophy from a prompt — we're reading the professor's actual red pen. On the submitted attempt for exam A (6/12 tests passing, 2.0/6.0 points, i.e. roughly proportional to tests passed but not purely so), the stated reasons for the mark were:

- `get()` was left unfinished (`.unwrap()` on an `Option` with a dead commented-out line above it, admitted in-code as "non sono riuscito a finirlo per mancanza di tempo").
- `Drop` for the handle type was **entirely missing** — this is almost certainly what caused the one test that hung indefinitely rather than failing cleanly: a consumer thread waiting forever on a `Condvar` that nothing will ever notify, because no resource is ever returned to the pool.
- The internal data structure chosen for the handle type was judged fundamentally wrong for solving the exercise (not just suboptimal — actually unable to reach a correct solution).
- The `Manager` struct's method structure was judged as needing a full rewrite.

Three conclusions follow, all corroborated by the explicit exam rules ("se il codice non compila, non verrà valutato"):

1. **Compiling beats being incomplete.** A method that compiles with a simplified/wrong body still earns partial credit through the tests that don't depend on it; a non-compiling submission earns zero regardless of how much is conceptually correct. Never leave a method un-compilable when time runs short — return a plausible-but-wrong value rather than nothing.
2. **A hang is worse than a failure.** Failing tests are graded proportionally; a deadlocked test is a red flag that gets called out by name in feedback ("1 resta bloccato in modo indefinito"). Before submitting, trace every `wait`/`wait_while`/`wait_timeout_while` call and confirm there is a `notify_one`/`notify_all` on **every** code path that changes the awaited condition — including `Drop`.
3. **Architecture is graded, not just behavior.** The comment about the `Manager`'s "method structure" needing a full rewrite implies the professor is reading the *shape* of the solution (does it match something like the three-tier skeleton in §3?), not just running the test suite and stopping there.

## 6. Expected reasoning process for an unseen problem in this family

Derived specifically from what these two exams have in common — not generic Rust advice:

1. **Read the "unit" trait first.** It almost always models a single item's lifecycle (a borrowed resource, a sent message) and tells you what the handle object must hold.
2. **Read the "service" trait second.** It tells you the shape of the shared, thread-safe backing store you need.
3. **Read the factory function signature last, but design from it.** `-> impl Trait` (or a tuple of them) means: *invent private struct(s); nothing about their names or fields is visible to the caller.*
4. **Underline every constraint adjective in the prose** and translate immediately: "senza consumare cicli di CPU" → Condvar; "esclusivo"/"al più un thread alla volta" → move `T` in and out of an `Option`, never share `&mut T`; "condivisibile tra più thread" → the service struct needs `Arc` internally (methods take `&self`, so mutability must come from `Mutex`, not from `&mut self`).
5. **Commit to the three-tier skeleton** (`SharedState` / `ServiceStruct` / `HandleStruct`) before writing any method bodies. Decide up front which struct owns the `Mutex`+`Condvar` (always `SharedState`, wrapped once in `Arc`, cloned into both the service and every handle).
6. **Implement the "happy path" method fully before its variant.** `acquire_timeout` is structurally `acquire` with `wait_timeout_while` substituted for `wait_while` and a `timed_out()` check added — write it by adapting the working method, not from scratch.
7. **Never let a method fail to compile.** If you're out of time on a method's real logic, return a minimally-plausible stub rather than leaving a syntax gap — compiling code with wrong-but-present logic still earns partial credit; non-compiling code earns none.
8. **Before submitting, trace blocking primitives for guaranteed wake-ups.** For every place you call something that can block indefinitely, find the exact statement elsewhere (including inside `Drop`) that guarantees it eventually wakes. A deadlocked test is explicitly worse than a failed one.

## 7. Concept frequency (n=2 — read as "seen at least once," not as statistics)

| Concept | Exam A | Exam B |
|---|---|---|
| `impl Trait` as an opaque return type | ✓ | ✓ |
| Generic parameter bounded by `Send` | ✓ (`T: Send`) | plausible, implicit |
| `Clone` bound on a producer-side trait | — | ✓ |
| `Arc` for cross-thread shared ownership | ✓ | plausible |
| `Mutex` guarding shared mutable state | ✓ | plausible |
| `Condvar` for wait-without-spin | ✓ (explicit in solution) | plausible (or inherited from a wrapped `mpsc` channel) |
| RAII via `Drop` | ✓ (explicit requirement) | not present — this sample uses an **explicit** `forget()` call instead |
| `Option<T>` "take-on-drop" trick | ✓ | n/a |
| Bounded/timeout variant (`Duration` → `Option`) | ✓ | not shown in this sample |
| MPSC topology | — (symmetric access) | ✓ (explicit) |
| Lazy discard of stale/cancelled entries | — | ✓ |
| Suggested implementation scaffold in the prompt | — | ✓ |

The RAII-vs-explicit-cancel split between the two samples is the single most important open question below — it determines whether "the professor always uses `Drop`" is a rule or just something that happened to fit exam A's story.

## 8. Difficulty & time calibration

Only exam A carries a measured data point: **6.0 points, ~65 minutes elapsed, partial credit tracked closely with tests-passed (6/12 tests ≈ half, but the awarded score was 2.0/6.0 ≈ a third — pulled down by the deadlock and the architecture-quality penalty described in §5).** Treat "roughly one hour per trait-driven problem of this size" as a first estimate for pacing, not a guarantee — a single exam's timer doesn't tell us whether this was the only question on that sitting or one of several.

Exam B has no attached score or time, so nothing here is measured for it; its extra scaffolding (3 explicit implementation steps, versus none in A) is the only available hint that it may be *intended* as slightly easier or earlier in a sequence — but that's speculation, flagged accordingly.

## 8a. Confirmed scope constraint

Nicola has confirmed directly: **Tokio/async is not in scope for the programming-exercise portion of the exam.** This rules out an entire branch of prediction that seemed plausible purely from course coverage (§1) — a reminder that "was taught in lecture" is necessary but not sufficient evidence for "could appear on this specific exam." All predictions below stay within `std::sync`/`std::thread`.

## 9. Predictions for future exams (explicitly speculative)

Extrapolating the meta-pattern in §2 to other "wrap a stdlib primitive with extra lifecycle semantics" scenarios that would fit the same mold and the same systems-programming spirit:

- A **rate limiter / semaphore-style permit system** (`acquire_permit() -> impl Permit`, permit returns capacity on `Drop`) — structurally almost identical to exam A with a counter instead of a `Vec`.
- A **debounced or coalescing channel** (multiple `send()`s collapse into one `recv()`) — same MPSC shape as exam B with different collapsing logic.
- A **broadcast / pub-sub channel with unsubscribe** — a "unit" trait for the subscription handle (`unsubscribe()` or `Drop`), a "service" trait for the publisher.
- A **read-write lock wrapper** exposing custom guard types instead of `std::sync::RwLockReadGuard`/`WriteGuard` directly.
- A **one-shot / promise-style channel** (`send` consumes `self`, single value, single receiver) — a plausible "simpler" companion to exam B's MPSC version.

None of these should be treated as confirmed exam content — they are pattern-consistent guesses for Agent 6 (Exam Generator) to draw from once more real material is available, explicitly labeled as synthetic when used.

## 10. Open questions — what a third exam sample would resolve

- **Is RAII (`Drop`) the default lifecycle mechanism, or did exam A just happen to fit it?** Exam B uses an explicit `forget()` call instead. A third sample would tell us whether the professor alternates deliberately (testing both mechanisms across the course) or whether one is simply more natural to each domain story.
- **Is a bounded/timeout variant a fixed second-half requirement, or specific to pool-style exercises?** Exam B shows no such variant in the captured text.
- **Is the "Suggerimenti implementativi" scaffold a stable feature of this exam's difficulty tier, or does it vary per question within the same sitting?** We don't know if exam A's paper had its own scaffold section that just wasn't included in what was uploaded.
- **Grading weight split between "tests passed" and "architecture quality"** — exam A gives one strong anchor point (33% for 50% tests-passed, pulled down by a deadlock and an explicit architecture penalty) but one point isn't enough to build a formula from.
- **How many questions per sitting, and total time budget** — exam A's header only tells us about this one question's slice of a 65-minute attempt; we don't know if that was the whole exam or one of several.

Feeding in the actual lecture material (Agent 1's job) and any additional past exams or lab exercises would let this document move several of the above from "hypothesis" to "confirmed pattern."

## 12. External corroboration — a department exam catalogue, not a copied source

A web search turned up no site hosting `ResourcePool` or `forgettable_channel` verbatim — nothing suggests the professor lifts exam text from a public source. But it turned up something more useful: a former PoliTO student's public archive of **this same course**, `Programmazione di Sistema`, from its pre-Rust C++ era (`github.com/mspronesti/system-programming-polito-2021`). That archive lists five past exam problems, and they are recognizably the same family as the two Rust samples:

| C++ (older years) | Closest Rust match so far | Status |
|---|---|---|
| **RAII Guard** — RAII-limited access to a resource, N permits, block without CPU | `ResourcePool` | Translated (richer: real items, not just a counter) |
| **CBuffer\<T\>** — producer/consumer FIFO, `terminate()`/`fail()`, `consume() -> optional<T>` | `forgettable_channel` | Related but not identical — the cancellation semantics differ |
| **Joiner** — N threads rendezvous each round, blocking until all N contribute, then reset for the next round | *(none yet)* | **Not yet seen in Rust** |
| **Exchanger\<T\>** — two threads block until both call `exchange`, then swap values | *(none yet)* | **Not yet seen in Rust** |
| **SingleThreadExecutor** — task queue with `submit`/`close`/`join` lifecycle | *(none yet)* | **Not yet seen in Rust** |

This reframes the whole prediction exercise: rather than inventing plausible-sounding new domains from scratch (§9), the strongest candidates for the *next* Rust exam are almost certainly **Rust translations of the three untranslated items above** — especially `Joiner`, since it introduces a synchronization idea neither Rust sample has tested yet: a **cyclic barrier with a reset/generation hazard** (the original spec explicitly warns about a fast thread re-entering before the previous round's waiters have all been released — a subtler correctness bug than anything in `ResourcePool` or `forgettable_channel`).

Confidence: moderate-to-high that this catalogue is the *design lineage*, not proof the professor is actively working through it in order — but it's a far better-grounded prediction basis than pattern extrapolation alone.
