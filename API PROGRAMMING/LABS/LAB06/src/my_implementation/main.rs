//! Laboratorio 6 - Esercizi avanzati sulla concorrenza
//!
//! Ogni esercizio è in un modulo separato. Il testo con la specifica di
//! ciascun esercizio si trova in `Lab6.md`.
//!
//! Per eseguire i test di tutti gli esercizi:
//!     cargo test
//!
//! Per eseguire i test di un singolo esercizio:
//!     cargo test es1_barrier
//!     cargo test es2_bounded_queue
//!     ...

mod es1_barrier;
mod es2_mpsc;
mod es3_pipeline;
mod es4_concurrent_cache;
mod es5_fan_out_fan_in;

fn main() {
    println!("Laboratorio 6 - Esercizi avanzati sulla concorrenza");
    println!("Eseguire i test con: cargo test");
}
