//! Laboratorio 7 - Esercizi in stile esame
//!
//! Ogni esercizio è in un modulo separato. Il file di testo con la specifica
//! di ciascun esercizio si trova in `Lab7.md`.
//!
//! Per eseguire i test di tutti gli esercizi:
//!     cargo test
//!
//! Per eseguire i test di un singolo esercizio:
//!     cargo test es1_asymmetric_rendezvous
//!     cargo test es2_wait_group
//!     ...

mod es1_asymmetric_rendezvous;
mod es2_wait_group;
mod es3_priority_channel;
mod es4_watchdog;
mod es5_memo_cache;
mod es6_throttle;

fn main() {
    println!("Laboratorio 7 - Esercizi in stile esame");
    println!("Eseguire i test con: cargo test");
}
