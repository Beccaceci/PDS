//! Laboratorio 5: Esercizi sulla concorrenza
//!
//! Ogni esercizio è in un modulo separato. Il file di testo con la specifica
//! di ciascun esercizio si trova in `Lab5.md`.
//!
//! Per eseguire i test di tutti gli esercizi:
//!     cargo test
//!
//! Per eseguire i test di un singolo esercizio:
//!     cargo test es1_parallel_sum
//!     cargo test es2_count_divisible
//!     ...

mod es1_parallel_sum;
mod es2_count_divisible;
mod es3_shared_stats;
mod es4_word_freq;
mod es5_parallel_grep;
mod es6_cache;

fn main() {
    println!("Laboratorio 5: Esercizi sulla concorrenza");
    println!("Eseguire i test con: cargo test");
}
