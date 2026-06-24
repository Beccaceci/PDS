use std::fmt;
use crate::aggregator::Aggregator;
use crate::cli::Args;

// this struct stores the information that will be printed
struct Output<'a> {
    mode: &'a str,
    column: Option<&'a str>,
    filter: Option<&'a str>,
    result: String,
    rows_analyzed: usize,
}


// these string references inside Output are valid as long as the borrowed Args data is valid
impl<'a> Output<'a> {
    // this creates an Output from the CLI arguments and computed values
    fn new(args: &'a Args, aggregator: &dyn Aggregator, rows_analyzed: usize) -> Self {
        Self {
            mode: aggregator.mode_name(),
            column: args.column.as_deref(),
            filter: args.filter.as_deref(),
            result: aggregator.result(),
            rows_analyzed,
        }
    }
}

// implement Display for Output with any lifetime
impl fmt::Display for Output<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "mode: {}", self.mode)?;

        if let Some(column) = self.column {
            writeln!(f, "column: {}", column)?;
        }

        if let Some(filter) = self.filter {
            writeln!(f, "filter: {}", filter)?;
        }

        // 3. Print the string directly
        writeln!(f, "result: {}", self.result)?;
        write!(f, "rows_analyzed: {}", self.rows_analyzed)
    }
}


pub fn print_output(args: &Args, aggregator: &dyn Aggregator, rows_analyzed: usize) {
    // creates an Output
    let output = Output::new(args, aggregator, rows_analyzed);

    // print it using the Display trait
    println!("{output}");
}