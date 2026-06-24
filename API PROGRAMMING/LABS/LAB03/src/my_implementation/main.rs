mod cli;
mod csv;
mod aggregator;
mod analysis;
mod output;

fn main() {
    // Call your parser. If it fails, clap's exit() will automatically print the error message and stop the program
    // trying to parse input arguments
    let args = match cli::parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error parsing arguments: {}", e);
            std::process::exit(1);
        }
    };

    /*
    // Debug parsed arguments
    println!("Parsed Arguments: {:#?}", args);
    */


    // try to read the csv file and handle the errors
    let csv_data = match csv::read_csv(&args.filename) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error reading CSV file: {}", e);
            std::process::exit(1);
        }
    };

    /*
    // Verify the CSV content was properly extracted
    println!("\nCSV File Contents:");
    println!("Header: {:?}", csv_data.header());
    println!("Number of rows: {}", csv_data.rows().len());
    println!("\nFirst few rows:");
    for (i, row) in csv_data.rows().iter().take(5).enumerate() {
        println!("Row {}: {:?}", i + 1, row);
    }
    */

    // create the aggregator
    let mut aggregator = match analysis::make_aggregator(&args.mode) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error creating aggregator: {}", e);
            std::process::exit(1);
        }
    };

    // create the filter (if specified)
    let filter = match &args.filter {
        Some(expression) => match analysis::make_filter(expression, csv_data.header()) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("Error creating filter: {}", e);
                std::process::exit(1);
            }
        },
        None => None,
    };

    // bool if the specified mode is "count", false otherwise
    let is_count_mode = args.mode == "count";

    // throw an error if a column name is specified for count mode or no column names are specified in the other scenarios
    match (is_count_mode, args.column.is_some()) {
        (false, false) => {
            eprintln!("Error: --column is required unless --mode count is used");
            std::process::exit(1);
        }
        (true, true) => {
            eprintln!("Error: --column must not be provided with --mode count");
            std::process::exit(1);
        }
        _ => {}
    }

    // find the target column index (if not in count mode)
    let mut target_col_idx = 0; // it will store the index of the column the program should analyze
    if !is_count_mode {
        // if a column name is specified, find the index of the column
        if let Some(col_name) = &args.column {
            // search through the CSV header and returns the index of the column whose name matches col_name
            target_col_idx = match csv_data
                .header() // search through the CSV header
                .iter() // convert the header into an iterator
                .position(|h| h == col_name) // find the index of the column whose name matches col_name
            {
                Some(idx) => idx, // store the valid result
                None => { // print an error
                    eprintln!("Error: column '{}' not found in CSV", col_name);
                    std::process::exit(1);
                }
            };
        }
    }


    // counter that reports how many rows were actually included in the analysis
    let mut rows_analyzed = 0;

    // loop through each row in the CSV
    for row in csv_data.rows() {
        // if a filter exists, apply it, otherwise skip this row
        if let Some(ref f) = filter {
            if !f(row) {
                continue; // row filtered out
            }
        }

        // filter passed
        rows_analyzed += 1;

        // extract the value and update the aggregator
        let value = if is_count_mode {
            0.0 // count mode ignores the value, so we can just pass 0.0
        }
        else {
            // we know target_col_idx is valid because we checked the header
            row.values()[target_col_idx].to_f64()
        };

        // update the aggregator
        aggregator.update(value);
    }

    // print the final result
    output::print_output(&args, &*aggregator, rows_analyzed);
}