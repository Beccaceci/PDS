mod cli;
mod csv;

fn main() {
    // try parsing input arguments
    let args = match cli::parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error parsing arguments: {}", e);
            std::process::exit(1);
        }
    };

    // try to read the csv file and handle the errors
    match csv::read_csv(&args.filename) {
        Ok(csv_data) => {
            println!("{}", csv_data);
        }

        Err(errors) => {
            eprintln!("Received errors after read the file: ");
            for err in errors {
                eprintln!(" - {}", err)
            }
            std::process::exit(1);
        }
    }
}
