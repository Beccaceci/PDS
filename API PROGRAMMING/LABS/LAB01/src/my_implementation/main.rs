mod cli;
mod io;

fn main () {
    let args = cli::parse_arguments(); // parsing the arguments

    let (filename, lines_to_print) = match args { // handling the result of parsing the arguments
        Ok((filename, num_lines)) => (filename, num_lines), // if parsing is successful, get the filename and number of lines to print
        Err(e) => { // if parsing fails, print the error message and exit
            eprintln!("Error: {}", e);
            return;
        }
    };

    let file_content = io::read_file(&filename); // reading the file content
    
    // print the content of the file
    match file_content { // handling the result of reading the file
        Ok(content) => { // if reading is successful, we print the content of the file
            for line in content.lines().take(lines_to_print) { // print the specified number of lines from the file content
                println!("{}", line);
            }
        },
        Err(e) => eprintln!("Error: {}", e) // if reading fails, print the error message
    }
}
