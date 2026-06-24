// handle the rguments passed by main.rs file

pub fn parse_arguments () -> Result<(String, usize), String> {
    let args: Vec<String> = std::env::args().collect();

    // Check if filename is provided as the second argument
    let filename = if args.len() > 1 {
        args[1].clone()
    }
    else {
        return Err("Filename not provided as the second argument.".to_string());
    };

    // Check if number of lines is provided as the fourth argument
    let num_lines = if args.len() > 3 {
        match args[3].parse::<usize>() {
            Ok(n) if n > 0 => n,
            Ok(_) => return Err("Number of lines must be positive.".to_string()),
            Err(_) => return Err("Number of lines must be a valid positive integer.".to_string()),
        }
    }
    else { // Set the number of lines to a default value
        10 
    };

    Ok((filename, num_lines))
}