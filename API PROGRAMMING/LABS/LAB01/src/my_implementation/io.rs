use std::fs;
use std::io::ErrorKind;

pub fn read_file (filename: &str) -> Result<String, String> {
    match fs::File::open(filename) { // try to open the file
        Ok(_) => match fs::read_to_string(filename) { // try to read the file content
            Ok(contents) => Ok(contents), // return the file content if successful
            Err(e) => Err(format!("Failed to read file '{}': {}", filename, e)),
        },
        Err(ref e) if e.kind() == ErrorKind::NotFound => { // file not found
            Err(format!("File '{}' does not exist.", filename))
        }
        Err(ref e) if e.kind() == ErrorKind::PermissionDenied => { // no permission to read the file
            Err(format!("No read permission for file '{}'.", filename))
        }
        Err(e) => Err(format!("Failed to open file '{}': {}", filename, e))
    }
}