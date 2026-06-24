use std::fs::File;
use std::io::{BufRead, BufReader};
use std::fmt;

use anyhow::{anyhow, Result};

// define a struct to represent the csv data
#[derive(Debug)]
pub enum CsvValue {
    Int(i64),
    Float(f64),
    String(String)
}

// implement the function that displays the single value
impl fmt::Display for CsvValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CsvValue::Int(v) => write!(f, "{}", v),
            CsvValue::Float(v) => {
                if v.fract() == 0.0 {
                    write!(f, "{:.1}", v)
                }
                else {
                    write!(f, "{}", v)
                }
            },
            CsvValue::String(v) => write!(f, "{}", v),
        }
    }
}

impl CsvValue {
    pub fn to_f64(&self) -> f64 {
        match self {
            CsvValue::Int(v) => *v as f64,
            CsvValue::Float(v) => *v,
            CsvValue::String(s) => s.len() as f64, // The spec: text value is its length
        }
    }
}

// define a struct to represent a row of the csv file
#[derive(Debug)]
pub struct Row {
    values: Vec<CsvValue>,
}

impl Row {
    pub fn values(&self) -> &[CsvValue] {
        &self.values
    }
}

// define a struct to represent the csv file
pub struct Csv {
    header: Vec<String>,
    rows: Vec<Row>,
}

impl Csv {
    pub fn header(&self) -> &[String] {
        &self.header
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
}

// this function tries to interpret a CSV cell as the most specific type possible
fn parse_cell(cell: &str) -> CsvValue {
    let trimmed = cell.trim(); // remove surrounding withespace

    if let Ok(value) = trimmed.parse::<i64>() { // tries to parse as an integer
        CsvValue::Int(value)
    }
    else if let Ok(value) = trimmed.parse::<f64>() { // tries to parse as a float
        CsvValue::Float(value)
    }
    else { // if both parsings fail, treats it as a string
        CsvValue::String(trimmed.to_string())
    }
}


pub fn read_csv(filename: &str) -> Result<Csv, anyhow::Error> {
    // try opening the file
    let file = File::open(filename).map_err(|e| anyhow!("Error while opening the file {} : {}", filename, e))?;

    // read the whole content of the file
    // BufReader makes reading from a file more efficient, especially line by line
    // without buffering, reading could be slower because it might access the file system too often
    let reader = BufReader::new(file);

    // read the csv line by line
    let mut lines = reader
                                                        .lines()// reader.lines() returns an iterator over the file’s lines
                                                        .enumerate(); // enumerate() wraps the iterator so each line also gets its index

    // try collecting the header line by extracting the first line
    let header_line = lines
                                .next() // lines.next() gets the first item from the iterator
                                .ok_or_else(|| anyhow!("CSV file is empty"))? // ok_or_else(...) transforms the Option<T> into a [Result<T, E>], mapping Some(v) to Ok(v) and None to Err
                                .1 // because of enumerate(), the value after ok_or_else(...)? is actually (line_number, line_result)
                                .map_err(|e| anyhow!("Error while reading header: {e}"))?;

    // extract the header from the header line by splitting it
    let header: Vec<String> = header_line
                                        .split(",")// splits the string wherever there is a comma
                                        .map(|part: &str| part.trim().to_string())// for each piece, trim() removes whitespace around it and to_string() converts it into an owned String
                                        .collect(); // .collect() collects all transformed pieces into a Vec<String>

    // create an empty vector to store all parsed rows
    let mut rows = Vec::new();

    // iterate over the remaining lines (starting by the second one beacuse the first one is the header)
    for (line_index, line_result) in lines {
        // + 1 converts from zero-based to one-based numbering
        // another + 1 accounts for the header line already consumed earlie
        let line_number = line_index + 2;

        // if reading the line failed, it turns the I/O error into an anyhow::Error with a helpful message
        let line_content = line_result.map_err(|e| anyhow!("Error while reading line {line_number}: {e}"))?;

        if line_content.trim().is_empty() { // skip blank lines
            continue;
        }

        // split the line on commas
        let cells:Vec<&str> = line_content.split(",").collect();

        // checks that the row has the same number of columns as the header
        if cells.len() != header.len() {
            return Err(anyhow!("Malformed CSV at line {line_number}: expected {} columns, found {}", header.len(), cells.len()));
        }

        // converts each cell into a CsvValue
        let values = cells
                                        .into_iter()
                                        .map(parse_cell)
                                        .collect();

        // wraps the parsed values in a Row and adds that row to the rows vector
        rows.push(Row {values});
    }


    Ok(Csv {header, rows})
}