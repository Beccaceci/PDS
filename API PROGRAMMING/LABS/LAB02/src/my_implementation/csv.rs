// read and parse the csv file
use std::fs::File;
use std::io::{self, BufRead};
use std::fmt;
use anyhow::{anyhow, Result};

// define a struct to represent the csv data
#[derive(Debug)]
enum CsvValue {
    Int(i64),
    Float(f64),
    String(String),
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

// define an enum to represent the type of a column in the csv file
#[derive(Clone, Copy)]
enum ColumnType {
    Int,
    Float,
    String,
}

// define a struct to represent a row of the csv file
pub struct Row {
    values: Vec<CsvValue>,
}

// define a struct to represent the csv file
pub struct Csv {
    header: Vec<String>,
    rows: Vec<Row>,
}

// implement the method that display the entire csv table
impl fmt::Display for Csv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // compute the maximum width for each line
        let mut widths = self.header.iter()
            .map(|h| h.len())
            .collect::<Vec<_>>();

        for row in &self.rows {
            for (i, val) in row.values.iter().enumerate() {
                let len = val.to_string().len();
                if len > widths[i] {
                    widths[i] = len;
                }
            }
        }

        // print the header line
        let header_line = self.header.iter().enumerate()
            .map(|(i, h)| format!(" {:<width$} ", h, width = widths[i]))
            .collect::<Vec<_>>()
            .join("|");
        writeln!(f, "{}", header_line)?;

        // print the separator line (ex. ---------+-----+-------)
        let sep_line = widths.iter()
            .map(|&w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+");
        writeln!(f, "{}", sep_line)?;

        // align the lines
        for row in &self.rows {
            let row_line = row.values.iter().enumerate()
                .map(|(i, v)| {
                    format!(" {:<width$} ", v.to_string(), width = widths[i])
                })
                .collect::<Vec<_>>()
                .join("|");
            writeln!(f, "{}", row_line)?;
        }

        Ok(())
    }
}


// Reads and parses a CSV file into a structured representation
// Returns Ok(Csv) if perfect, or Err(Vec<Error>) if any data inconsistencies are found
pub fn read_csv(filename: &str) -> Result<Csv, Vec<anyhow::Error>> {
    // Catastrophic error: if the file cannot be opened, we cannot proceed
    let file = File::open(filename).map_err(|e| vec![anyhow!("Error while opening the file {} : {}", filename, e)])?;
    let reader = io::BufReader::new(file);
    let mut errors = Vec::new(); // create a vector to store the parsing errors

    // create a new Csv struct to store the parsed data
    let mut csv = Csv {
        header: Vec::new(),
        rows: Vec::new(),
    };

    // read the csv file line by line
    let mut lines = reader.lines().enumerate();

    // The first line is treated strictly as column names
    let header_line = match lines.next() {
        Some((_, Ok(line))) => line,
        Some((_, Err(e))) => return Err(vec![anyhow!("Error while reading the header line: {}", e)]),
        None => return Err(vec![anyhow!("Error: the file is empty")])
    };
    csv.header = header_line.split(',').map(|s| s.trim().to_string()).collect();


    // store each fields' type in a vector to check the consistency of the data rows
    let mut field_types: Vec<Option<ColumnType>> = vec![None; csv.header.len()];

    // read the data rows
    for (line_idx, line_result) in lines {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => {
                errors.push(anyhow!("Line {}: IO Error", line_idx + 1));
                continue;
            }
        };

        let values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

        // Validate column count against header before parsing types
        if values.len() != csv.header.len() {
            errors.push(anyhow!("Row {}: Expected {} columns, found {}", line_idx+1, csv.header.len(), values.len()));
            continue;
        }

        let mut row_values = Vec::new();
        let mut row_error_found = false;
        for (i, val) in values.into_iter().enumerate() {
            let col_name = &csv.header[i];

            let parsed_result = match field_types[i] {
                // Type deduction phase: triggered only by the first data row
                None => {
                    if let Ok(v) = val.parse::<i64>() {
                        field_types[i] = Some(ColumnType::Int);
                        Ok(CsvValue::Int(v))
                    }
                    else if let Ok(v) = val.parse::<f64>() {
                        field_types[i] = Some(ColumnType::Float);
                        Ok(CsvValue::Float(v))
                    }
                    else {
                        field_types[i] = Some(ColumnType::String);
                        Ok(CsvValue::String(val.to_string()))
                    }
                }
                // Validation phase: enforce consistency with previous rows
                Some(expected_type) => match expected_type {
                    ColumnType::Int => val.parse::<i64>()
                        .map(CsvValue::Int)
                        .map_err(|_| anyhow!("Row {}, Column '{}': expected Integer, found '{}'", line_idx+1, col_name, val)),
                    ColumnType::Float => val.parse::<f64>()
                        .map(CsvValue::Float)
                        .map_err(|_| anyhow!("Row {}, Column '{}': expected Float, found '{}'", line_idx+1, col_name, val)),
                    ColumnType::String => Ok(CsvValue::String(val.to_string())) // this conversion cannot fail
                }
            };

            match parsed_result {
                Ok(csv_val) => {
                    row_values.push(csv_val)
                },
                Err(e) => {
                    errors.push(e);
                    row_error_found = true;
                }
            };

        }

        // Only commit the row to the final set if every field was valid
        if !row_error_found {
            csv.rows.push(Row {
                values: row_values
            })
        }
    }

    // Report all collected errors or return the complete table
    if errors.is_empty() {
        Ok(csv)
    }
    else {
        Err(errors)
    }
}