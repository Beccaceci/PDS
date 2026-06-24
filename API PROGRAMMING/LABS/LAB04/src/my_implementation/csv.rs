use anyhow::{anyhow, Result};

use crate::dataset::{CellValue, ColumnType, Dataset, Row};


// Function to parse a CSV file into a Dataset
pub fn parse_csv(content: &str) -> Result<Dataset> {
    // Split the content into lines and trim whitespace
    let mut lines = content.lines();

    // Get the header line and infer column types
    let header_line = lines
        .next()
        .ok_or_else(|| anyhow!("CSV file is empty"))?;

    // Split the header line into cells and trim whitespace
    let headers: Vec<String> = header_line
        .split(',')
        .map(|cell| cell.trim().to_string())
        .collect();

    // Get the first data line and infer column types
    let first_data_line = lines.next();

    // If there is a first data line, infer column types from it; otherwise, use text columns
    let column_types = match first_data_line {
        Some(line) => {
            let inferred: Vec<ColumnType> = line
                .split(',')
                .map(|cell| {
                    let cell = cell.trim();
                    if cell.parse::<i32>().is_ok() {
                        ColumnType::Integer
                    }
                    else if cell.parse::<f64>().is_ok() {
                        ColumnType::Float
                    }
                    else {
                        ColumnType::Text
                    }
                })
                .collect();

            // Check if the inferred types match the header length
            if inferred.len() != headers.len() {
                return Err(anyhow!(
                    "First data row has {} columns but header has {}",
                    inferred.len(),
                    headers.len()
                ));
            }

            inferred
        }
        None => (0..headers.len()).map(|_| ColumnType::Text).collect(),
    };

    // Parse the remaining lines into rows
    let mut rows = Vec::new();

    // If there is a first data line, parse it and add it to the rows
    if let Some(line) = first_data_line {
        rows.push(parse_row(line, &column_types, headers.len())?);
    }

    // Parse the remaining lines into rows
    for line in lines {
        rows.push(parse_row(line, &column_types, headers.len())?);
    }

    Ok(Dataset {
        headers,
        column_types,
        rows,
    })
}


// Function to parse a single row of CSV data
fn parse_row(line: &str, column_types: &[ColumnType], expected_columns: usize) -> Result<Row> {
    // Split the line into cells and trim whitespace
    let raw_cells: Vec<&str> = line.split(',').map(|cell| cell.trim()).collect();

    // Check if the number of cells matches the expected number of columns
    if raw_cells.len() != expected_columns {
        return Err(anyhow!(
            "Malformed row: expected {} columns but found {}",
            expected_columns,
            raw_cells.len()
        ));
    }

    // Convert each cell to the appropriate type
    let cells = raw_cells
        .into_iter()
        .zip(column_types.iter())
        .map(|(raw_cell, column_type)| match column_type {
            ColumnType::Text => Ok(CellValue::Text(raw_cell.to_string())),
            ColumnType::Integer => raw_cell
                .parse::<i32>()
                .map(CellValue::Integer)
                .map_err(|e| anyhow!("Invalid integer value '{}': {}", raw_cell, e)),
            ColumnType::Float => raw_cell
                .parse::<f64>()
                .map(CellValue::Float)
                .map_err(|e| anyhow!("Invalid float value '{}': {}", raw_cell, e)),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Row { cells })
}

#[cfg(test)]
mod tests {
    use super::{parse_csv, parse_row};
    use crate::dataset::{CellValue, ColumnType, Row};


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_rejects_empty_content() {
        // Attempt to parse an empty CSV file
        let result = parse_csv("");
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_parses_header_only_file() {
        // Create a CSV file with only headers
        let dataset = parse_csv("name,age,score").expect("header-only CSV should parse");
        // Assert that the dataset has the correct headers and empty rows
        assert_eq!(dataset.headers, vec!["name", "age", "score"]);
        assert_eq!(
            dataset.column_types,
            vec![ColumnType::Text, ColumnType::Text, ColumnType::Text]
        );
        assert!(dataset.rows.is_empty());
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_parses_multiline_csv_with_inferred_types() {
        // Create a CSV file with multiple lines and inferred types
        let dataset = parse_csv("name,age,score\nAlice,30,9.5\nBob,20,6.5")
            .expect("valid CSV should parse");
        // Assert that the dataset has the correct headers, column types and rows
        assert_eq!(dataset.headers, vec!["name", "age", "score"]);
        assert_eq!(
            dataset.column_types,
            vec![ColumnType::Text, ColumnType::Integer, ColumnType::Float]
        );
        assert_eq!(dataset.rows.len(), 2);
        assert_eq!(
            dataset.rows[0],
            Row {
                cells: vec![
                    CellValue::Text("Alice".to_string()),
                    CellValue::Integer(30),
                    CellValue::Float(9.5),
                ],
            }
        );
        assert_eq!(
            dataset.rows[1],
            Row {
                cells: vec![
                    CellValue::Text("Bob".to_string()),
                    CellValue::Integer(20),
                    CellValue::Float(6.5),
                ],
            }
        );
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_trims_whitespace_in_headers_and_cells() {
        // Create a CSV file with whitespace in the headers and cells
        let dataset = parse_csv(" name , age , score \n Alice , 30 , 9.5 ")
            .expect("CSV with whitespace should parse");
        // Assert that the dataset has the correct headers and cells
        assert_eq!(dataset.headers, vec!["name", "age", "score"]);
        assert_eq!(
            dataset.rows[0],
            Row {
                cells: vec![
                    CellValue::Text("Alice".to_string()),
                    CellValue::Integer(30),
                    CellValue::Float(9.5),
                ],
            }
        );
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_rejects_first_data_row_with_wrong_width() {
        // Create a CSV file with a row that has the wrong number of columns
        let result = parse_csv("name,age\nAlice,30,extra");
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("First data row has 3 columns but header has 2")
        );
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_rejects_later_row_with_wrong_width() {
        // Create a CSV file with a row that has the wrong number of columns
        let result = parse_csv("name,age\nAlice,30\nBob");
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Malformed row: expected 2 columns but found 1")
        );
    }


    // Function that tests the parse_csv function
    #[test]
    fn parse_csv_rejects_invalid_integer_value_for_inferred_integer_column() {
        // Create a CSV file with an invalid integer value for an inferred integer column
        let result = parse_csv("name,age\nAlice,30\nBob,not_an_int");
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid integer value"));
    }


    // Function that tests the parse_row function
    #[test]
    fn parse_row_parses_text_integer_and_float_cells() {
        // Create a row with text, integer, and float cells
        let row = parse_row(
            "Alice,30,9.5",
            &[ColumnType::Text, ColumnType::Integer, ColumnType::Float],
            3,
        )
        .expect("row should parse");

        // Assert that the row has the correct cells
        assert_eq!(
            row,
            Row {
                cells: vec![
                    CellValue::Text("Alice".to_string()),
                    CellValue::Integer(30),
                    CellValue::Float(9.5),
                ],
            }
        );
    }


    // Function that tests the parse_row function
    #[test]
    fn parse_row_rejects_wrong_column_count() {
        // Create a row with the wrong number of columns
        let result = parse_row("Alice,30", &[ColumnType::Text], 1);
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Malformed row: expected 1 columns but found 2")
        );
    }


    // Function that tests the parse_row function
    #[test]
    fn parse_row_rejects_invalid_float_value() {
        // Create a row with an invalid float value
        let result = parse_row("oops", &[ColumnType::Float], 1);
        // Assert that the result is an error
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid float value"));
    }
}
