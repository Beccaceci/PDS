use anyhow::{anyhow, Result};
use crate::aggregator::{Aggregator, Count, Sum, Average, Min, Max};
use crate::csv::Row;

// it creates and returns the right kind of Aggregator based on the mode string
pub fn make_aggregator(mode: &str) -> Result<Box<dyn Aggregator>> {
    match mode {
        "count" => Ok(Box::new(Count::new())),
        "sum" => Ok(Box::new(Sum::new())),
        "avg" => Ok(Box::new(Average::new())),
        "min" => Ok(Box::new(Min::new())),
        "max" => Ok(Box::new(Max::new())),
        _ => Err(anyhow::anyhow!("invalid aggregation mode: {mode}")),
    }
}


// returns a boxed closure Box<dyn Fn(&Row) -> bool> that can be used to test whether a Row matches the filter
// expression is the raw filter string (ex. "age>=18")
// headers is the column names, used to find which column the filter refers to
pub fn make_filter(expression: &str, headers: &[String]) -> Result<Box<dyn Fn(&Row) -> bool>> {
    // defined the supported operators the parser can understand
    let operators = ["<=", ">=", "=", "!=", "<", ">"];

    // find the operator inside the expression (if exists)
    let (operator, position) = operators
                                                    .iter() // loop over the operator list until an operator in the list is found and return the associated index
                                                    .find_map(|op| expression.find(op).map(|pos| (*op, pos))) // immediately stop when a match is found and return the associated index
                                                    .ok_or_else(|| anyhow!("malformed filter expression: {expression}"))?; // if none are found return an error

    // split the expression into column name and value
    let column_name = expression[..position].trim(); // column_name = what is on the left side with respect to the operator found
    let raw_value = expression[position + operator.len()..].trim(); // raw_value = what is on the right side with respect to the operator found

    // continue if and only if both sides exist
    if column_name.is_empty() || raw_value.is_empty() {
        return Err(anyhow!("malformed filter expression: {expression}"));
    }

    // find the column index from the headers
    let column_index = headers
                                    .iter()// iterate over the header list
                                    .position(|header| header == column_name)// immediately stop when a match is found and return the associate column
                                    .ok_or_else(|| anyhow!("unknown column in filter: {column_name}"))?;  // if none are found return an error

    // precompute the expected value
    let expected_text = raw_value.to_string();
    let expected_number = raw_value.parse::<f64>().ok(); // available only if parsing as f64 succeeds


    // create a closure that returns true if the row matches, false otherwise
    let filter = move |row: &Row| -> bool {
        // read the target cell from the row
        let Some(cell) = row.values().get(column_index) else {
            return false;
        };

        // convert the cell to text and (maybe) to number
        let cell_text = cell.to_string();
        let cell_number = cell_text.parse::<f64>().ok();

        // apply the chosen operator
        // for each operator, if both the cell and the expected value can be parsed as numbers, compare numerically, otherwise compare as strings
        match operator {
            "=" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left == right
                }
                else {
                    cell_text == expected_text
                }
            }
            "!=" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left != right
                }
                else {
                    cell_text != expected_text
                }
            }
            ">" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left > right
                }
                else {
                    cell_text > expected_text
                }
            }
            "<" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left < right
                }
                else {
                    cell_text < expected_text
                }
            }
            ">=" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left >= right
                }
                else {
                    cell_text >= expected_text
                }
            }
            "<=" => {
                if let (Some(left), Some(right)) = (cell_number, expected_number) {
                    left <= right
                }
                else {
                    cell_text <= expected_text
                }
            }
            _ => false,
        }
    };

    Ok(Box::new(filter))
}