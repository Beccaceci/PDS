mod aggregator;
mod analysis;
mod cli;
mod csv;
mod dataset;

use std::collections::BTreeMap;
use std::fs;

use anyhow::{anyhow, Result};
use clap::Parser;

use crate::cli::{Cli, Mode};
use crate::dataset::{CellValue, ColumnType, Dataset, Row};

fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Cli::parse();

    // Read the input file
    let content = fs::read_to_string(&args.input)?;
    // Determine the file extension (default to CSV)
    let extension = args
        .input
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("csv");

    // Parse the input file based on the file extension
    let mut dataset = match extension {
        "json" => Dataset::load_json(&content)?,
        "csv" => csv::parse_csv(&content)?,
        _ => csv::parse_csv(&content)?,
    };

    // Apply transformations if specified
    if let Some(transform) = args.transform.as_deref() {
        let (column, operation) = transform
            .split_once('=')
            .ok_or_else(|| anyhow!("Invalid transform format: {}", transform))?;
        dataset.transform(column.trim(), operation.trim())?;
    }

    // Apply filters if specified
    if let Some(export_path) = args.export.as_ref() {
        let export_path = export_path
            .to_str()
            .ok_or_else(|| anyhow!("Export path must be valid UTF-8"))?;
        dataset.export_json(export_path)?;
        return Ok(());
    }

    // Apply filters if specified
    let filters = args
        .filters
        .iter()
        .map(|expression| {
            validate_filter_expression(expression, &dataset)?;
            analysis::make_filter(expression, &dataset.headers)
        })
        .collect::<Result<Vec<_>>>()?;

    // Apply group_by if specified
    let grouped = args.group_by.is_some();
    // If group_by is not specified, group by the entire dataset
    let groups = match args.group_by.as_deref() {
        Some(column) => dataset.group_by(column)?,
        None => default_group(&dataset),
    };

    // Apply mode if specified
    let mode = args.mode.ok_or_else(|| anyhow!("Missing analysis mode"))?;
    // If mode is count, skip aggregation and print the number of rows
    let value_column_index = match mode {
        Mode::Count => None,
        _ => Some(resolve_column_index(&dataset, args.column.as_deref())?),
    };

    // Perform the analysis based on the specified mode
    for (group_key, rows) in groups {
        // Filter the rows based on the specified filters
        let filtered_rows: Vec<&Row> = rows
            .into_iter()
            .filter(|row| filters.iter().all(|filter| filter(row)))
            .collect();

        // If no rows match the filters, skip this group
        if grouped && filtered_rows.is_empty() {
            continue;
        }

        print_configuration(&args, mode);

        // Aggregate the filtered rows based on the specified mode
        let mut aggregator = analysis::make_aggregator(mode_name(mode))?;
        // If mode is count, skip aggregation and print the number of rows
        match value_column_index {
            Some(column_index) => {
                for row in &filtered_rows {
                    let cell = row
                        .cells
                        .get(column_index)
                        .ok_or_else(|| anyhow!("Row is missing target column"))?;
                    aggregator.update(cell_to_f64(cell));
                }
            }
            None => {
                for _ in &filtered_rows {
                    aggregator.update(1.0);
                }
            }
        }

        // Print the result based on the specified mode
        if grouped {
            println!("{}: {}", group_key, aggregator.result());
        }
        else {
            println!("result: {}", aggregator.result());
        }
        println!("rows_analyzed: {}", filtered_rows.len());
    }

    Ok(())
}


// Function to group the dataset by a single column
fn default_group<'a>(dataset: &'a Dataset) -> BTreeMap<String, Vec<&'a Row>> {
    // Create a map to store the groups
    let mut groups = BTreeMap::new();
    // Iterate over the rows in the dataset
    groups.insert("result".to_string(), dataset.rows.iter().collect());
    groups
}


// Function that resolves the index of a column in the dataset based on its name
fn resolve_column_index(dataset: &Dataset, column: Option<&str>) -> Result<usize> {
    // If column is not specified, return the index of the first column
    let column = column.ok_or_else(|| anyhow!("Missing target column"))?;
    // Find the index of the column in the headers
    dataset
        .headers
        .iter()
        .position(|header| header == column)
        .ok_or_else(|| anyhow!("Column not found: {}", column))
}


// Function to validate a filter expression
fn validate_filter_expression(expression: &str, dataset: &Dataset) -> Result<()> {
    // Split the expression into column, operator, and target value
    let (column, operator, _) = split_filter_expression(expression)?;
    // Find the index of the column in the headers
    let column_index = dataset
        .headers
        .iter()
        .position(|header| header == column)
        .ok_or_else(|| anyhow!("Column '{}' not found", column))?;

    // Check if the operator is valid for the column type
    if matches!(operator, '>' | '<')
        && matches!(dataset.column_types.get(column_index), Some(ColumnType::Text))
    {
        return Err(anyhow!("Filter uses incompatible types"));
    }

    Ok(())
}


// Function to split a filter expression into column, operator, and target value
fn split_filter_expression(expression: &str) -> Result<(&str, char, &str)> {
    // Split the expression into column, operator, and target value
    for operator in ['>', '<', '='] {
        // Check if the operator is present in the expression
        if let Some((left, right)) = expression.split_once(operator) {
            // Trim whitespace from the left and right sides of the expression
            let column = left.trim();
            let value = right.trim();
            // Check if the column and value are not empty
            if column.is_empty() || value.is_empty() {
                return Err(anyhow!("Malformed filter expression: '{}'", expression));
            }
            return Ok((column, operator, value));
        }
    }

    Err(anyhow!("Malformed filter expression: '{}'", expression))
}


// Function to convert a CellValue to a f64
fn cell_to_f64(cell: &CellValue) -> f64 {
    // Convert the CellValue to a f64 based on its type
    match cell {
        CellValue::Integer(value) => *value as f64,
        CellValue::Float(value) => *value,
        CellValue::Text(value) => value.len() as f64,
    }
}

// Function to print the configuration of the analysis
fn print_configuration(args: &Cli, mode: Mode) {
    println!("mode: {}", mode_name(mode));

    if let Some(column) = args.column.as_deref() {
        println!("column: {}", column);
    }

    if let Some(group_by) = args.group_by.as_deref() {
        println!("group_by: {}", group_by);
    }

    for filter in &args.filters {
        println!("filter: {}", filter);
    }

    if let Some(transform) = args.transform.as_deref() {
        println!("transform: {}", transform);
    }
}


// Function to get the name of the mode based on the Mode enum
fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Count => "count",
        Mode::Sum => "sum",
        Mode::Avg => "avg",
        Mode::Min => "min",
        Mode::Max => "max",
    }
}
