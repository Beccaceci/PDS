use anyhow::{anyhow, Result};

use crate::aggregator::{Aggregator, Average, Count, Max, Min, Sum};
use crate::dataset::{CellValue, Row};


// Function to create an aggregator based on the mode provided
pub fn make_aggregator(mode: &str) -> Result<Box<dyn Aggregator>> {
    // Create an aggregator based on the mode
    match mode {
        "count" => Ok(Box::new(Count::new())),
        "sum" => Ok(Box::new(Sum::new())),
        "avg" => Ok(Box::new(Average::new())),
        "min" => Ok(Box::new(Min::new())),
        "max" => Ok(Box::new(Max::new())),
        _ => Err(anyhow!(
            "Invalid mode '{}'. Available options: count, sum, avg, min, max",
            mode
        )),
    }
}


// Function to parse a filter expression and create a filter function
pub fn make_filter(expression: &str, headers: &[String]) -> Result<Box<dyn Fn(&Row) -> bool>> {
    // Parse the expression into column name, operator, and target value
    let (column_name, operator, target_value) = parse_expression(expression)?;
    // Find the index of the column in the headers
    let column_index = headers
        .iter()
        .position(|header| header == column_name)
        .ok_or_else(|| anyhow!("Column '{}' not found", column_name))?;

    if matches!(operator, '>' | '<') && target_value.parse::<f64>().is_err() {
        return Err(anyhow!("Filter uses incompatible types"));
    }

    // Convert the target value to a number if possible
    let target_string = target_value.to_string();
    let target_number = target_value.parse::<f64>().ok();

    // Create the filter function based on the column type and operator
    Ok(Box::new(move |row: &Row| {
        let Some(cell) = row.cells.get(column_index) else {
            return false;
        };

        match cell {
            CellValue::Integer(value) => compare_numbers(*value as f64, target_number, operator),
            CellValue::Float(value) => compare_numbers(*value, target_number, operator),
            CellValue::Text(value) => compare_text(value, &target_string, operator),
        }
    }))
}

fn parse_expression(expression: &str) -> Result<(&str, char, &str)> {
    let operator_position = expression
        .char_indices()
        .find(|(_, ch)| matches!(ch, '=' | '>' | '<'))
        .ok_or_else(|| anyhow!("Malformed filter expression: '{}'", expression))?;

    let operator = operator_position.1;
    let operator_index = operator_position.0;

    let column_name = expression[..operator_index].trim();
    let target_value = expression[operator_index + operator.len_utf8()..].trim();

    if column_name.is_empty() || target_value.is_empty() {
        return Err(anyhow!("Malformed filter expression: '{}'", expression));
    }

    Ok((column_name, operator, target_value))
}

fn compare_numbers(left: f64, right: Option<f64>, operator: char) -> bool {
    let Some(right) = right else {
        return false;
    };

    match operator {
        '=' => left == right,
        '>' => left > right,
        '<' => left < right,
        _ => false,
    }
}

fn compare_text(left: &str, right: &str, operator: char) -> bool {
    matches!(operator, '=') && left == right
}

#[cfg(test)]
mod tests {
    use super::{
        compare_numbers, compare_text, make_aggregator, make_filter, parse_expression,
    };
    use crate::dataset::{CellValue, Row};

    fn sample_headers() -> Vec<String> {
        vec!["name".into(), "age".into(), "score".into()]
    }

    fn sample_row() -> Row {
        Row {
            cells: vec![
                CellValue::Text("Alice".to_string()),
                CellValue::Integer(30),
                CellValue::Float(7.75),
            ],
        }
    }

    fn short_row() -> Row {
        Row {
            cells: vec![CellValue::Text("Alice".to_string())],
        }
    }

    #[test]
    fn make_aggregator_creates_count_aggregator() {
        let aggregator = make_aggregator("count").expect("count should be valid");
        assert_eq!(aggregator.mode_name(), "count");
    }

    #[test]
    fn make_aggregator_creates_avg_aggregator() {
        let aggregator = make_aggregator("avg").expect("avg should be valid");
        assert_eq!(aggregator.mode_name(), "avg");
    }

    #[test]
    fn make_aggregator_creates_sum_aggregator() {
        let aggregator = make_aggregator("sum").expect("sum should be valid");
        assert_eq!(aggregator.mode_name(), "sum");
    }

    #[test]
    fn make_aggregator_creates_min_aggregator() {
        let aggregator = make_aggregator("min").expect("min should be valid");
        assert_eq!(aggregator.mode_name(), "min");
    }

    #[test]
    fn make_aggregator_creates_max_aggregator() {
        let aggregator = make_aggregator("max").expect("max should be valid");
        assert_eq!(aggregator.mode_name(), "max");
    }

    #[test]
    fn make_aggregator_rejects_unknown_mode() {
        let error = make_aggregator("median")
            .err()
            .expect("median should be rejected");
        assert!(error.to_string().contains("Invalid mode"));
    }

    #[test]
    fn parse_expression_supports_equals_operator() {
        let (column, operator, value) =
            parse_expression("name=Alice").expect("expression should parse");
        assert_eq!(column, "name");
        assert_eq!(operator, '=');
        assert_eq!(value, "Alice");
    }

    #[test]
    fn parse_expression_supports_greater_than_operator() {
        let (column, operator, value) =
            parse_expression("age>25").expect("expression should parse");
        assert_eq!(column, "age");
        assert_eq!(operator, '>');
        assert_eq!(value, "25");
    }

    #[test]
    fn parse_expression_supports_less_than_operator_and_trims_whitespace() {
        let (column, operator, value) =
            parse_expression(" score < 10.5 ").expect("expression should parse");
        assert_eq!(column, "score");
        assert_eq!(operator, '<');
        assert_eq!(value, "10.5");
    }

    #[test]
    fn parse_expression_rejects_missing_operator() {
        assert!(parse_expression("name~Alice").is_err());
    }

    #[test]
    fn parse_expression_rejects_empty_column_name() {
        assert!(parse_expression("=Alice").is_err());
    }

    #[test]
    fn parse_expression_rejects_empty_target_value() {
        assert!(parse_expression("name=").is_err());
    }

    #[test]
    fn compare_numbers_supports_equals() {
        assert!(compare_numbers(30.0, Some(30.0), '='));
        assert!(!compare_numbers(30.0, Some(31.0), '='));
    }

    #[test]
    fn compare_numbers_supports_greater_than() {
        assert!(compare_numbers(30.0, Some(25.0), '>'));
        assert!(!compare_numbers(20.0, Some(25.0), '>'));
    }

    #[test]
    fn compare_numbers_supports_less_than() {
        assert!(compare_numbers(20.0, Some(25.0), '<'));
        assert!(!compare_numbers(30.0, Some(25.0), '<'));
    }

    #[test]
    fn compare_numbers_returns_false_when_rhs_is_missing() {
        assert!(!compare_numbers(30.0, None, '='));
        assert!(!compare_numbers(30.0, None, '>'));
        assert!(!compare_numbers(30.0, None, '<'));
    }

    #[test]
    fn compare_text_only_allows_equals() {
        assert!(compare_text("Alice", "Alice", '='));
        assert!(!compare_text("Alice", "Bob", '='));
        assert!(!compare_text("Alice", "Bob", '>'));
        assert!(!compare_text("Alice", "Bob", '<'));
    }

    #[test]
    fn make_filter_supports_integer_greater_than_comparisons() {
        let filter = make_filter("age>25", &sample_headers()).expect("valid filter");
        assert!(filter(&sample_row()));
    }

    #[test]
    fn make_filter_supports_float_less_than_comparisons() {
        let filter = make_filter("score<8.0", &sample_headers()).expect("valid filter");
        assert!(filter(&sample_row()));
    }

    #[test]
    fn make_filter_supports_numeric_equality() {
        let filter = make_filter("age=30", &sample_headers()).expect("valid filter");
        assert!(filter(&sample_row()));
    }

    #[test]
    fn make_filter_supports_string_equality() {
        let filter = make_filter("name=Alice", &sample_headers()).expect("valid filter");
        assert!(filter(&sample_row()));
    }

    #[test]
    fn make_filter_returns_false_when_string_does_not_match() {
        let filter = make_filter("name=Bob", &sample_headers()).expect("valid filter");
        assert!(!filter(&sample_row()));
    }

    #[test]
    fn make_filter_returns_false_when_numeric_predicate_does_not_match() {
        let filter = make_filter("age<25", &sample_headers()).expect("valid filter");
        assert!(!filter(&sample_row()));
    }

    #[test]
    fn make_filter_returns_false_when_row_is_missing_the_indexed_cell() {
        let filter = make_filter("age>25", &sample_headers()).expect("valid filter");
        assert!(!filter(&short_row()));
    }

    #[test]
    fn make_filter_rejects_malformed_expression() {
        assert!(make_filter("name~Alice", &sample_headers()).is_err());
    }

    #[test]
    fn make_filter_rejects_missing_column() {
        assert!(make_filter("missing=10", &sample_headers()).is_err());
    }

    #[test]
    fn make_filter_rejects_incompatible_numeric_operand() {
        let error = make_filter("age>Bob", &sample_headers())
            .err()
            .expect("should fail");
        assert!(error.to_string().contains("incompatible"));
    }

    #[test]
    fn make_filter_rejects_text_greater_than_operand() {
        let error = make_filter("name>Alice", &sample_headers())
            .err()
            .expect("should fail");
        assert!(error.to_string().contains("incompatible"));
    }

    #[test]
    fn make_filter_rejects_text_less_than_operand() {
        let error = make_filter("name<Bob", &sample_headers())
            .err()
            .expect("should fail");
        assert!(error.to_string().contains("incompatible"));
    }
}
