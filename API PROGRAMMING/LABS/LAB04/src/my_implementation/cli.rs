use std::path::PathBuf;

use clap::{ArgGroup, Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
#[command(
    group(
        ArgGroup::new("action")
            .args(["mode", "export"])
            .required(true)
            .multiple(false)
    )
)]
pub struct Cli {
    /// Input dataset path (.csv or exported .json).
    pub input: PathBuf,

    /// Analysis mode to run on the dataset.
    #[arg(long, value_enum)]
    pub mode: Option<Mode>,

    /// Target column for modes that operate on a specific column.
    #[arg(long, requires = "mode", conflicts_with = "export")]
    pub column: Option<String>,

    /// Row filters such as "age>25" or "name=Bob".
    #[arg(long = "filter")]
    pub filters: Vec<String>,

    /// Text transform in the form "column=operation".
    #[arg(long)]
    pub transform: Option<String>,

    /// Group analysis results by the given column.
    #[arg(long = "group-by", requires = "mode", conflicts_with = "export")]
    pub group_by: Option<String>,

    /// Export the dataset to a JSON file.
    #[arg(long, conflicts_with_all = ["mode", "group_by"])]
    pub export: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use clap::error::ErrorKind;

    fn parse_ok(args: Vec<&str>) -> Cli {
        Cli::try_parse_from(args).expect("expected CLI parsing to succeed")
    }

    fn parse_err_kind(args: Vec<&str>) -> ErrorKind {
        Cli::try_parse_from(args)
            .expect_err("expected CLI parsing to fail")
            .kind()
    }

    #[test]
    fn test_parses_basic_analysis_with_input_and_mode_sum() {
        let cli = parse_ok(vec!["program_name", "input.csv", "--mode", "sum"]);

        assert_eq!(cli.input, PathBuf::from("input.csv"));
        assert_eq!(cli.mode, Some(Mode::Sum));
        assert_eq!(cli.column, None);
        assert!(cli.filters.is_empty());
        assert_eq!(cli.transform, None);
        assert_eq!(cli.group_by, None);
        assert_eq!(cli.export, None);
    }

    #[test]
    fn test_parses_complex_analysis_with_grouping_filters_and_transform() {
        let cli = parse_ok(vec![
            "program_name",
            "studenti.csv",
            "--mode",
            "avg",
            "--column",
            "voto",
            "--group-by",
            "corso",
            "--filter",
            "age>25",
            "--filter",
            "name=Bob",
            "--transform",
            "name=uppercase",
        ]);

        assert_eq!(cli.input, PathBuf::from("studenti.csv"));
        assert_eq!(cli.mode, Some(Mode::Avg));
        assert_eq!(cli.column.as_deref(), Some("voto"));
        assert_eq!(cli.group_by.as_deref(), Some("corso"));
        assert_eq!(cli.transform.as_deref(), Some("name=uppercase"));
        assert_eq!(cli.export, None);
        assert_eq!(cli.filters, vec!["age>25", "name=Bob"]);
    }

    #[test]
    fn test_parses_export_only_with_input_and_export_path() {
        let cli = parse_ok(vec!["program_name", "input.csv", "--export", "data.json"]);

        assert_eq!(cli.input, PathBuf::from("input.csv"));
        assert_eq!(cli.export, Some(PathBuf::from("data.json")));
        assert_eq!(cli.mode, None);
        assert_eq!(cli.column, None);
        assert!(cli.filters.is_empty());
        assert_eq!(cli.transform, None);
        assert_eq!(cli.group_by, None);
    }

    #[test]
    fn test_fails_when_no_action_argument_is_provided() {
        let error_kind = parse_err_kind(vec!["program_name", "input.csv"]);

        assert_eq!(error_kind, ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn test_fails_when_export_and_mode_are_combined() {
        let error_kind = parse_err_kind(vec![
            "program_name",
            "input.csv",
            "--export",
            "data.json",
            "--mode",
            "sum",
        ]);

        assert_eq!(error_kind, ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_fails_when_export_and_group_by_are_combined() {
        let error_kind = parse_err_kind(vec![
            "program_name",
            "input.csv",
            "--export",
            "data.json",
            "--group-by",
            "corso",
        ]);

        assert_eq!(error_kind, ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_fails_when_column_is_provided_without_mode_even_with_export() {
        let error_kind = parse_err_kind(vec![
            "program_name",
            "input.csv",
            "--export",
            "data.json",
            "--column",
            "voto",
        ]);

        assert_eq!(error_kind, ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_fails_when_group_by_is_provided_without_mode_even_with_export() {
        let error_kind = parse_err_kind(vec![
            "program_name",
            "input.csv",
            "--export",
            "data.json",
            "--group-by",
            "corso",
        ]);

        assert_eq!(error_kind, ErrorKind::ArgumentConflict);
    }
}
