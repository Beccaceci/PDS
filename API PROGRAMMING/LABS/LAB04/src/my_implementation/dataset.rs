use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)] // Tells Serde not to add a type field to the JSON
pub enum CellValue {
    Text(String),
    Integer(i32),
    Float(f64)
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum ColumnType {
    Text,
    Integer,
    Float
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(transparent)] // Forces Serdeto to serial this directly as an array of CellValue
pub struct Row {
    pub cells: Vec<CellValue>
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Dataset {
    pub headers: Vec<String>,
    pub column_types: Vec<ColumnType>,
    pub rows: Vec<Row>
}

impl Dataset {
    // --------------------------------------------------------
    // 1. LOAD
    // --------------------------------------------------------
    pub fn load_json(json_content: &str) -> Result<Self> {
        // Use serde_json::from_str to convert the string into a Dataset
        // Return Ok(dataset) or an error
        match serde_json::from_str(json_content) {
            Ok(value) => Ok(value),
            Err(e) => Err(anyhow::anyhow!("Error parsing JSON: {}", e)),
        }
    }

    // --------------------------------------------------------
    // 2. TRANSFORM
    // --------------------------------------------------------
    // Uses `&mut self` because it modifies the internal Vec<Row> in-place
    pub fn transform(&mut self, col_name: &str, operation: &str) -> Result<()> {
        // Find the index of col_name in self.headers
        let col_index = self.headers.iter().position(|h| h == col_name)
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found", col_name))?;

        if !matches!(self.column_types.get(col_index), Some(ColumnType::Text)) {
            return Err(anyhow::anyhow!(
                "Transform can only be applied to Text columns"
            ));
        }

        // Determine if operation is "uppercase" or "lowercase"
        let transform_fn: Box<dyn Fn(&str) -> String> = match operation {
            "uppercase" => Box::new(|s: &str| s.to_uppercase()),
            "lowercase" => Box::new(|s: &str| s.to_lowercase()),
            _ => return Err(anyhow::anyhow!("Unknown transform operation: {}", operation)),
        };

        // Iterate over self.rows as mutable (&mut row)
        for row in &mut self.rows {
            // Apply a closure to modify the specific CellValue at the found index
            if let Some(cell) = row.cells.get_mut(col_index) {
                if let CellValue::Text(text) = cell {
                    *text = transform_fn(text);
                }
            }
        }

        Ok(())
        
    }

    // --------------------------------------------------------
    // 3. EXPORT
    // --------------------------------------------------------
    // Uses `&self` because exporting doesn't change the data in memory
    pub fn export_json(&self, filepath: &str) -> Result<()> {
        // Use serde_json::to_writer or to_string to save `self` to the disk
        use std::fs::File;
        use std::io::BufWriter;

        let file = File::create(filepath)?;
        let writer = BufWriter::new(file);

        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }

    // --------------------------------------------------------
    // 4. GROUP BY
    // --------------------------------------------------------
    // Uses `&self`. Returns a BTreeMap where the key is the string value of the column and the value is a Vec containing IMMUTABLE REFERENCES (&Row) to the original rows.
    pub fn group_by(&self, col_name: &str) -> Result<BTreeMap<String, Vec<&Row>>> {
        // Find the index of col_name in self.headers
        let col_index = self
            .headers
            .iter()
            .position(|h| h == col_name)
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found", col_name))?;

        // Create an empty BTreeMap
        let mut groups: BTreeMap<String, Vec<&Row>> = BTreeMap::new();

        // Iterate over rows and build the key from the cell at col_index
        for row in &self.rows {
            let cell = row
                .cells
                .get(col_index)
                .ok_or_else(|| anyhow::anyhow!("Row is missing column '{}'", col_name))?;

            let key = match cell {
                CellValue::Text(s) => s.clone(),
                CellValue::Integer(i) => i.to_string(),
                CellValue::Float(f) => f.to_string(),
            };

            // Push a reference to the row into the group
            groups.entry(key).or_default().push(row);
        }

        Ok(groups)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // ---------------------------------------------------------------
    // Test helpers
    // ---------------------------------------------------------------

    /// Default schema reused across most tests: a `Text` column + an `Integer` column.
    fn text_int_columns() -> Vec<ColumnType> {
        vec![ColumnType::Text, ColumnType::Integer]
    }

    /// Build a `Row` from a list of `CellValue`s without the repetitive struct literal.
    fn make_row(cells: Vec<CellValue>) -> Row {
        Row { cells }
    }

    /// Shortcut for `CellValue::Text("...".to_string())`.
    fn txt(s: &str) -> CellValue {
        CellValue::Text(s.to_string())
    }

    /// Build a people dataset covering all three `CellValue` variants,
    /// so tests also exercise the `Float` branch.
    fn people_dataset() -> Dataset {
        Dataset {
            headers: vec!["name".into(), "age".into(), "score".into()],
            column_types: vec![ColumnType::Text, ColumnType::Integer, ColumnType::Float],
            rows: vec![
                make_row(vec![txt("Alice"), CellValue::Integer(28), CellValue::Float(9.5)]),
                make_row(vec![txt("Bob"), CellValue::Integer(35), CellValue::Float(7.0)]),
                make_row(vec![txt("Charlie"), CellValue::Integer(22), CellValue::Float(6.25)]),
            ],
        }
    }

    /// Build a cities dataset with duplicate keys for `group_by` tests.
    fn cities_dataset() -> Dataset {
        Dataset {
            headers: vec!["city".into(), "population".into()],
            column_types: text_int_columns(),
            rows: vec![
                make_row(vec![txt("Rome"), CellValue::Integer(2_800_000)]),
                make_row(vec![txt("Milan"), CellValue::Integer(1_400_000)]),
                make_row(vec![txt("Rome"), CellValue::Integer(2_900_000)]),
            ],
        }
    }

    /// Run `f` with a unique temp file path and always clean it up afterwards.
    fn with_tempfile<F: FnOnce(&str)>(name: &str, f: F) {
        let path = format!("test_output_{name}.json");
        f(&path);
        let _ = fs::remove_file(&path);
    }

    // ---------------------------------------------------------------
    // 0. Serde round-trip
    // ---------------------------------------------------------------

    #[test]
    fn json_roundtrip_preserves_all_cell_variants() {
        let original = people_dataset();
        let json = serde_json::to_string(&original).unwrap();
        let reconstructed: Dataset = serde_json::from_str(&json).unwrap();
        assert_eq!(original, reconstructed);
    }

    // ---------------------------------------------------------------
    // 1. load_json
    // ---------------------------------------------------------------

    #[test]
    fn load_json_parses_valid_payload() {
        let json = r#"{
            "headers": ["name", "age"],
            "column_types": ["Text", "Integer"],
            "rows": [
                ["Alice", 28],
                ["Bob", 35]
            ]
        }"#;

        let parsed = Dataset::load_json(json).expect("valid JSON should parse");

        assert_eq!(parsed.headers, vec!["name", "age"]);
        assert_eq!(parsed.rows.len(), 2);
        assert_eq!(parsed.rows[0].cells[0], txt("Alice"));
        assert_eq!(parsed.rows[0].cells[1], CellValue::Integer(28));
    }

    #[test]
    fn load_json_fails_on_malformed_input() {
        assert!(Dataset::load_json("{ not valid json").is_err());
    }

    // ---------------------------------------------------------------
    // 2. transform
    // ---------------------------------------------------------------

    #[test]
    fn transform_uppercase_mutates_text_column() {
        let mut dataset = people_dataset();
        dataset.transform("name", "uppercase").unwrap();

        let CellValue::Text(first) = &dataset.rows[0].cells[0] else {
            panic!("expected a Text cell");
        };
        assert_eq!(first, "ALICE");
    }

    #[test]
    fn transform_lowercase_mutates_text_column() {
        let mut dataset = people_dataset();
        dataset.transform("name", "lowercase").unwrap();

        assert_eq!(dataset.rows[1].cells[0], txt("bob"));
    }

    #[test]
    fn transform_fails_on_unknown_operation() {
        let mut dataset = people_dataset();
        assert!(dataset.transform("name", "reverse").is_err());
    }

    #[test]
    fn transform_fails_on_missing_column() {
        let mut dataset = people_dataset();
        assert!(dataset.transform("does_not_exist", "uppercase").is_err());
    }

    // ---------------------------------------------------------------
    // 3. export_json
    // ---------------------------------------------------------------

    #[test]
    fn export_json_writes_file_that_can_be_loaded_back() {
        let dataset = people_dataset();

        with_tempfile("export", |path| {
            dataset.export_json(path).unwrap();

            let content = fs::read_to_string(path).unwrap();
            let loaded = Dataset::load_json(&content).unwrap();
            assert_eq!(dataset, loaded);
        });
    }

    // ---------------------------------------------------------------
    // 4. group_by
    // ---------------------------------------------------------------

        #[test]
        fn group_by_text_column_collects_duplicates() -> anyhow::Result<()> {
            let dataset = cities_dataset();
            let groups = dataset.group_by("city")?;

            assert_eq!(groups.len(), 2, "expected two distinct cities");

            let rome = groups.get("Rome").expect("Rome group missing");
            assert_eq!(rome.len(), 2);
            assert_eq!(rome[0].cells[1], CellValue::Integer(2_800_000));
            assert_eq!(rome[1].cells[1], CellValue::Integer(2_900_000));

            let milan = groups.get("Milan").expect("Milan group missing");
            assert_eq!(milan.len(), 1);
            assert_eq!(milan[0].cells[1], CellValue::Integer(1_400_000));

            Ok(())
        }

    #[test]
    fn group_by_integer_column_uses_stringified_keys() {
        let dataset = Dataset {
            headers: vec!["age".into(), "name".into()],
            column_types: vec![ColumnType::Integer, ColumnType::Text],
            rows: vec![
                make_row(vec![CellValue::Integer(30), txt("Alice")]),
                make_row(vec![CellValue::Integer(30), txt("Bob")]),
                make_row(vec![CellValue::Integer(42), txt("Eve")]),
            ],
        };

        let groups = dataset.group_by("age").unwrap();
        assert_eq!(groups.get("30").unwrap().len(), 2);
        assert_eq!(groups.get("42").unwrap().len(), 1);
    }

    #[test]
    fn group_by_float_column_uses_stringified_keys() {
        let dataset = Dataset {
            headers: vec!["score".into()],
            column_types: vec![ColumnType::Float],
            rows: vec![
                make_row(vec![CellValue::Float(1.5)]),
                make_row(vec![CellValue::Float(1.5)]),
                make_row(vec![CellValue::Float(2.0)]),
            ],
        };

        let groups = dataset.group_by("score").unwrap();
        assert_eq!(groups.get("1.5").unwrap().len(), 2);
        assert_eq!(groups.get("2").unwrap().len(), 1);
    }

    #[test]
    fn group_by_fails_on_missing_column() {
        assert!(cities_dataset().group_by("nope").is_err());
    }
}
