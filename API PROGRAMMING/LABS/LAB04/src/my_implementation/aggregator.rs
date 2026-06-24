pub trait Aggregator {
    /// Update the internal state of the aggregator with a new value
    fn update(&mut self, value: f64);
    /// Return the current result of the aggregation
    fn result(&self) -> String;
    /// Return the name of the aggregation mode (e.g., "sum", "avg", "min", "max")
    fn mode_name(&self) -> &'static str;
}

fn format_f64(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }

    if value.fract() == 0.0 {
        format!("{value:.1}")
    }
    else {
        value.to_string()
    }
}

pub struct Count {
    count: usize,
}

impl Count {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Aggregator for Count {
    fn update(&mut self, _value: f64) {
        self.count += 1;
    }

    fn result(&self) -> String {
        self.count.to_string()
    }

    fn mode_name(&self) -> &'static str {
        "count"
    }
}

pub struct Sum {
    total: f64,
    seen_any: bool,
}

impl Sum {
    pub fn new() -> Self {
        Self {
            total: 0.0,
            seen_any: false,
        }
    }
}

impl Aggregator for Sum {
    fn update(&mut self, value: f64) {
        self.total += value;
        self.seen_any = true;
    }

    fn result(&self) -> String {
        if self.seen_any {
            format_f64(self.total)
        } else {
            "0.0".to_string()
        }
    }

    fn mode_name(&self) -> &'static str {
        "sum"
    }
}

pub struct Average {
    total: f64,
    count: usize,
}

impl Average {
    pub fn new() -> Self {
        Self {
            total: 0.0,
            count: 0,
        }
    }
}

impl Aggregator for Average {
    fn update(&mut self, value: f64) {
        self.total += value;
        self.count += 1;
    }

    fn result(&self) -> String {
        if self.count == 0 {
            "NaN".to_string()
        } else {
            format_f64(self.total / self.count as f64)
        }
    }

    fn mode_name(&self) -> &'static str {
        "avg"
    }
}

pub struct Min {
    min_value: Option<f64>,
}

impl Min {
    pub fn new() -> Self {
        Self { min_value: None }
    }
}

impl Aggregator for Min {
    fn update(&mut self, value: f64) {
        self.min_value = Some(match self.min_value {
            Some(current) if current < value => current,
            _ => value,
        });
    }

    fn result(&self) -> String {
        match self.min_value {
            Some(value) => format_f64(value),
            None => "NaN".to_string(),
        }
    }

    fn mode_name(&self) -> &'static str {
        "min"
    }
}

pub struct Max {
    max_value: Option<f64>,
}

impl Max {
    pub fn new() -> Self {
        Self { max_value: None }
    }
}

impl Aggregator for Max {
    fn update(&mut self, value: f64) {
        self.max_value = Some(match self.max_value {
            Some(current) if current > value => current,
            _ => value,
        });
    }

    fn result(&self) -> String {
        match self.max_value {
            Some(value) => format_f64(value),
            None => "NaN".to_string(),
        }
    }

    fn mode_name(&self) -> &'static str {
        "max"
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregator, Average, Count, Max, Min, Sum};

    #[test]
    fn count_returns_zero_when_empty_and_integer_when_updated() {
        // Create a new Count aggregator
        let mut aggregator = Count::new();
        // Check that the result is "0" and the mode name is "count"
        assert_eq!(aggregator.result(), "0");
        assert_eq!(aggregator.mode_name(), "count");

        // Update the aggregator with values 3.5, 11.0, and 7.75
        aggregator.update(10.0);
        aggregator.update(2.5);
        // Check that the result is "2"
        assert_eq!(aggregator.result(), "2");
    }


    // Function to test the Sum aggregator
    #[test]
    fn sum_formats_empty_integer_and_decimal_results() {
        // Create a new Sum aggregator
        let mut empty = Sum::new();

        // Test that the result is "0.0" and the mode name is "sum"
        assert_eq!(empty.result(), "0.0");
        assert_eq!(empty.mode_name(), "sum");

        // Update the aggregator with values 3.5, 11.0, and 7.75
        let mut whole = Sum::new();
        whole.update(40.0);
        whole.update(60.0);
        // Test that the result is "100.0"
        assert_eq!(whole.result(), "100.0");

        // Test that the result is "10.5" when there are no values
        let mut decimal = Sum::new();
        decimal.update(10.5);
        decimal.update(21.25);
        // Test that the result is "31.75"
        assert_eq!(decimal.result(), "31.75");
    }


    // Function to test the Average aggregator
    #[test]
    fn average_returns_nan_when_empty_and_formats_values_correctly() {
        // Create a new Average aggregator
        let mut empty = Average::new();

        // Test that the result is "NaN" and the mode name is "avg"
        assert_eq!(empty.result(), "NaN");
        assert_eq!(empty.mode_name(), "avg");

        // Update the aggregator with values 3.5, 11.0, and 7.75
        let mut whole = Average::new();
        whole.update(20.0);
        whole.update(30.0);
        // Test that the result is "15.0"
        assert_eq!(whole.result(), "25.0");

        // Test that the result is "NaN" when there are no values
        let mut decimal = Average::new();
        decimal.update(6.5);
        decimal.update(11.0);
        assert_eq!(decimal.result(), "8.75");
    }


    // Function to test the Min aggregator
    #[test]
    fn min_returns_nan_when_empty_and_tracks_lowest_value() {
        // Create a new Min aggregator
        let mut aggregator = Min::new();
        // Test that the result is "NaN" and the mode name is "min"
        assert_eq!(aggregator.result(), "NaN");
        assert_eq!(aggregator.mode_name(), "min");

        // Update the aggregator with values 3.5, 11.0, and 7.75
        aggregator.update(30.0);
        aggregator.update(10.0);
        aggregator.update(20.5);

        // Test that the result is "10.0"
        assert_eq!(aggregator.result(), "10.0");
    }


    // Function to test the Max aggregator
    #[test]
    fn max_returns_nan_when_empty_and_tracks_highest_value() {
        // Create a new Max aggregator
        let mut aggregator = Max::new();

        // Check that the result is "NaN" and the mode name is "max"
        assert_eq!(aggregator.result(), "NaN");
        assert_eq!(aggregator.mode_name(), "max");

        // Update the aggregator with values 3.5, 11.0, and 7.75
        aggregator.update(3.5);
        aggregator.update(11.0);
        aggregator.update(7.75);
        // Check that the result is "11.0"
        assert_eq!(aggregator.result(), "11.0");
    }
}
