pub trait Aggregator {
    // update the internal state with a new numerical value
    fn update(&mut self, value: f64);

    // return the result as a string
    fn result(&self) -> String;

    // return mode name
    fn mode_name(&self) -> &'static str;
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
    sum: f64,
}

impl Sum {
    pub fn new() -> Self {
        Self { sum: 0.0 }
    }
}

impl Aggregator for Sum {
    fn update(&mut self, value: f64) {
        self.sum += value;
    }

    fn result(&self) -> String {
        self.sum.to_string()
    }

    fn mode_name(&self) -> &'static str {
        "sum"
    }
}

pub struct Average {
    sum: f64,
    count: usize,
}

impl Average {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Aggregator for Average {
    fn update(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    fn result(&self) -> String {
        if self.count == 0 {
            "NaN".to_string()
        } else {
            (self.sum / self.count as f64).to_string()
        }
    }

    fn mode_name(&self) -> &'static str {
        "avg"
    }
}

pub struct Min {
    min: Option<f64>,
}

impl Min {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Aggregator for Min {
    fn update(&mut self, value: f64) {
        self.min = Some(match self.min {
            Some(current) => current.min(value),
            None => value,
        });
    }

    fn result(&self) -> String {
        match self.min {
            Some(value) => value.to_string(),
            None => "NaN".to_string(),
        }
    }

    fn mode_name(&self) -> &'static str {
        "min"
    }
}

pub struct Max {
    max: Option<f64>,
}

impl Max {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Aggregator for Max {
    fn update(&mut self, value: f64) {
        self.max = Some(match self.max {
            Some(current) => current.max(value),
            None => value,
        });
    }

    fn result(&self) -> String {
        match self.max {
            Some(value) => value.to_string(),
            None => "NaN".to_string(),
        }
    }

    fn mode_name(&self) -> &'static str {
        "max"
    }
}