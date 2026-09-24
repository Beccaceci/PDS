use crate::{Position, SystemTimeSource, TimeSource};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::sync::Arc;

/// Supplies the client's next geographic position.
///
/// Implementations may obtain positions from a GPS device, a file, or another
/// source. `None` indicates that the provider has no position to report.
pub trait PositionProvider {
    fn next_position(&mut self) -> Option<Position>;
}

/// Supplies positions by reading latitude/longitude pairs from a CSV file.
pub struct FilePositionProvider {
    reader: BufReader<File>,
    last_position: Option<Position>,
    time_source: Arc<dyn TimeSource>,
}

impl FilePositionProvider {
    /// Opens the specified CSV file containing route coordinates.
    pub fn new(file_path: &str) -> io::Result<Self> {
        Self::with_time_source(file_path, Arc::new(SystemTimeSource))
    }

    /// Opens a CSV route using the supplied source for position timestamps.
    pub fn with_time_source(file_path: &str, time_source: Arc<dyn TimeSource>) -> io::Result<Self> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            last_position: None,
            time_source,
        })
    }
}

impl PositionProvider for FilePositionProvider {
    /// Reads the next line from the CSV and returns the position with the current timestamp.
    /// If the file ends, it returns the last known position to simulate a stopped vehicle.
    fn next_position(&mut self) -> Option<Position> {
        let mut line = String::new();

        loop {
            line.clear();
            match self.reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line_trimmed = line.trim();
                    if line_trimmed.is_empty() {
                        continue;
                    }

                    let mut parts = line_trimmed.split(',');
                    if let (Some(lat_str), Some(lon_str)) = (parts.next(), parts.next()) {
                        if let (Ok(lat), Ok(lon)) =
                            (lat_str.trim().parse::<f64>(), lon_str.trim().parse::<f64>())
                        {
                            let pos = Position {
                                latitude: lat,
                                longitude: lon,
                                timestamp: self.time_source.now_secs(),
                            };
                            self.last_position = Some(pos);
                            return Some(pos);
                        }
                    }
                }
            }
        }

        if let Some(mut last_pos) = self.last_position {
            last_pos.timestamp = self.time_source.now_secs();
            return Some(last_pos);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::{FilePositionProvider, PositionProvider};
    use crate::TimeSource;
    use std::io::{self, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::{NamedTempFile, TempDir};

    #[derive(Debug)]
    struct FixedTimeSource {
        now: AtomicU64,
    }

    impl FixedTimeSource {
        fn new(now: u64) -> Self {
            Self {
                now: AtomicU64::new(now),
            }
        }

        fn set(&self, now: u64) {
            self.now.store(now, Ordering::Relaxed);
        }
    }

    impl TimeSource for FixedTimeSource {
        fn now_secs(&self) -> u64 {
            self.now.load(Ordering::Relaxed)
        }
    }

    fn provider_for(contents: &str) -> (NamedTempFile, FilePositionProvider) {
        let mut route_file = NamedTempFile::new().unwrap();
        write!(route_file, "{contents}").unwrap();
        route_file.flush().unwrap();

        let provider = FilePositionProvider::new(route_file.path().to_str().unwrap()).unwrap();
        (route_file, provider)
    }

    #[test]
    fn empty_file_has_no_position() {
        let (_route_file, mut provider) = provider_for("");
        assert_eq!(provider.next_position(), None);
    }

    #[test]
    fn file_without_a_valid_coordinate_has_no_position() {
        let (_route_file, mut provider) = provider_for("\nnot,a,position\n45.0\n");
        assert_eq!(provider.next_position(), None);
    }

    #[test]
    fn parses_float_coordinates() {
        let (_route_file, mut provider) =
            provider_for("45.0703,7.6869\n0.0,0.0\n-45.0703,-7.6869\n");

        let positive = provider.next_position().unwrap();
        assert_eq!((positive.latitude, positive.longitude), (45.0703, 7.6869));

        let zero = provider.next_position().unwrap();
        assert_eq!((zero.latitude, zero.longitude), (0.0, 0.0));

        let negative = provider.next_position().unwrap();
        assert_eq!((negative.latitude, negative.longitude), (-45.0703, -7.6869));
    }

    #[test]
    fn skips_invalid_rows() {
        let (_route_file, mut provider) = provider_for("not,a,position\n\n45.0703,7.6869\n");
        let first = provider.next_position().unwrap();
        assert_eq!(first.latitude, 45.0703);
        assert_eq!(first.longitude, 7.6869);
    }

    #[test]
    fn repeats_the_last_position_after_reaching_end_of_file() {
        let mut route_file = NamedTempFile::new().unwrap();
        writeln!(route_file, "45.0703,7.6869").unwrap();
        route_file.flush().unwrap();
        let time_source = Arc::new(FixedTimeSource::new(1_000));
        let mut provider = FilePositionProvider::with_time_source(
            route_file.path().to_str().unwrap(),
            time_source.clone(),
        )
        .unwrap();

        let first = provider.next_position().unwrap();
        assert_eq!(first.timestamp, 1_000);

        time_source.set(1_000 + crate::STOPPED_AFTER_SECS);
        let repeated = provider.next_position().unwrap();
        assert_eq!(repeated.timestamp, 1_000 + crate::STOPPED_AFTER_SECS);
        assert_eq!(repeated.latitude, first.latitude);
        assert_eq!(repeated.longitude, first.longitude);
    }

    #[test]
    fn missing_file_returns_not_found() {
        let directory = TempDir::new().unwrap();
        let missing_file = directory.path().join("missing.csv");

        let error = match FilePositionProvider::new(missing_file.to_str().unwrap()) {
            Ok(_) => panic!("opening a missing route file must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
