use std::time::{SystemTime, UNIX_EPOCH};

/// Supplies Unix timestamps in seconds to components that need wall-clock time.
///
/// Production code uses [`SystemTimeSource`]. Tests can provide a deterministic
/// implementation without sleeping or changing the process clock.
pub trait TimeSource: Send + Sync {
    fn now_secs(&self) -> u64;
}

/// Wall-clock time source backed by the operating system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System clock is set before UNIX Epoch")
            .as_secs()
    }
}

/// Returns the current Unix timestamp using the production wall clock.
pub fn current_timestamp_secs() -> u64 {
    SystemTimeSource.now_secs()
}
