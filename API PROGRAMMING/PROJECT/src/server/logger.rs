use crate::{SystemTimeSource, TimeSource};
use std::collections::VecDeque;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use sysinfo::{Pid, System};

/// Global flag controlling whether the ASCII CPU chart is automatically printed every 120s interval.
pub static AUTO_SHOW_CHART: AtomicBool = AtomicBool::new(false);

/// Encapsulates server CPU usage metrics, history tracking, and ASCII histogram rendering.
pub struct CpuTracker {
    pub history: VecDeque<(u64, f32)>, // Stores (timestamp, cpu_usage_percentage)
    pub max_samples: usize,
    pub sys: System,
    pub pid: Pid,
    pub log_path: PathBuf,
    time_source: Arc<dyn TimeSource>,
}

impl CpuTracker {
    /// Initializes a new CpuTracker instance with sample buffer capacity and log file path.
    pub fn new(max_samples: usize, log_path: impl Into<PathBuf>) -> Self {
        Self::with_time_source(max_samples, log_path, Arc::new(SystemTimeSource))
    }

    /// Initializes a tracker using an injected source for sample timestamps.
    pub fn with_time_source(
        max_samples: usize,
        log_path: impl Into<PathBuf>,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        Self {
            history: VecDeque::with_capacity(max_samples),
            max_samples,
            sys: System::new_all(),
            pid: Pid::from_u32(process::id()),
            log_path: log_path.into(),
            time_source,
        }
    }

    /// Logs the server binary executable size in bytes and megabytes.
    pub fn log_executable_size() {
        if let Ok(exe_path) = env::current_exe() {
            if let Ok(metadata) = fs::metadata(&exe_path) {
                let size_bytes = metadata.len();
                let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
                println!(
                    "[SERVER] Executable binary size: {} bytes ({:.2} MB)",
                    size_bytes, size_mb
                );
            }
        }
    }

    /// Measures current process CPU usage percentage silently, pushes it into the history queue,
    /// and appends the log entry to configured log file (default 'cpu_usage.log').
    pub fn measure_and_push(&mut self) -> f32 {
        self.sys
            .refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), true);
        let cpu_usage = match self.sys.process(self.pid) {
            Some(process) => process.cpu_usage(),
            None => 0.0,
        };

        let timestamp = self.time_source.now_secs();
        let log_entry = format!("[TIMESTAMP: {}] CPU Usage: {:.2}%\n", timestamp, cpu_usage);

        // Append log metric to configured log file, reporting any write errors
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            Ok(mut file) => {
                if let Err(err) = file.write_all(log_entry.as_bytes()) {
                    eprintln!(
                        "[LOGGER ERROR] Failed to write CPU log to {:?}: {}",
                        self.log_path, err
                    );
                }
            }
            Err(err) => {
                eprintln!(
                    "[LOGGER ERROR] Failed to open CPU log file {:?}: {}",
                    self.log_path, err
                );
            }
        }

        // Maintain capacity bounds in history queue (handles max_samples == 0)
        if self.max_samples == 0 {
            self.history.clear();
            return cpu_usage;
        }

        while self.history.len() >= self.max_samples {
            self.history.pop_front();
        }
        self.history.push_back((timestamp, cpu_usage));

        cpu_usage
    }

    /// Renders an ASCII / Unicode histogram bar chart of the recent CPU utilization history to stdout.
    pub fn render_ascii_chart(&self) {
        if self.history.is_empty() {
            print!("[LOGGER] No CPU data recorded yet.\r\n");
            let _ = std::io::stdout().flush();
            return;
        }

        print!(
            "\r\n======================= SERVER CPU UTILIZATION HISTOGRAM =======================\r\n"
        );

        let mut sum_cpu = 0.0;
        let mut max_cpu: f32 = 0.0;

        for (idx, (ts, cpu)) in self.history.iter().enumerate() {
            sum_cpu += cpu;
            if *cpu > max_cpu {
                max_cpu = *cpu;
            }

            // Map CPU percentage (0-100%) to a 20-character wide bar
            let bar_length: usize = 20;
            let filled_blocks = ((*cpu / 100.0) * bar_length as f32)
                .round()
                .clamp(0.0, bar_length as f32) as usize;
            let empty_blocks = bar_length.saturating_sub(filled_blocks);

            let bar = format!("{}{}", "█".repeat(filled_blocks), "░".repeat(empty_blocks));
            let alert_tag = if *cpu >= 80.0 { " [HIGH LOAD]" } else { "" };

            print!(
                "Window #{:<2} [TS: {}]  {}  {:>5.1}%{}\r\n",
                idx + 1,
                ts,
                bar,
                cpu,
                alert_tag
            );
        }

        let avg_cpu = sum_cpu / self.history.len() as f32;
        print!(
            "--------------------------------------------------------------------------------\r\n"
        );
        print!(
            "Average CPU: {:.2}% | Peak CPU: {:.2}% | Samples: {} windows ({} mins)\r\n",
            avg_cpu,
            max_cpu,
            self.history.len(),
            self.history.len() * 2
        );
        print!(
            "================================================================================\r\n\r\n"
        );
        let _ = std::io::stdout().flush();
    }
}

/// Asynchronous background task that measures CPU usage at the configured interval.
/// Logs metrics silently to file, and only prints chart if AUTO_SHOW_CHART flag is enabled.
pub async fn start_cpu_logger(
    tracker: Arc<tokio::sync::Mutex<CpuTracker>>,
    sample_interval: Duration,
) {
    let mut interval = tokio::time::interval(sample_interval);

    loop {
        interval.tick().await;

        let mut guard = tracker.lock().await;
        guard.measure_and_push();

        // Print chart automatically only if AUTO_SHOW_CHART flag is true
        if AUTO_SHOW_CHART.load(Ordering::Relaxed) {
            guard.render_ascii_chart();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    #[derive(Debug)]
    struct FixedTimeSource(AtomicU64);

    impl TimeSource for FixedTimeSource {
        fn now_secs(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn evicts_at_capacity() {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("cpu.log");

        let mut tracker = CpuTracker::new(2, log_file);
        tracker.measure_and_push();
        tracker.measure_and_push();
        tracker.measure_and_push();
        assert_eq!(tracker.history.len(), 2);
    }

    #[test]
    fn zero_capacity_keeps_no_samples() {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("cpu.log");

        let mut zero_capacity_tracker = CpuTracker::new(0, log_file);
        zero_capacity_tracker.measure_and_push();
        assert_eq!(zero_capacity_tracker.history.len(), 0);
    }

    #[test]
    fn injected_time_source_controls_sample_timestamp() {
        let temp_dir = TempDir::new().unwrap();
        let log_file = temp_dir.path().join("cpu.log");
        let time_source = Arc::new(FixedTimeSource(AtomicU64::new(42)));
        let mut tracker = CpuTracker::with_time_source(1, &log_file, time_source);

        tracker.measure_and_push();

        assert_eq!(
            tracker.history.front().map(|(timestamp, _)| *timestamp),
            Some(42)
        );
        assert!(
            std::fs::read_to_string(log_file)
                .unwrap()
                .contains("[TIMESTAMP: 42]")
        );
    }
}
