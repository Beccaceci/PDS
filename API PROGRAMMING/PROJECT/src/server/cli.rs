use crate::TimeWindow;
use crate::terminal::{CliTerminal, Terminal, TerminalEvent};
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;

use super::logger::{AUTO_SHOW_CHART, CpuTracker};
use super::state::ServerState;

/// Fleet administrator CLI, including the interactive terminal it controls.
struct AdminCli<T = Terminal> {
    terminal: T,
}

#[derive(Debug, PartialEq, Eq)]
enum AdminCommand {
    Exit,
    Cpu,
    Chart(Option<bool>),
    Broadcast(String),
    Direct {
        recipient: Option<String>,
        message: Option<String>,
    },
    Stats {
        driver: Option<String>,
        time_window: TimeWindow,
    },
    List,
    Help,
    Unknown(String),
}

impl AdminCli {
    fn new() -> io::Result<Self> {
        let terminal = Terminal::new("[SHELL] > ")?;
        Ok(Self::with_terminal(terminal))
    }
}

impl<T> AdminCli<T>
where
    T: CliTerminal,
{
    fn with_terminal(terminal: T) -> Self {
        terminal.write_plain(
            "[SHELL] Fleet Admin CLI Active. Type 'help' for available commands.\r\n[SHELL] > ",
        );
        Self { terminal }
    }

    /// Runs the interactive administrator console until an exit request or terminal error.
    async fn run(&mut self, state: ServerState, cpu_tracker: Arc<Mutex<CpuTracker>>) {
        let mut driver_msg_rx = state.driver_message_sender.subscribe();

        loop {
            enum AdminEvent {
                Driver(Result<(String, String), tokio::sync::broadcast::error::RecvError>),
                Terminal(io::Result<TerminalEvent>),
            }

            let event = tokio::select! {
                message = driver_msg_rx.recv() => AdminEvent::Driver(message),
                input = self.terminal.read() => AdminEvent::Terminal(input),
            };

            match event {
                AdminEvent::Driver(Ok((sender, content))) => {
                    self.terminal
                        .write(&format!("[DRIVER MESSAGE] From '{}': {}", sender, content));
                }

                AdminEvent::Driver(Err(tokio::sync::broadcast::error::RecvError::Lagged(
                    skipped,
                ))) => {
                    self.terminal.write(&format!(
                        "[WARN] Dropped {skipped} messages while the console was busy."
                    ));
                }

                AdminEvent::Driver(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,

                AdminEvent::Terminal(Ok(TerminalEvent::Line(line))) => {
                    if line.trim().is_empty() {
                        self.terminal.show_prompt();
                        continue;
                    }

                    let should_exit =
                        execute_admin_command(Self::parse_line(&line), &state, &cpu_tracker).await;
                    if should_exit {
                        break;
                    }
                    self.terminal.show_prompt();
                }
                AdminEvent::Terminal(Ok(TerminalEvent::Exit)) => {
                    self.terminal
                        .write_plain("\r\n[SHELL] Exiting Fleet Admin console...\r\n");
                    break;
                }
                AdminEvent::Terminal(Err(_)) => break,
            }
        }
    }

    fn parse_line(line: &str) -> AdminCommand {
        let trimmed = line.trim();
        let command_end = trimmed.find(char::is_whitespace).unwrap_or(trimmed.len());
        let command_text = &trimmed[..command_end];
        let rest = trimmed[command_end..].trim();

        match command_text.to_lowercase().as_str() {
            "exit" | "quit" => AdminCommand::Exit,
            "cpu" => AdminCommand::Cpu,
            "chart" => match rest
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_lowercase()
                .as_str()
            {
                "on" => AdminCommand::Chart(Some(true)),
                "off" => AdminCommand::Chart(Some(false)),
                _ => AdminCommand::Chart(None),
            },
            "broadcast" => AdminCommand::Broadcast(rest.to_string()),
            "direct" => {
                let (recipient, message) = match rest.split_once(char::is_whitespace) {
                    Some((recipient, message)) => (Some(recipient), Some(message.trim())),
                    None if rest.is_empty() => (None, None),
                    None => (Some(rest), None),
                };
                let recipient = recipient
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                let message = message.filter(|value| !value.is_empty()).map(str::to_owned);
                AdminCommand::Direct { recipient, message }
            }
            "stats" => {
                let mut parts = rest.split_whitespace();
                let driver = parts.next().map(str::to_owned);
                let time_window = match parts.next().unwrap_or("day").to_lowercase().as_str() {
                    "week" => TimeWindow::CurrentWeek,
                    "month" => TimeWindow::CurrentMonth,
                    _ => TimeWindow::CurrentDay,
                };
                AdminCommand::Stats {
                    driver,
                    time_window,
                }
            }
            "list" => AdminCommand::List,
            "help" => AdminCommand::Help,
            other => AdminCommand::Unknown(other.to_string()),
        }
    }
}

/// Runs the administrator CLI with an injected terminal implementation.
pub async fn start_admin_cli_with_terminal<T>(
    state: ServerState,
    cpu_tracker: Arc<Mutex<CpuTracker>>,
    terminal: T,
) where
    T: CliTerminal,
{
    let mut cli = AdminCli::with_terminal(terminal);
    cli.run(state, cpu_tracker).await;
}

/// Asynchronous background task handling the interactive Fleet Admin CLI shell on stdin.
/// Uses crossterm EventStream to preserve unsubmitted user input across asynchronous driver messages.
pub async fn start_admin_cli(state: ServerState, cpu_tracker: Arc<Mutex<CpuTracker>>) {
    // If running in an interactive TTY, enable raw mode with crossterm EventStream;
    // otherwise fallback to line-buffered stdin reader for headless environments.
    if let Ok(mut cli) = AdminCli::new() {
        cli.run(state, cpu_tracker).await;
    } else {
        // Fallback for non-TTY / pipe environments
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        println!("[SHELL] Fleet Admin CLI Active. Type 'help' for available commands.");

        loop {
            line.clear();
            if let Ok(bytes) = reader.read_line(&mut line).await {
                if bytes == 0 {
                    break;
                }

                if line.trim().is_empty() {
                    continue;
                }

                let should_exit = execute_admin_command(
                    AdminCli::<Terminal>::parse_line(&line),
                    &state,
                    &cpu_tracker,
                )
                .await;
                if should_exit {
                    break;
                }
            }
        }
    }
}

/// Executes a parsed administrative command against ServerState or CpuTracker.
/// Returns true if the user requested to exit the console shell.
async fn execute_admin_command(
    command: AdminCommand,
    state: &ServerState,
    cpu_tracker: &Arc<Mutex<CpuTracker>>,
) -> bool {
    match command {
        AdminCommand::Exit => {
            print!("[SHELL] Exiting Fleet Admin console...\r\n");
            let _ = io::stdout().flush();
            true
        }
        AdminCommand::Cpu => {
            let guard = cpu_tracker.lock().await;
            guard.render_ascii_chart();
            false
        }
        AdminCommand::Chart(Some(true)) => {
            AUTO_SHOW_CHART.store(true, Ordering::Relaxed);
            print!("[SHELL] Automatic CPU chart printing every 120s: ENABLED.\r\n");
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Chart(Some(false)) => {
            AUTO_SHOW_CHART.store(false, Ordering::Relaxed);
            print!(
                "[SHELL] Automatic CPU chart printing every 120s: DISABLED (silent logging).\r\n"
            );
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Chart(None) => {
            print!("[SHELL] Usage: chart <on|off>\r\n");
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Broadcast(message) => {
            if message.is_empty() {
                print!("[SHELL] Usage: broadcast <message>\r\n");
            } else {
                let count = state.send_admin_broadcast(&message).await;
                print!(
                    "[ADMIN BROADCAST] Message sent to {} active drivers.\r\n",
                    count
                );
            }
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Direct { recipient, message } => {
            let (Some(recipient), Some(message)) = (recipient, message) else {
                print!("[SHELL] Usage: direct <driver> <message>\r\n");
                let _ = io::stdout().flush();
                return false;
            };

            match state.send_admin_direct(&recipient, &message).await {
                Ok(()) => print!("[ADMIN DIRECT] Message sent to driver '{}'.\r\n", recipient),
                Err(err) => print!("[ADMIN DIRECT ERROR] {}\r\n", err),
            }
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Stats {
            driver,
            time_window,
        } => {
            let Some(driver) = driver else {
                print!("[SHELL] Usage: stats <driver> [day|week|month]\r\n");
                let _ = io::stdout().flush();
                return false;
            };

            match state.calculate_user_stats(&driver, time_window).await {
                Ok(stats) => {
                    print!(
                        "\r\n======================= MOVEMENT STATS FOR '{}' [{:?}] =======================\r\n",
                        driver, stats.time_window
                    );
                    print!(
                        "  Total Distance:           {:.2} km\r\n",
                        stats.total_distance_km
                    );
                    print!(
                        "  Average Speed:            {:.2} km/h\r\n",
                        stats.average_speed_kmh
                    );
                    print!(
                        "  Movement Duration:        {} secs ({} mins)\r\n",
                        stats.movement_duration_secs,
                        stats.movement_duration_secs / 60
                    );
                    print!(
                        "  Pause Duration (Stopped): {} secs ({} mins)\r\n",
                        stats.pause_duration_secs,
                        stats.pause_duration_secs / 60
                    );
                    print!(
                        "================================================================================\r\n\r\n"
                    );
                }
                Err(err) => print!("[ADMIN STATS ERROR] {}\r\n", err),
            }
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::List => {
            let drivers = state.get_driver_list().await;
            print!(
                "\r\n======================= REGISTERED FLEET DRIVERS ({}) =======================\r\n",
                drivers.len()
            );
            if drivers.is_empty() {
                print!("  No drivers currently registered.\r\n");
            } else {
                for (username, state_enum, pos_count) in drivers {
                    print!(
                        "  Driver: {:<15} | State: {:<20} | Positions: {}\r\n",
                        username, format!("{:?}", state_enum), pos_count
                    );
                }
            }
            print!(
                "================================================================================\r\n\r\n"
            );
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Help => {
            print!("\r\n=== FLEET ADMIN CONSOLE COMMANDS ===\r\n");
            print!(
                "  list                    - List all registered drivers, current states, and position counts\r\n"
            );
            print!(
                "  broadcast <msg>         - Broadcast text message to all connected truck drivers\r\n"
            );
            print!(
                "  direct <driver> <msg>   - Send direct text message to a specific connected driver\r\n"
            );
            print!(
                "  stats <driver> [window] - Query movement analytics for driver (day|week|month)\r\n"
            );
            print!("  cpu                     - Display CPU histogram of recent time windows\r\n");
            print!(
                "  chart <on|off>          - Enable or disable automatic 120s CPU chart printing\r\n"
            );
            print!("  exit / quit             - Exit Fleet Admin console and shut down server\r\n");
            print!("  help                    - Display this help menu\r\n\r\n");
            let _ = io::stdout().flush();
            false
        }
        AdminCommand::Unknown(other) => {
            print!(
                "[SHELL] Unknown command: '{}'. Type 'help' for available commands.\r\n",
                other
            );
            let _ = io::stdout().flush();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::tests::{VALID_PASSWORD, state_with_users};
    use super::*;
    use crate::ServerMessage;
    use std::collections::{BTreeSet, VecDeque};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::OnceLock;
    use tokio::time::{Duration, timeout};

    struct ScriptedTerminal {
        events: VecDeque<TerminalEvent>,
        output: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl CliTerminal for ScriptedTerminal {
        fn show_prompt(&self) {
            self.output.lock().unwrap().push("prompt".into());
        }

        fn write(&self, message: &str) {
            self.output.lock().unwrap().push(message.into());
        }

        fn write_plain(&self, message: &str) {
            self.output.lock().unwrap().push(message.into());
        }

        fn read(&mut self) -> Pin<Box<dyn Future<Output = io::Result<TerminalEvent>> + '_>> {
            Box::pin(async move {
                self.events.pop_front().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "script is exhausted")
                })
            })
        }
    }

    fn command(line: &str) -> AdminCommand {
        AdminCli::<Terminal>::parse_line(line)
    }

    #[tokio::test]
    async fn injected_terminal_drives_admin_commands() {
        let (state, _db) = state_with_users(&["driver17"]);
        let mut receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let terminal = ScriptedTerminal {
            events: VecDeque::from([
                TerminalEvent::Line("broadcast injected alert".into()),
                TerminalEvent::Line("exit".into()),
            ]),
            output: output.clone(),
        };

        start_admin_cli_with_terminal(state, cpu_tracker, terminal).await;

        assert_eq!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .expect("broadcast must arrive"),
            Some(ServerMessage::TextMessage {
                sender: "FleetAdmin".into(),
                content: "injected alert".into(),
            })
        );
        assert!(
            output
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains("Fleet Admin CLI Active"))
        );
    }

    struct ChartSettingRestore(bool);

    impl Drop for ChartSettingRestore {
        fn drop(&mut self) {
            AUTO_SHOW_CHART.store(self.0, Ordering::SeqCst);
        }
    }

    #[test]
    fn parse_chart() {
        assert_eq!(
            command(" \tCHART   ON \r\n"),
            AdminCommand::Chart(Some(true))
        );
        assert_eq!(command("chart OFF"), AdminCommand::Chart(Some(false)));
        assert_eq!(command("chart enabled"), AdminCommand::Chart(None));
    }

    #[test]
    fn parse_simple_commands() {
        assert_eq!(command("exit"), AdminCommand::Exit);
        assert_eq!(command("QUIT"), AdminCommand::Exit);
        assert_eq!(command("cpu"), AdminCommand::Cpu);
        assert_eq!(command("list"), AdminCommand::List);
        assert_eq!(command("help"), AdminCommand::Help);
    }

    #[test]
    fn parse_broadcast() {
        assert_eq!(
            command("broadcast   Route 66 is  clear 🚚   "),
            AdminCommand::Broadcast("Route 66 is  clear 🚚".into())
        );
    }

    #[test]
    fn parse_direct() {
        assert_eq!(
            command("direct  driver1   Please call dispatch"),
            AdminCommand::Direct {
                recipient: Some("driver1".into()),
                message: Some("Please call dispatch".into()),
            }
        );
        assert_eq!(
            command("direct driver1"),
            AdminCommand::Direct {
                recipient: Some("driver1".into()),
                message: None,
            }
        );
        assert_eq!(
            command("direct    "),
            AdminCommand::Direct {
                recipient: None,
                message: None,
            }
        );
    }

    #[test]
    fn parse_stats() {
        assert_eq!(
            command("stats driver1 week"),
            AdminCommand::Stats {
                driver: Some("driver1".into()),
                time_window: TimeWindow::CurrentWeek,
            }
        );
        assert_eq!(
            command("stats driver1 MONTH"),
            AdminCommand::Stats {
                driver: Some("driver1".into()),
                time_window: TimeWindow::CurrentMonth,
            }
        );
        assert_eq!(
            command("stats driver1"),
            AdminCommand::Stats {
                driver: Some("driver1".into()),
                time_window: TimeWindow::CurrentDay,
            }
        );
        assert_eq!(
            command("stats driver1 year"),
            AdminCommand::Stats {
                driver: Some("driver1".into()),
                time_window: TimeWindow::CurrentDay,
            }
        );
        assert_eq!(
            command("stats"),
            AdminCommand::Stats {
                driver: None,
                time_window: TimeWindow::CurrentDay,
            }
        );
    }

    #[test]
    fn parse_empty() {
        assert_eq!(command(""), AdminCommand::Unknown("".into()));
    }

    #[test]
    fn parse_unknown() {
        assert_eq!(command("  WAT  "), AdminCommand::Unknown("wat".into()));
    }

    #[tokio::test]
    async fn exit_exits() {
        let (state, _db) = state_with_users(&[]);
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(execute_admin_command(command("exit"), &state, &cpu_tracker).await);
    }

    #[tokio::test]
    async fn quit_exits() {
        let (state, _db) = state_with_users(&[]);
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(execute_admin_command(command("quit"), &state, &cpu_tracker).await);
        assert!(execute_admin_command(command("EXIT"), &state, &cpu_tracker).await);
        assert!(execute_admin_command(command("QUIT"), &state, &cpu_tracker).await);
        assert!(execute_admin_command(command("  exit  "), &state, &cpu_tracker).await);
    }

    #[tokio::test]
    async fn commands_stay_open() {
        let (state, _db) = state_with_users(&[]);
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(!execute_admin_command(command("help"), &state, &cpu_tracker).await);
        assert!(!execute_admin_command(command("list"), &state, &cpu_tracker).await);
        assert!(!execute_admin_command(command("cpu"), &state, &cpu_tracker).await);
    }

    #[tokio::test]
    async fn unknown_stays_open() {
        let (state, _db) = state_with_users(&[]);
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(!execute_admin_command(command("unknown_cmd"), &state, &cpu_tracker).await);
        assert!(!execute_admin_command(command(""), &state, &cpu_tracker).await);
    }

    #[tokio::test]
    async fn chart_updates_flag() {
        static CHART_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
        let _lock = CHART_TEST_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .expect("chart test lock must not be poisoned");
        let _restore = ChartSettingRestore(AUTO_SHOW_CHART.swap(false, Ordering::SeqCst));
        let (state, _db) = state_with_users(&[]);
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(!execute_admin_command(command("chart on"), &state, &cpu_tracker).await);
        assert!(AUTO_SHOW_CHART.load(Ordering::SeqCst));
        assert!(!execute_admin_command(command("chart OFF"), &state, &cpu_tracker).await);
        assert!(!AUTO_SHOW_CHART.load(Ordering::SeqCst));
        assert!(!execute_admin_command(command("chart invalid"), &state, &cpu_tracker).await);
        assert!(!AUTO_SHOW_CHART.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn direct_is_unicast() {
        let (state, _db) = state_with_users(&["driver17", "driver18"]);
        let mut first_receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let mut second_receiver = state.user_login("driver18", VALID_PASSWORD).await.unwrap();
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(
            !execute_admin_command(
                command("direct driver17 inspection at 14:00"),
                &state,
                &cpu_tracker,
            )
            .await
        );
        assert_eq!(
            timeout(Duration::from_millis(100), first_receiver.recv())
                .await
                .expect("targeted driver must receive a message"),
            Some(ServerMessage::TextMessage {
                sender: "FleetAdmin".into(),
                content: "inspection at 14:00".into(),
            })
        );
        assert!(matches!(
            second_receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn invalid_messages_skip_send() {
        let (state, _db) = state_with_users(&["driver17"]);
        let mut receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));

        assert!(!execute_admin_command(command("broadcast   "), &state, &cpu_tracker).await);
        assert!(!execute_admin_command(command("direct driver17"), &state, &cpu_tracker).await);
        assert!(!execute_admin_command(command("stats"), &state, &cpu_tracker).await);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn broadcasts_reach_all() {
        let (state, _db) = state_with_users(&["driver17", "driver18"]);
        let mut first_receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let mut second_receiver = state.user_login("driver18", VALID_PASSWORD).await.unwrap();
        let first_state = state.clone();
        let second_state = state.clone();
        let first_tracker = Arc::new(Mutex::new(CpuTracker::new(5, "dummy.log")));
        let second_tracker = first_tracker.clone();

        let (first_exits, second_exits) = tokio::join!(
            execute_admin_command(
                command("broadcast first alert"),
                &first_state,
                &first_tracker
            ),
            execute_admin_command(
                command("broadcast second alert"),
                &second_state,
                &second_tracker
            ),
        );
        assert!(!first_exits);
        assert!(!second_exits);

        for receiver in [&mut first_receiver, &mut second_receiver] {
            let mut received = BTreeSet::new();
            for _ in 0..2 {
                let Some(ServerMessage::TextMessage { sender, content }) =
                    timeout(Duration::from_millis(100), receiver.recv())
                        .await
                        .expect("each broadcast must arrive")
                else {
                    panic!("active driver must receive an admin text message");
                };
                assert_eq!(sender, "FleetAdmin");
                received.insert(content);
            }
            assert_eq!(
                received,
                BTreeSet::from(["first alert".to_string(), "second alert".to_string()])
            );
        }
    }
}
