use super::network::{ServerReader, send_message};
use crate::terminal::{CliTerminal, Terminal, TerminalEvent};
use crate::{ClientMessage, ServerMessage};
use std::io::Write;
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};

const PROMPT_PREFIX: &str = "> ";

/// Terminal-facing vehicle CLI that parses completed driver input.
pub struct ClientCli<T = Terminal> {
    terminal: T,
}

/// Domain-level input emitted by the vehicle CLI.
pub enum ClientCliEvent {
    Message(ClientMessage),
    Exit,
}

impl ClientCli {
    /// Creates the interactive vehicle CLI and shows its chat prompt.
    pub fn new() -> io::Result<Self> {
        let terminal = Terminal::new(PROMPT_PREFIX)?;
        Ok(Self::with_terminal(terminal))
    }
}

impl<T> ClientCli<T>
where
    T: CliTerminal,
{
    /// Creates a vehicle CLI backed by an injected terminal implementation.
    pub fn with_terminal(terminal: T) -> Self {
        terminal.show_prompt();
        Self { terminal }
    }

    /// Waits for a completed non-empty driver message or an exit request.
    pub async fn read(&mut self) -> io::Result<ClientCliEvent> {
        loop {
            match self.terminal.read().await? {
                TerminalEvent::Line(line) => {
                    self.terminal.show_prompt();
                    if let Some(message) = parse_line(line) {
                        return Ok(ClientCliEvent::Message(message));
                    }
                }
                TerminalEvent::Exit => return Ok(ClientCliEvent::Exit),
            }
        }
    }

    /// Displays a server, vehicle, or client-status message in the driver console.
    pub fn write(&self, message: &str) {
        self.terminal.write(message);
    }

    /// Prints the vehicle-specific exit message.
    pub fn exit(&self) {
        self.terminal
            .write_plain("\r\n[CLIENT] Exiting Vehicle Terminal...\r\n");
    }
}

impl ClientCli {
    /// Converts a non-empty submitted line into a driver chat message.
    pub fn parse_line(line: String) -> Option<ClientMessage> {
        parse_line(line)
    }

    /// Handles the interactive login or registration flow using standard input.
    pub async fn authenticate<R, W>(
        server_reader: &mut ServerReader<R>,
        server_writer: &mut W,
    ) -> Option<String>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut stdin = BufReader::new(io::stdin());
        Self::authenticate_with_input(&mut stdin, server_reader, server_writer).await
    }

    /// Handles authentication with an injected input stream.
    ///
    /// This has the same interactive behavior as [`Self::authenticate`] while
    /// allowing tests to supply scripted input and in-memory streams.
    pub async fn authenticate_with_input<I, R, W>(
        stdin: &mut I,
        server_reader: &mut ServerReader<R>,
        server_writer: &mut W,
    ) -> Option<String>
    where
        I: AsyncBufRead + Unpin,
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        loop {
            println!("========================================");
            println!("1. Login (Access existing account)");
            println!("2. Register (Create new account)");
            println!("========================================");

            let choice = match prompt_input(stdin, "> Choose an option (1 or 2): ").await {
                Some(c) => c,
                None => continue,
            };

            if choice != "1" && choice != "2" {
                println!("\n[ERROR] Invalid choice. Please enter 1 or 2.\n");
                continue;
            }

            let username = match prompt_input(stdin, "> Username: ").await {
                Some(u) if !u.is_empty() => u,
                _ => continue,
            };

            let password = match prompt_input(stdin, "> Password: ").await {
                Some(p) if !p.is_empty() => p,
                _ => continue,
            };

            let msg = if choice == "1" {
                ClientMessage::Login {
                    username: username.clone(),
                    password,
                }
            } else {
                ClientMessage::Register {
                    username: username.clone(),
                    password,
                }
            };

            if send_message(server_writer, &msg).await.is_err() {
                println!("\n[CRITICAL] Failed to send data to server.");
                return None;
            }

            match server_reader.read_message().await {
                Ok(None) | Err(_) => {
                    println!("\n[CRITICAL] Connection lost with server.");
                    return None;
                }
                Ok(Some(server_msg)) => match server_msg {
                    ServerMessage::AuthResult(Ok(())) => {
                        println!("\n[SUCCESS] Authentication successful!");
                        return Some(username);
                    }
                    ServerMessage::AuthResult(Err(e)) => {
                        println!("\n[DENIED] Authentication error: {}\n", e);
                    }
                    ServerMessage::ErrorMessage(e) => {
                        println!("\n[SERVER ERROR] {}\n", e);
                    }
                    _ => {
                        println!("\n[ERROR] Unexpected response from server.\n");
                    }
                },
            }
        }
    }
}

fn parse_line(line: String) -> Option<ClientMessage> {
    let content = line.trim();
    (!content.is_empty()).then(|| ClientMessage::SendText {
        content: content.to_string(),
    })
}

/// Prints a prompt and reads one trimmed line from an asynchronous input stream.
pub async fn prompt_input<R: AsyncBufRead + Unpin>(
    stdin: &mut R,
    prompt_text: &str,
) -> Option<String> {
    print!("{}", prompt_text);
    std::io::stdout().flush().unwrap();

    let mut input = String::new();
    match stdin.read_line(&mut input).await {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(input.trim().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    struct ScriptedTerminal {
        events: VecDeque<TerminalEvent>,
        output: Arc<Mutex<Vec<String>>>,
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

    /// Runs the client against scripted terminal input and server replies.
    ///
    /// Returns the authentication result together with every request written
    /// by the client, so callers can assert both sides of the exchange.
    async fn run_client(
        input_data: &[u8],
        responses: &[ServerMessage],
    ) -> (Option<String>, Vec<ClientMessage>) {
        // The client reads prompts from this in-memory terminal stream.
        let (mut input_sender, input_stream) = tokio::io::duplex(4096);
        input_sender.write_all(input_data).await.unwrap();
        drop(input_sender);
        let mut input = BufReader::new(input_stream);

        // Preload the messages the mock server will return to the client.
        let (mut response_sender, response_stream) = tokio::io::duplex(4096);
        for response in responses {
            let response = serde_json::to_string(response).unwrap();
            response_sender
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
        drop(response_sender);
        let mut server_reader = ServerReader::new(response_stream);

        // Capture and decode the newline-delimited requests sent by the client.
        let (mut request_sender, request_stream) = tokio::io::duplex(4096);
        let username =
            ClientCli::authenticate_with_input(&mut input, &mut server_reader, &mut request_sender)
                .await;
        drop(request_sender);

        let mut requests = Vec::new();
        let mut lines = BufReader::new(request_stream).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            requests.push(serde_json::from_str(&line).unwrap());
        }

        (username, requests)
    }

    async fn assert_login_after_skipped_input(input: &[u8]) {
        let expected = ClientMessage::Login {
            username: "driver17".into(),
            password: "Pass1234".into(),
        };
        let (username, requests) = run_client(input, &[ServerMessage::AuthResult(Ok(()))]).await;

        assert_eq!(username, Some("driver17".into()));
        assert_eq!(requests, vec![expected]);
    }

    async fn assert_login_after_retry(reply: ServerMessage) {
        let input = b"1\ndriver17\nPass1234\n1\ndriver17\nPass1234\n";
        let (username, requests) =
            run_client(input, &[reply, ServerMessage::AuthResult(Ok(()))]).await;

        assert_eq!(username, Some("driver17".into()));
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|message| {
            matches!(message, ClientMessage::Login { username, password }
                if username == "driver17" && password == "Pass1234")
        }));
    }

    #[tokio::test]
    async fn injected_terminal_drives_and_captures_the_cli() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let terminal = ScriptedTerminal {
            events: VecDeque::from([
                TerminalEvent::Line("   ".into()),
                TerminalEvent::Line("traffic ahead".into()),
                TerminalEvent::Exit,
            ]),
            output: output.clone(),
        };
        let mut cli = ClientCli::with_terminal(terminal);

        assert!(matches!(
            cli.read().await.unwrap(),
            ClientCliEvent::Message(ClientMessage::SendText { content })
                if content == "traffic ahead"
        ));
        cli.write("status message");
        cli.exit();
        assert!(matches!(cli.read().await.unwrap(), ClientCliEvent::Exit));

        let output = output.lock().unwrap();
        assert!(output.iter().any(|line| line == "status message"));
        assert!(
            output
                .iter()
                .any(|line| line.contains("Exiting Vehicle Terminal"))
        );
        assert_eq!(output.iter().filter(|line| *line == "prompt").count(), 3);
    }

    #[test]
    fn parse_trims() {
        assert_eq!(
            ClientCli::parse_line("  traffic ahead  ".into()),
            Some(ClientMessage::SendText {
                content: "traffic ahead".into(),
            })
        );
    }

    #[test]
    fn parse_blanks() {
        assert_eq!(ClientCli::parse_line("".into()), None);
        assert_eq!(ClientCli::parse_line(" \t ".into()), None);
    }

    #[test]
    fn parse_unicode() {
        assert_eq!(
            ClientCli::parse_line("  traffic  is  slow 🚚  ".into()),
            Some(ClientMessage::SendText {
                content: "traffic  is  slow 🚚".into(),
            })
        );
    }

    #[test]
    fn parse_large_message() {
        let content = "x".repeat(16_384);
        assert_eq!(
            ClientCli::parse_line(content.clone()),
            Some(ClientMessage::SendText { content })
        );
    }

    #[tokio::test]
    async fn auth_registers() {
        let expected_request = ClientMessage::Register {
            username: "driver17".into(),
            password: "Pass1234".into(),
        };

        let (username, requests) = run_client(
            b"2\ndriver17\nPass1234\n",
            &[ServerMessage::AuthResult(Ok(()))],
        )
        .await;
        assert_eq!(username, Some("driver17".into()));
        assert_eq!(requests, vec![expected_request]);
    }

    #[tokio::test]
    async fn auth_logs_in() {
        let expected_request = ClientMessage::Login {
            username: "driver17".into(),
            password: "Pass1234".into(),
        };

        let (username, requests) = run_client(
            b"1\ndriver17\nPass1234\n",
            &[ServerMessage::AuthResult(Ok(()))],
        )
        .await;
        assert_eq!(username, Some("driver17".into()));
        assert_eq!(requests, vec![expected_request]);
    }

    #[tokio::test]
    async fn auth_retries_on_rejection() {
        let responses = [
            ServerMessage::AuthResult(Err("Username already exists".into())),
            ServerMessage::AuthResult(Ok(())),
        ];
        let expected_requests = [
            ClientMessage::Register {
                username: "driver17".into(),
                password: "Pass1234".into(),
            },
            ClientMessage::Login {
                username: "driver17".into(),
                password: "Pass1234".into(),
            },
        ];
        let (username, requests) = run_client(
            b"2\ndriver17\nPass1234\n1\ndriver17\nPass1234\n",
            &responses,
        )
        .await;
        assert_eq!(username, Some("driver17".into()));
        assert_eq!(requests, expected_requests);
    }

    #[tokio::test]
    async fn auth_skips_invalid_choice() {
        assert_login_after_skipped_input(b"wrong\n1\ndriver17\nPass1234\n").await;
    }

    #[tokio::test]
    async fn auth_skips_blank_username() {
        assert_login_after_skipped_input(b"1\n\n1\ndriver17\nPass1234\n").await;
    }

    #[tokio::test]
    async fn auth_skips_blank_password() {
        assert_login_after_skipped_input(b"1\ndriver17\n\n1\ndriver17\nPass1234\n").await;
    }

    #[tokio::test]
    async fn auth_retries_after_error_reply() {
        assert_login_after_retry(ServerMessage::ErrorMessage("try again".into())).await;
    }

    #[tokio::test]
    async fn auth_retries_after_unexpected_reply() {
        assert_login_after_retry(ServerMessage::TextMessage {
            sender: "FleetAdmin".into(),
            content: "not an auth reply".into(),
        })
        .await;
    }

    #[tokio::test]
    async fn auth_stops_when_server_closes() {
        let (username, requests) = run_client(b"1\ndriver17\nPass1234\n", &[]).await;

        assert_eq!(username, None);
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn auth_stops_on_bad_reply() {
        let (mut input_sender, input_stream) = tokio::io::duplex(1024);
        input_sender
            .write_all(b"1\ndriver17\nPass1234\n")
            .await
            .unwrap();
        drop(input_sender);
        let mut input = BufReader::new(input_stream);

        let (mut response_sender, response_stream) = tokio::io::duplex(1024);
        response_sender.write_all(b"not-json\n").await.unwrap();
        drop(response_sender);
        let mut server_reader = ServerReader::new(response_stream);
        let (mut request_sender, _request_stream) = tokio::io::duplex(1024);

        assert_eq!(
            ClientCli::authenticate_with_input(&mut input, &mut server_reader, &mut request_sender)
                .await,
            None
        );
    }

    #[tokio::test]
    async fn auth_stops_on_write_error() {
        let (mut input_sender, input_stream) = tokio::io::duplex(1024);
        input_sender
            .write_all(b"1\ndriver17\nPass1234\n")
            .await
            .unwrap();
        drop(input_sender);
        let mut input = BufReader::new(input_stream);

        let (response_sender, response_stream) = tokio::io::duplex(1024);
        drop(response_sender);
        let mut server_reader = ServerReader::new(response_stream);
        let (mut request_sender, request_stream) = tokio::io::duplex(1024);
        drop(request_stream);

        assert_eq!(
            ClientCli::authenticate_with_input(&mut input, &mut server_reader, &mut request_sender)
                .await,
            None
        );
    }

    #[tokio::test]
    async fn prompt_trims_input() {
        let (mut sender, stream) = tokio::io::duplex(64);
        sender.write_all(b"  driver17  \r\n").await.unwrap();
        drop(sender);
        let mut input = BufReader::new(stream);

        assert_eq!(prompt_input(&mut input, "").await, Some("driver17".into()));
    }

    #[tokio::test]
    async fn prompt_stops_at_eof() {
        let (sender, stream) = tokio::io::duplex(64);
        drop(sender);
        let mut input = BufReader::new(stream);

        assert_eq!(prompt_input(&mut input, "").await, None);
    }
}
