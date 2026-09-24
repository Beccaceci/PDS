//! Shared interactive terminal support for the client and server CLIs.

use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;

use crossterm::{
    ExecutableCommand,
    cursor::MoveToColumn,
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;

/// A completed user interaction from an interactive terminal.
#[derive(Debug, PartialEq, Eq)]
pub enum TerminalEvent {
    /// A line submitted with Enter. The caller is responsible for domain-specific parsing.
    Line(String),
    /// An exit request submitted with Ctrl+C.
    Exit,
}

/// Input/output boundary used by the interactive client and administrator CLIs.
///
/// Production code uses [`Terminal`]. Tests and embedding applications can
/// provide scripted implementations without enabling raw terminal mode.
pub trait CliTerminal {
    /// Displays the prompt and any input currently being edited.
    fn show_prompt(&self);

    /// Displays an asynchronous message while preserving the active prompt.
    fn write(&self, message: &str);

    /// Writes text that does not need prompt preservation.
    fn write_plain(&self, message: &str);

    /// Waits for the next completed line or exit request.
    fn read(&mut self) -> Pin<Box<dyn Future<Output = io::Result<TerminalEvent>> + '_>>;
}

/// Interactive terminal that owns raw-mode handling, editing state, and command history.
pub struct Terminal {
    prompt: String,
    input: String,
    history: InputHistory,
    events: EventStream,
    _raw_mode: RawModeGuard,
}

impl Terminal {
    /// Enables raw mode and creates a terminal with the given prompt.
    pub fn new(prompt: impl Into<String>) -> io::Result<Self> {
        Ok(Self {
            prompt: prompt.into(),
            input: String::new(),
            history: InputHistory::default(),
            events: EventStream::new(),
            _raw_mode: RawModeGuard::try_new()?,
        })
    }

    /// Displays the prompt and any in-progress input.
    pub fn show_prompt(&self) {
        refresh_prompt(&self.prompt, &self.input);
    }

    /// Prints an asynchronous message without losing the user's in-progress input.
    pub fn write(&self, message: &str) {
        print_with_prompt(message, &self.prompt, &self.input);
    }

    /// Waits for terminal input, handling editing and history navigation internally.
    pub async fn read(&mut self) -> io::Result<TerminalEvent> {
        loop {
            let Some(event) = self.events.next().await else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "terminal event stream closed",
                ));
            };
            let event = event?;

            let Event::Key(key_event) = event else {
                continue;
            };
            if key_event.kind != KeyEventKind::Press {
                continue;
            }

            match process_key_event(
                key_event.code,
                key_event.modifiers,
                &mut self.input,
                &mut self.history,
            ) {
                InputAction::InsertChar(character) => {
                    print!("{character}");
                    let _ = io::stdout().flush();
                }
                InputAction::Backspace => {
                    print!("\x08 \x08");
                    let _ = io::stdout().flush();
                }
                InputAction::Redraw => self.show_prompt(),
                InputAction::Submit(line) => {
                    self.history.push(line.clone());
                    print!("\r\n");
                    let _ = io::stdout().flush();
                    return Ok(TerminalEvent::Line(line));
                }
                InputAction::Exit => return Ok(TerminalEvent::Exit),
                InputAction::None => {}
            }
        }
    }
}

impl CliTerminal for Terminal {
    fn show_prompt(&self) {
        Terminal::show_prompt(self);
    }

    fn write(&self, message: &str) {
        Terminal::write(self, message);
    }

    fn write_plain(&self, message: &str) {
        print!("{message}");
        let _ = io::stdout().flush();
    }

    fn read(&mut self) -> Pin<Box<dyn Future<Output = io::Result<TerminalEvent>> + '_>> {
        Box::pin(Terminal::read(self))
    }
}

/// RAII guard that restores the terminal when an interactive CLI exits.
struct RawModeGuard;

impl RawModeGuard {
    fn try_new() -> io::Result<Self> {
        enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

/// History for one interactive input prompt.
#[derive(Default, Debug, PartialEq, Eq)]
struct InputHistory {
    entries: Vec<String>,
    cursor: Option<usize>,
}

impl InputHistory {
    fn push(&mut self, input: String) {
        let trimmed = input.trim();
        if !trimmed.is_empty() && self.entries.last().map(|entry| entry.as_str()) != Some(trimmed) {
            self.entries.push(trimmed.to_string());
        }
        self.cursor = None;
    }

    fn up(&mut self) -> Option<&str> {
        if self.entries.is_empty() {
            return None;
        }

        let index = self
            .cursor
            .map_or(self.entries.len() - 1, |index| index.saturating_sub(1));
        self.cursor = Some(index);
        Some(&self.entries[index])
    }

    fn down(&mut self) -> Option<&str> {
        if self.entries.is_empty() {
            return None;
        }

        match self.cursor {
            Some(index) if index + 1 < self.entries.len() => {
                let index = index + 1;
                self.cursor = Some(index);
                Some(&self.entries[index])
            }
            Some(_) => {
                self.cursor = None;
                Some("")
            }
            None => None,
        }
    }
}

enum InputAction {
    InsertChar(char),
    Backspace,
    Redraw,
    Submit(String),
    Exit,
    None,
}

fn process_key_event(
    key_code: KeyCode,
    modifiers: KeyModifiers,
    current_input: &mut String,
    history: &mut InputHistory,
) -> InputAction {
    if modifiers.contains(KeyModifiers::CONTROL) && key_code == KeyCode::Char('c') {
        return InputAction::Exit;
    }

    match key_code {
        KeyCode::Up => match history.up() {
            Some(previous) => {
                *current_input = previous.to_string();
                InputAction::Redraw
            }
            None => InputAction::None,
        },
        KeyCode::Down => match history.down() {
            Some(next) => {
                *current_input = next.to_string();
                InputAction::Redraw
            }
            None => InputAction::None,
        },
        KeyCode::Char(character) => {
            current_input.push(character);
            InputAction::InsertChar(character)
        }
        KeyCode::Backspace if current_input.pop().is_some() => InputAction::Backspace,
        KeyCode::Backspace => InputAction::None,
        KeyCode::Enter => InputAction::Submit(std::mem::take(current_input)),
        _ => InputAction::None,
    }
}

fn refresh_prompt(prompt: &str, buffer: &str) {
    let mut stdout = io::stdout();
    let _ = stdout.execute(Clear(ClearType::CurrentLine));
    let _ = stdout.execute(MoveToColumn(0));
    print!("{prompt}{buffer}");
    let _ = stdout.flush();
}

fn print_with_prompt(message: &str, prompt: &str, buffer: &str) {
    let mut stdout = io::stdout();
    let _ = stdout.execute(Clear(ClearType::CurrentLine));
    let _ = stdout.execute(MoveToColumn(0));
    print!("{message}\r\n{prompt}{buffer}");
    let _ = stdout.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_empty_nav() {
        let mut history = InputHistory::default();

        assert_eq!(history.up(), None);
        assert_eq!(history.down(), None);
    }

    #[test]
    fn history_ignores_blanks() {
        let mut history = InputHistory::default();

        history.push("".into());
        history.push(" \t\r\n ".into());

        assert_eq!(history.up(), None);
        assert_eq!(history.down(), None);
    }

    #[test]
    fn history_dedups_entries() {
        let mut history = InputHistory::default();
        history.push("first".into());
        history.push(" first ".into());

        assert_eq!(history.entries, vec!["first".to_string()]);
    }

    #[test]
    fn history_navigates_bounds() {
        let mut history = InputHistory::default();
        history.push("first".into());
        history.push("second".into());

        assert_eq!(history.up(), Some("second"));
        assert_eq!(history.up(), Some("first"));
        assert_eq!(history.up(), Some("first"));
        assert_eq!(history.down(), Some("second"));
        assert_eq!(history.down(), Some(""));
        assert_eq!(history.down(), None);
    }

    #[test]
    fn history_resets_cursor() {
        let mut history = InputHistory::default();
        history.push("first".into());
        history.push("second".into());
        assert_eq!(history.up(), Some("second"));

        history.push("third".into());

        assert_eq!(history.up(), Some("third"));
        assert_eq!(history.up(), Some("second"));
    }

    #[test]
    fn chars_append() {
        let mut buffer = String::new();
        let mut history = InputHistory::default();

        for character in ['R', 'o', 'u', 't', 'e', ' ', '🚚'] {
            assert!(matches!(
                process_key_event(
                    KeyCode::Char(character),
                    KeyModifiers::NONE,
                    &mut buffer,
                    &mut history
                ),
                InputAction::InsertChar(value) if value == character
            ));
        }
        assert_eq!(buffer, "Route 🚚");
    }

    #[test]
    fn enter_submits() {
        let mut buffer = "Route 🚚".to_string();
        let mut history = InputHistory::default();

        let action = process_key_event(
            KeyCode::Enter,
            KeyModifiers::NONE,
            &mut buffer,
            &mut history,
        );
        let InputAction::Submit(line) = action else {
            panic!("Enter must submit the current buffer");
        };

        assert_eq!(line, "Route 🚚");
        assert!(buffer.is_empty());
    }

    #[test]
    fn backspace_handles_unicode() {
        let mut buffer = "A🚚".to_string();
        let mut history = InputHistory::default();

        assert!(matches!(
            process_key_event(
                KeyCode::Backspace,
                KeyModifiers::NONE,
                &mut buffer,
                &mut history
            ),
            InputAction::Backspace
        ));
        assert_eq!(buffer, "A");
    }

    #[test]
    fn backspace_ignores_empty() {
        let mut buffer = String::new();
        let mut history = InputHistory::default();

        assert!(matches!(
            process_key_event(
                KeyCode::Backspace,
                KeyModifiers::NONE,
                &mut buffer,
                &mut history
            ),
            InputAction::None
        ));
        assert!(buffer.is_empty());
    }

    #[test]
    fn arrows_replace_buffer() {
        let mut buffer = "draft".to_string();
        let mut history = InputHistory::default();
        history.push("oldest".into());
        history.push("newest".into());

        assert!(matches!(
            process_key_event(KeyCode::Up, KeyModifiers::NONE, &mut buffer, &mut history),
            InputAction::Redraw
        ));
        assert_eq!(buffer, "newest");
        assert!(matches!(
            process_key_event(KeyCode::Down, KeyModifiers::NONE, &mut buffer, &mut history),
            InputAction::Redraw
        ));
        assert_eq!(buffer, "");
    }

    #[test]
    fn ignored_keys_do_nothing() {
        let mut buffer = String::new();
        let mut history = InputHistory::default();

        assert!(matches!(
            process_key_event(KeyCode::Home, KeyModifiers::NONE, &mut buffer, &mut history),
            InputAction::None
        ));
        assert!(buffer.is_empty());
    }

    #[test]
    fn history_trims_entries() {
        let mut history = InputHistory::default();

        history.push("  command  ".into());

        assert_eq!(history.up(), Some("command"));
    }

    #[test]
    fn ctrl_c_exits() {
        let mut buffer = String::new();
        let mut history = InputHistory::default();

        assert!(matches!(
            process_key_event(
                KeyCode::Char('c'),
                KeyModifiers::CONTROL,
                &mut buffer,
                &mut history
            ),
            InputAction::Exit
        ));
    }
}
