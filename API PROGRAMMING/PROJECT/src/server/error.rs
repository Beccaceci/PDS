use std::fmt;

/// Typed failures produced by server-domain operations.
///
/// These variants are deliberately independent of transport and database
/// implementation errors, so callers can make decisions without matching user
/// facing strings. `Display` keeps the current protocol text stable at the
/// handler boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    /// Username length does not meet the required 4-20 characters range.
    InvalidUsernameLength,
    /// Username contains non-alphanumeric characters.
    InvalidUsernameCharacters,
    /// Password length is shorter than the required minimum 8 characters.
    PasswordTooShort,
    /// Password lacks required character classes (lowercase, uppercase, digit).
    PasswordMissingRequiredCharacters,
    /// Username is already registered in the system.
    UsernameTaken,
    /// Provided credentials do not match any registered account or are invalid.
    InvalidCredentials,
    /// User is already active in another concurrent network session.
    AlreadyLoggedIn,
    /// This network session has not authenticated a user.
    Unauthenticated,
    /// This network session has already authenticated a user.
    AlreadyAuthenticated,
    /// Requested user account does not exist in the registry.
    UserNotFound,
    /// Submitted GPS position timestamp is older than the last recorded position.
    OutOfOrderPosition,
    /// Text message payload is empty or contains only whitespace.
    EmptyMessage,
    /// Target recipient driver is not currently connected to the server.
    RecipientOffline { recipient: String },
    /// Target recipient driver's in-memory message queue is saturated.
    MailboxFull { recipient: String },
    /// Bcrypt password hashing computation failed.
    PasswordHashingFailed,
    /// Bcrypt password verification failed.
    PasswordVerificationFailed,
    /// SQLite database read or write operation failed.
    PersistenceFailed,
    /// Tokio background blocking task execution failed.
    BackgroundTaskFailed,
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidUsernameLength => {
                "Validation error: Username must be between 4 and 20 characters long"
            }
            Self::InvalidUsernameCharacters => "Validation error: Username must be alphanumeric",
            Self::PasswordTooShort => {
                "Validation error: Password must be at least 8 characters long"
            }
            Self::PasswordMissingRequiredCharacters => {
                "Validation error: Password must contain at least one lowercase letter, one uppercase letter, and one number"
            }
            Self::UsernameTaken => "Registration failed: username is already registered",
            Self::InvalidCredentials => "Incorrect username or password",
            Self::AlreadyLoggedIn => "User is already logged in from another session",
            Self::Unauthenticated => {
                "Access denied: Client is not authenticated. Please log in first."
            }
            Self::AlreadyAuthenticated => "You are already logged in on this session.",
            Self::UserNotFound => "User profile not found",
            Self::OutOfOrderPosition => {
                "Invalid timestamp: Point is older than last recorded position"
            }
            Self::EmptyMessage => {
                "Validation error: Message content cannot be empty or whitespace only"
            }
            Self::RecipientOffline { recipient } => {
                return write!(
                    formatter,
                    "Driver '{recipient}' is not currently connected."
                );
            }
            Self::MailboxFull { recipient } => {
                return write!(
                    formatter,
                    "Driver '{recipient}' message queue is saturated (full)."
                );
            }
            Self::PasswordHashingFailed => "Server error: failed to hash password",
            Self::PasswordVerificationFailed => "Server error: failed to verify password",
            Self::PersistenceFailed => "Server error: failed to persist data",
            Self::BackgroundTaskFailed => "Server error: background task failed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ServerError {}
