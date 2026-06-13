use thiserror::Error;

/// The crate-wide error type.
///
/// Step closures and workflow functions return `Result<T>`; application errors
/// should use [`Error::app`].
#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A stored value could not be decoded: an unrecognized serialization
    /// format, or corrupt base64.
    #[error("serialization format error: {0}")]
    Serialization(String),

    #[error("no workflow registered under name `{0}`")]
    UnknownWorkflow(String),

    #[error("no queue registered under name `{0}`")]
    UnknownQueue(String),

    /// The workflow was cancelled by an operator; execution was refused.
    #[error("workflow `{0}` was cancelled")]
    Cancelled(String),

    /// A blocking operation (recv/get_event/get_result) or a workflow deadline
    /// elapsed before completion.
    #[error("operation timed out")]
    Timeout,

    /// An error raised by user code inside a step or workflow.
    #[error("{0}")]
    App(String),
}

impl Error {
    /// Construct an application-level error from anything string-like.
    pub fn app(msg: impl Into<String>) -> Self {
        Error::App(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
