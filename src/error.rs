use crate::serialize::PortableWorkflowError;
use thiserror::Error;

/// A stable, programmatic classification of an [`Error`](enum@Error), returned by
/// [`Error::code`]. Lets callers branch on *what kind* of failure occurred
/// without matching every concrete variant. Non-exhaustive: new codes may be
/// added as the SDK grows, so always include a `_` arm when matching.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// A database query or connection failed.
    Database,
    /// Setting up the system database (e.g. running migrations) failed.
    Initialization,
    /// A value could not be serialized or deserialized.
    Serialization,
    /// No workflow is registered under the requested name.
    WorkflowNotRegistered,
    /// No queue is registered under the requested name.
    QueueNotRegistered,
    /// The referenced workflow id does not exist.
    NonExistentWorkflow,
    /// An enqueue was rejected because its deduplication key is already in use
    /// on the queue.
    QueueDeduplicated,
    /// The workflow was cancelled; execution was refused.
    WorkflowCancelled,
    /// Two workflows were registered under the same name (or configured-instance
    /// key) when building the engine.
    ConflictingRegistration,
    /// A blocking operation or workflow deadline elapsed.
    Timeout,
    /// A replay found a different step recorded at this position — the
    /// workflow function is non-deterministic.
    UnexpectedStep,
    /// An error raised by user code.
    Application,
}

/// The crate-wide error type.
///
/// Step closures and workflow functions return `Result<T>`; application errors
/// should use [`Error::app`]. Use [`Error::code`] for programmatic handling and
/// the `is_*` helpers to classify the underlying database failure.
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

    /// The referenced workflow id does not exist (e.g. sending to it).
    #[error("workflow `{0}` does not exist")]
    NonExistentWorkflow(String),

    /// An enqueue collided with an existing workflow holding the same
    /// deduplication key on the queue.
    #[error("deduplication id `{dedup_id}` already in use on queue `{queue_name}`")]
    QueueDeduplicated {
        queue_name: String,
        dedup_id: String,
    },

    /// The workflow was cancelled by an operator; execution was refused.
    #[error("workflow `{0}` was cancelled")]
    Cancelled(String),

    /// Two workflows were registered under the same name (or configured-instance
    /// key) when building the engine — the name→function registry must be
    /// unambiguous for recovery to re-dispatch correctly.
    #[error("workflow name `{0}` is registered more than once")]
    ConflictingRegistration(String),

    /// A blocking operation (recv/get_event/get_result) or a workflow deadline
    /// elapsed before completion.
    #[error("operation timed out")]
    Timeout,

    /// A replay found a different operation recorded at this step position:
    /// the workflow is now executing `expected` where `recorded` ran on the
    /// original execution. The workflow function is non-deterministic — its
    /// steps were reordered, renamed, added, or removed between executions —
    /// so replaying the stored checkpoint would return the wrong step's result.
    #[error(
        "workflow `{workflow_id}` step {step_id} is `{expected}` but `{recorded}` \
         is recorded there — the workflow function is non-deterministic"
    )]
    UnexpectedStep {
        workflow_id: String,
        step_id: i32,
        /// The operation the workflow is executing now.
        expected: String,
        /// The operation recorded at this position by the original execution.
        recorded: String,
    },

    /// An error raised by user code inside a step or workflow.
    #[error("{0}")]
    App(String),

    /// A structured, cross-language error raised by user code: a type/class
    /// `name`, a human `message`, and optional app-level `code`/`data`. Under
    /// portable serialization it is stored as the [`PortableWorkflowError`]
    /// envelope so an observer in any language reads its structure; the display
    /// form is the message (like the other SDKs' `Error()`). A workflow that
    /// failed under portable mode is read back as this variant.
    #[error("{}", .0.message)]
    Portable(PortableWorkflowError),
}

impl Error {
    /// Construct an application-level error from anything string-like.
    pub fn app(msg: impl Into<String>) -> Self {
        Error::App(msg.into())
    }

    /// Construct a structured cross-language error with a type `name` and a
    /// `message` (no `code`/`data`). Build [`Error::Portable`] directly to set
    /// those — its [`PortableWorkflowError`] fields are public.
    pub fn portable(name: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Portable(PortableWorkflowError {
            name: name.into(),
            message: message.into(),
            code: None,
            data: None,
        })
    }

    /// Construct a [`Error::NonExistentWorkflow`] for the given workflow id.
    pub fn nonexistent_workflow(id: impl Into<String>) -> Self {
        Error::NonExistentWorkflow(id.into())
    }

    /// Construct a [`Error::ConflictingRegistration`] for a duplicate name.
    pub fn conflicting_registration(name: impl Into<String>) -> Self {
        Error::ConflictingRegistration(name.into())
    }

    /// Construct a [`Error::QueueDeduplicated`] for a rejected enqueue.
    pub fn queue_deduplicated(queue_name: impl Into<String>, dedup_id: impl Into<String>) -> Self {
        Error::QueueDeduplicated {
            queue_name: queue_name.into(),
            dedup_id: dedup_id.into(),
        }
    }

    /// Construct an [`Error::UnexpectedStep`] for a replay that found
    /// `recorded` where the workflow is now executing `expected`.
    pub fn unexpected_step(
        workflow_id: impl Into<String>,
        step_id: i32,
        expected: impl Into<String>,
        recorded: impl Into<String>,
    ) -> Self {
        Error::UnexpectedStep {
            workflow_id: workflow_id.into(),
            step_id,
            expected: expected.into(),
            recorded: recorded.into(),
        }
    }

    /// The stable [`ErrorCode`] for this error, for programmatic handling.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::Db(_) => ErrorCode::Database,
            Error::Migrate(_) => ErrorCode::Initialization,
            Error::Serde(_) | Error::Serialization(_) => ErrorCode::Serialization,
            Error::UnknownWorkflow(_) => ErrorCode::WorkflowNotRegistered,
            Error::UnknownQueue(_) => ErrorCode::QueueNotRegistered,
            Error::NonExistentWorkflow(_) => ErrorCode::NonExistentWorkflow,
            Error::QueueDeduplicated { .. } => ErrorCode::QueueDeduplicated,
            Error::Cancelled(_) => ErrorCode::WorkflowCancelled,
            Error::ConflictingRegistration(_) => ErrorCode::ConflictingRegistration,
            Error::Timeout => ErrorCode::Timeout,
            Error::UnexpectedStep { .. } => ErrorCode::UnexpectedStep,
            Error::App(_) | Error::Portable(_) => ErrorCode::Application,
        }
    }

    /// Whether this wraps a database unique-constraint violation.
    pub fn is_unique_violation(&self) -> bool {
        matches!(self, Error::Db(sqlx::Error::Database(e)) if e.is_unique_violation())
    }

    /// Whether this wraps a database foreign-key violation.
    pub fn is_foreign_key_violation(&self) -> bool {
        matches!(self, Error::Db(sqlx::Error::Database(e)) if e.is_foreign_key_violation())
    }

    /// Whether this wraps a transient database failure worth retrying:
    /// connection loss, a closed/timed-out pool, or a busy/locked database.
    /// Serialization failures are *not* included — those need the whole
    /// transaction retried, which is the caller's decision.
    pub fn is_retryable(&self) -> bool {
        let Error::Db(e) = self else { return false };
        match e {
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
            sqlx::Error::Database(db) => {
                db.code().map(|c| is_retryable_db_code(&c)).unwrap_or(false)
            }
            _ => false,
        }
    }

    /// A transaction-level conflict that must be retried by restarting the whole
    /// transaction on a fresh one: Postgres `40001` serialization_failure / `40P01`
    /// deadlock_detected, or SQLite `SQLITE_BUSY` / `SQLITE_LOCKED`.
    pub fn is_tx_conflict(&self) -> bool {
        let Error::Db(sqlx::Error::Database(db)) = self else {
            return false;
        };
        db.code().map(|c| is_tx_conflict_code(&c)).unwrap_or(false)
    }
}

/// Classify a database error code as a transaction-level conflict (see
/// [`Error::is_tx_conflict`]).
fn is_tx_conflict_code(code: &str) -> bool {
    if code.len() == 5 {
        matches!(code, "40001" | "40P01")
    } else {
        code.parse::<i32>().is_ok_and(|n| matches!(n & 0xFF, 5 | 6))
    }
}

/// Classify a database error code as a transient (retryable) failure.
/// Postgres reports five-character SQLSTATEs; the connection-exception class is
/// `08xxx`, plus a few admin/shutdown codes. SQLite reports numeric result
/// codes whose low byte is `5` (BUSY) or `6` (LOCKED), including their extended
/// variants.
fn is_retryable_db_code(code: &str) -> bool {
    if code.len() == 5 {
        return code.starts_with("08") || matches!(code, "57P01" | "57P02" | "57P03" | "53300");
    }
    code.parse::<i32>().is_ok_and(|n| matches!(n & 0xFF, 5 | 6))
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_to_variants() {
        assert_eq!(Error::app("x").code(), ErrorCode::Application);
        assert_eq!(Error::Timeout.code(), ErrorCode::Timeout);
        assert_eq!(
            Error::nonexistent_workflow("wf").code(),
            ErrorCode::NonExistentWorkflow
        );
        assert_eq!(
            Error::queue_deduplicated("q", "d").code(),
            ErrorCode::QueueDeduplicated
        );
        assert_eq!(
            Error::UnknownWorkflow("n".into()).code(),
            ErrorCode::WorkflowNotRegistered
        );
        assert_eq!(
            Error::unexpected_step("wf", 3, "new", "old").code(),
            ErrorCode::UnexpectedStep
        );
    }

    #[test]
    fn non_db_errors_are_not_retryable_or_violations() {
        let e = Error::app("boom");
        assert!(!e.is_retryable());
        assert!(!e.is_unique_violation());
        assert!(!e.is_foreign_key_violation());
    }

    #[test]
    fn db_code_classification() {
        assert!(is_retryable_db_code("08006")); // pg connection failure
        assert!(is_retryable_db_code("57P01")); // pg admin shutdown
        assert!(is_retryable_db_code("5")); // sqlite BUSY
        assert!(is_retryable_db_code("261")); // sqlite BUSY_RECOVERY (5 | 1<<8)
        assert!(!is_retryable_db_code("23505")); // unique violation: not retryable
        assert!(!is_retryable_db_code("40001")); // serialization failure: opt-in only
    }

    #[test]
    fn tx_conflict_classification() {
        assert!(is_tx_conflict_code("40001")); // pg serialization failure
        assert!(is_tx_conflict_code("40P01")); // pg deadlock detected
        assert!(is_tx_conflict_code("5")); // sqlite BUSY
        assert!(is_tx_conflict_code("6")); // sqlite LOCKED
        assert!(!is_tx_conflict_code("23505")); // unique violation: not a conflict
        assert!(!is_tx_conflict_code("08006")); // connection failure: not a tx conflict
    }
}
