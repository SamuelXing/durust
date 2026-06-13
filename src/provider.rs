use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

/// Workflow lifecycle states, aligned with the DBOS Go SDK.
///
/// `ENQUEUED` — sitting in a queue, waiting to be claimed by a dispatcher.
/// `DELAYED` — enqueued with a delay; transitions to `ENQUEUED` when due.
/// `PENDING` — claimed by an executor and running.
/// `SUCCESS` / `ERROR` — terminal outcomes.
/// `CANCELLED` — terminated by an operator; replay is refused.
/// `MAX_RECOVERY_ATTEMPTS_EXCEEDED` — recovered too many times; parked until
/// manually resumed.
pub const STATUS_ENQUEUED: &str = "ENQUEUED";
pub const STATUS_DELAYED: &str = "DELAYED";
pub const STATUS_PENDING: &str = "PENDING";
pub const STATUS_SUCCESS: &str = "SUCCESS";
pub const STATUS_ERROR: &str = "ERROR";
pub const STATUS_CANCELLED: &str = "CANCELLED";
pub const STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED: &str = "MAX_RECOVERY_ATTEMPTS_EXCEEDED";

/// `true` if `status` is terminal (no further execution will occur).
pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        STATUS_SUCCESS | STATUS_ERROR | STATUS_CANCELLED | STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    )
}

/// A persisted workflow instance — the Go SDK's `WorkflowStatus`.
///
/// Carries everything the engine, queues, and management APIs need. Fields for
/// features that are not implemented yet (e.g. child workflows) are present
/// anyway so the storage schema stays stable as those features land.
#[derive(Clone, Debug)]
pub struct WorkflowStatus {
    pub id: String,
    pub name: String,
    pub status: String,
    pub input: Value,
    /// Present once the workflow reaches `SUCCESS`.
    pub output: Option<Value>,
    /// Present once the workflow reaches `ERROR`.
    pub error: Option<String>,
    /// The executor (process) that owns this run; empty until claimed.
    pub executor_id: String,
    /// Application version that produced this row — recovery is version-gated.
    pub app_version: String,
    /// Queue this workflow was enqueued on, if any.
    pub queue_name: Option<String>,
    /// Dispatch priority within a queue; lower runs first.
    pub priority: i32,
    /// Deduplication key, unique per queue among active workflows.
    pub dedup_id: Option<String>,
    /// How many times recovery has re-dispatched this workflow (not yet
    /// incremented; reserved for capping recovery retries).
    pub recovery_attempts: i32,
    /// Parent workflow id (reserved for child workflows; not yet populated).
    pub parent_workflow_id: Option<String>,
    /// Wall-clock timeout for the whole workflow, if one was requested.
    /// For queued workflows the deadline is computed from this at claim time.
    pub timeout_ms: Option<i64>,
    /// Absolute deadline in epoch millis, fixed once the workflow starts.
    pub deadline_ms: Option<i64>,
    /// When the workflow was claimed and started (ENQUEUED→PENDING), epoch ms.
    pub started_at_ms: Option<i64>,
    /// `true` when dequeued from a rate-limited queue, so the rate limiter only
    /// counts starts it governs.
    pub rate_limited: bool,
    /// For `DELAYED` workflows: when to transition to `ENQUEUED`, epoch ms.
    pub delay_until_ms: Option<i64>,
    /// When the workflow reached a terminal state, epoch ms.
    pub completed_at_ms: Option<i64>,
    /// On a forked workflow, the id it was forked from.
    pub forked_from: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WorkflowStatus {
    /// A fresh row for `id`/`name`/`input` in the given non-terminal `status`,
    /// stamped with the owning executor and app version. Optional fields default
    /// to empty; callers set queue/priority/etc. as needed.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        input: Value,
        status: impl Into<String>,
        executor_id: impl Into<String>,
        app_version: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            name: name.into(),
            status: status.into(),
            input,
            output: None,
            error: None,
            executor_id: executor_id.into(),
            app_version: app_version.into(),
            queue_name: None,
            priority: 0,
            dedup_id: None,
            recovery_attempts: 0,
            parent_workflow_id: None,
            timeout_ms: None,
            deadline_ms: None,
            started_at_ms: None,
            rate_limited: false,
            delay_until_ms: None,
            completed_at_ms: None,
            forked_from: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Filter for [`StateProvider::list_workflows`] — the Rust analog of Go's
/// `ListWorkflows` options. All fields are ANDed; empty/`None` fields are
/// ignored. Times are epoch milliseconds, matched against `created_at`.
#[derive(Clone, Default)]
pub struct ListFilter {
    pub workflow_ids: Vec<String>,
    pub workflow_id_prefix: Option<String>,
    pub name: Option<String>,
    /// Match any of these statuses.
    pub status: Vec<String>,
    pub queue_name: Option<String>,
    pub app_version: Option<String>,
    pub executor_ids: Vec<String>,
    pub forked_from: Option<String>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Sort by `created_at` descending instead of ascending.
    pub sort_desc: bool,
}

/// Parameters for one dequeue iteration, computed by the engine's dispatcher
/// from a [`crate::WorkflowQueue`]'s configuration. Plain scalars so the storage
/// layer stays decoupled from the queue type.
#[derive(Clone, Debug)]
pub struct DequeueRequest {
    pub queue_name: String,
    /// Executor claiming the workflows.
    pub executor_id: String,
    /// Only workflows of this application version (or none) are claimed.
    pub app_version: String,
    /// Upper bound for this iteration, already adjusted for worker concurrency
    /// (`worker_concurrency - locally running`).
    pub max_tasks: i64,
    /// If set, cap claims so queue-wide PENDING never exceeds this.
    pub global_concurrency: Option<i64>,
    /// If set with `rate_limit_period_ms`: cap claims so the number of
    /// rate-limited starts within the trailing period stays under this.
    pub rate_limit_max: Option<i64>,
    pub rate_limit_period_ms: Option<i64>,
}

/// The pluggable durable-state backend.
///
/// This is the single seam that decouples the runtime from storage. v0.1 ships a
/// Postgres implementation and an in-memory one; a DynamoDB / Aurora DSQL
/// implementation can be added later **without touching the engine**.
///
/// Every method must be **idempotent** with respect to its keys, because the
/// engine may re-run a workflow after a crash and replay completed steps.
#[async_trait]
pub trait StateProvider: Send + Sync {
    /// Create tables / indexes if they do not yet exist.
    async fn init(&self) -> Result<()>;

    /// Idempotently insert a workflow row. If `status.id` already exists, the
    /// existing row is returned unchanged (so a re-submitted id is a no-op, not a
    /// duplicate). This is the single creation path for both direct runs and
    /// enqueues.
    async fn insert_workflow_status(&self, status: WorkflowStatus) -> Result<WorkflowStatus>;

    /// Fetch a workflow row by id, if it exists.
    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>>;

    /// Transition a workflow to a new status, optionally writing its terminal
    /// `output` or `error`. Bumps `updated_at`.
    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<()>;

    /// Return a previously checkpointed step result, if any.
    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>>;

    /// Idempotently record a step result keyed by `(workflow_id, seq)`.
    ///
    /// Returns the **canonical** stored value: if a concurrent/duplicate
    /// execution already wrote this step, the previously-stored value wins and is
    /// returned, guaranteeing every caller observes the same result.
    ///
    /// Durable sleep is built on this too: the wake instant is recorded as an
    /// ordinary step (`DBOS.sleep`), exactly as the Go SDK stores it in
    /// `operation_outputs` — there is no separate timers table.
    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
    ) -> Result<Value>;

    /// Atomically claim up to `req.max_tasks` `ENQUEUED` workflows from a queue,
    /// transitioning them to `PENDING` stamped with the claiming executor, the
    /// app version, and `started_at`. Candidates are ordered by
    /// `(priority, created_at)`. Honors `global_concurrency` (queue-wide PENDING
    /// cap) and the rate-limit window if set; for workflows with a stored
    /// `timeout_ms`, the absolute deadline is fixed at claim time.
    ///
    /// Must be safe under concurrent dispatchers: a workflow is claimed by
    /// exactly one caller (Postgres uses `FOR UPDATE SKIP LOCKED` / `NOWAIT`).
    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>>;

    /// Transition every `DELAYED` workflow whose `delay_until_ms <= now_ms` to
    /// `ENQUEUED`. Returns how many were transitioned. Called by the dispatcher
    /// at the top of each polling iteration.
    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64>;

    /// Append a message for `destination_id` on `topic`. Errors if the
    /// destination workflow does not exist (FK violation in the SQL backends).
    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
    ) -> Result<()>;

    /// Atomically claim the **oldest unconsumed** message for
    /// `(workflow_id, topic)` and record it as the step checkpoint
    /// `(workflow_id, seq)` in the same transaction — if claiming and
    /// checkpointing were separate, a crash between them would lose the
    /// message. Returns the message, or `None` when the mailbox is empty
    /// (nothing is recorded in that case).
    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>>;

    /// Set (or overwrite) the value of event `key` on `workflow_id`.
    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()>;

    /// Read the current value of event `key` on `workflow_id`, if set.
    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>>;

    /// List workflows matching `filter`, newest- or oldest-first per
    /// `filter.sort_desc`.
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>>;

    /// Cancel a workflow: if it is not already terminal, set it `CANCELLED`,
    /// stamp `completed_at`, and clear queue assignment / dedup so it leaves any
    /// queue. A running workflow stops cooperatively at its next step.
    async fn cancel_workflow(&self, id: &str) -> Result<()>;

    /// Resume a `CANCELLED` (or otherwise non-terminal) workflow by returning it
    /// to `PENDING`, resetting `recovery_attempts` and clearing deadline / dedup
    /// / started / completed. Returns `true` if a row was actually transitioned
    /// (i.e. it existed and was not already `SUCCESS`/`ERROR`). The caller
    /// re-dispatches it.
    async fn resume_workflow(&self, id: &str) -> Result<bool>;

    /// Create `new_id` as a fork of `original_id`: a fresh `PENDING` workflow
    /// with the same name/input, `forked_from = original_id`, and the original's
    /// step checkpoints with `seq < start_step` copied in so execution resumes
    /// from that step. Marks the original `was_forked_from`. Errors if the
    /// original does not exist.
    async fn fork_workflow(
        &self,
        original_id: &str,
        new_id: &str,
        start_step: i32,
        app_version: &str,
    ) -> Result<()>;

    /// Atomically increment a workflow's `recovery_attempts` and return the new
    /// value. If it exceeds `max`, the workflow is parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED` instead of being recovered again.
    async fn bump_recovery_attempts(&self, id: &str, max: i32) -> Result<i32>;
}
