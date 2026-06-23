use crate::error::{Error, Result};
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use crate::tx::TxBody;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;

/// Map a `workflow_status` insert failure to a typed deduplication error when it
/// is a unique-constraint violation — the queue-scoped dedup index. A primary
/// key conflict never reaches here; the inserts use `ON CONFLICT DO NOTHING`.
pub(crate) fn dedup_or(e: sqlx::Error, s: &WorkflowStatus) -> Error {
    let err = Error::from(e);
    if err.is_unique_violation() {
        return Error::queue_deduplicated(
            s.queue_name.clone().unwrap_or_default(),
            s.dedup_id.clone().unwrap_or_default(),
        );
    }
    err
}

/// Map a notification insert failure to a typed "no such workflow" error when
/// the destination foreign key is violated.
pub(crate) fn nonexistent_or(e: sqlx::Error, destination_id: &str) -> Error {
    let err = Error::from(e);
    if err.is_foreign_key_violation() {
        return Error::nonexistent_workflow(destination_id);
    }
    err
}

/// Encode the authenticated-roles list for storage in the single nullable
/// `authenticated_roles` text column: a JSON array, or `NULL` when empty. This
/// is the cross-SDK on-disk shape, so workers in other languages read it back.
pub(crate) fn encode_roles(roles: &[String]) -> Option<String> {
    if roles.is_empty() {
        None
    } else {
        serde_json::to_string(roles).ok()
    }
}

/// Decode the `authenticated_roles` column written by [`encode_roles`] (or by
/// another SDK): a JSON array of strings, with `NULL`/unparseable → empty.
pub(crate) fn decode_roles(stored: Option<&str>) -> Vec<String> {
    stored
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

/// Workflow lifecycle states — the values stored in the `status` column.
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

/// Value written into a stream's `value` column to mark it closed. Stored
/// verbatim (no serialization), so a reader in any language recognizes the
/// close the same way — a shared on-disk identifier, like the `DBOS.*` step
/// names. User values are serializer-encoded, so they never collide with it.
pub(crate) const STREAM_CLOSED_SENTINEL: &str = "__DBOS_STREAM_CLOSED__";

/// `true` if `status` is terminal (no further execution will occur).
pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        STATUS_SUCCESS | STATUS_ERROR | STATUS_CANCELLED | STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    )
}

/// A persisted workflow instance.
///
/// Carries everything the engine, queues, and management APIs need. Fields for
/// features that are not implemented yet are present anyway so the storage
/// schema stays stable as those features land.
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
    /// Partition key within a partitioned queue, if any.
    pub queue_partition_key: Option<String>,
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
    /// User on whose behalf the workflow was started, if any.
    pub authenticated_user: Option<String>,
    /// Role the workflow assumed for this run, if any.
    pub assumed_role: Option<String>,
    /// Full set of roles available to the authenticated user.
    pub authenticated_roles: Vec<String>,
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
            queue_partition_key: None,
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
            authenticated_user: None,
            assumed_role: None,
            authenticated_roles: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

/// Filter for [`StateProvider::list_workflows`]. All fields are ANDed;
/// empty/`None` fields are ignored. Times are epoch milliseconds.
///
/// `start_time_ms`/`end_time_ms` bound `created_at`; the dedicated
/// `completed_*`/`dequeued_*` bounds match `completed_at`/`started_at`.
#[derive(Clone)]
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
    /// Lower/upper bound on `completed_at` (epoch ms).
    pub completed_after_ms: Option<i64>,
    pub completed_before_ms: Option<i64>,
    /// Lower/upper bound on `started_at` — when the workflow was dequeued/started
    /// (epoch ms).
    pub dequeued_after_ms: Option<i64>,
    pub dequeued_before_ms: Option<i64>,
    /// `Some(true)` keeps only workflows that have a parent; `Some(false)` only
    /// those that don't; `None` does not filter on parentage.
    pub has_parent: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Sort by `created_at` descending instead of ascending.
    pub sort_desc: bool,
    /// Return only workflows that are (or were) on a queue — those with a
    /// non-null `queue_name`.
    pub queues_only: bool,
    /// When `false`, the `input` field is omitted from results (returned as
    /// `Null`) and not read from the database. Defaults to `true`.
    pub load_input: bool,
    /// When `false`, the `output` field is omitted from results (returned as
    /// `None`) and not read from the database. Defaults to `true`.
    pub load_output: bool,
}

impl Default for ListFilter {
    fn default() -> Self {
        Self {
            workflow_ids: Vec::new(),
            workflow_id_prefix: None,
            name: None,
            status: Vec::new(),
            queue_name: None,
            app_version: None,
            executor_ids: Vec::new(),
            forked_from: None,
            start_time_ms: None,
            end_time_ms: None,
            completed_after_ms: None,
            completed_before_ms: None,
            dequeued_after_ms: None,
            dequeued_before_ms: None,
            has_parent: None,
            limit: None,
            offset: None,
            sort_desc: false,
            queues_only: false,
            // Loading input/output is the default; callers opt out for cheaper scans.
            load_input: true,
            load_output: true,
        }
    }
}

/// Grouping and filters for [`StateProvider::get_workflow_aggregates`]: count
/// workflows grouped by one or more `workflow_status` columns and/or a
/// `created_at` time bucket.
///
/// At least one `by_*` flag must be set, or `time_bucket_ms` must be `Some`;
/// the filter fields narrow which workflows are counted before grouping.
#[derive(Clone, Default)]
pub struct WorkflowAggregateQuery {
    pub by_status: bool,
    pub by_name: bool,
    pub by_queue_name: bool,
    pub by_executor_id: bool,
    pub by_app_version: bool,
    /// Also group by `created_at` bucket of this size in milliseconds.
    pub time_bucket_ms: Option<i64>,
    // Filters (all ANDed; empty/`None` ignored).
    pub status: Vec<String>,
    pub name: Vec<String>,
    pub app_version: Vec<String>,
    pub executor_ids: Vec<String>,
    pub queue_names: Vec<String>,
    pub workflow_id_prefix: Option<String>,
    pub start_time_ms: Option<i64>,
    pub end_time_ms: Option<i64>,
    /// Cap on the number of group rows returned.
    pub limit: Option<i64>,
}

/// The grouping-dimension keys used in [`WorkflowAggregate::group`], in a stable
/// order. Shared identifiers, matching the `workflow_status` column names.
pub(crate) const AGG_DIMENSIONS: &[(&str, &str)] = &[
    ("status", "status"),
    ("name", "name"),
    ("queue_name", "queue_name"),
    ("executor_id", "executor_id"),
    ("application_version", "application_version"),
];

impl WorkflowAggregateQuery {
    /// The enabled grouping dimensions as `(group_key, column)` pairs, in stable
    /// order; the `time_bucket` dimension (if any) is handled separately by each
    /// backend since it is a computed expression.
    pub(crate) fn enabled_columns(&self) -> Vec<(&'static str, &'static str)> {
        let flags = [
            self.by_status,
            self.by_name,
            self.by_queue_name,
            self.by_executor_id,
            self.by_app_version,
        ];
        AGG_DIMENSIONS
            .iter()
            .zip(flags)
            .filter(|(_, on)| *on)
            .map(|(d, _)| *d)
            .collect()
    }

    /// `true` when nothing to group by — an invalid query.
    pub fn is_empty(&self) -> bool {
        self.enabled_columns().is_empty() && self.time_bucket_ms.is_none()
    }
}

/// One aggregate group from [`StateProvider::get_workflow_aggregates`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowAggregate {
    /// Each enabled grouping dimension → its value for this group. `None` is a
    /// NULL grouped value (e.g. a workflow with no `queue_name`). The
    /// `time_bucket` value, when present, is the bucket's start in epoch ms.
    pub group: std::collections::BTreeMap<String, Option<String>>,
    /// How many workflows fell into this group.
    pub count: i64,
}

/// A step's status derived from `operation_outputs`: a NULL `error` means the
/// step succeeded, otherwise it errored. There is no explicit status column, so
/// this SQL expression stands in for one wherever step status is grouped or
/// filtered.
pub(crate) const STEP_STATUS_EXPR: &str =
    "(CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)";

/// Grouping, selected aggregates, and filters for
/// [`StateProvider::get_step_aggregates`]: aggregate `operation_outputs` rows
/// grouped by function name and/or derived status and/or a `completed_at` time
/// bucket.
///
/// At least one `by_*` flag must be set or `time_bucket_ms` must be `Some`, and
/// at least one `select_*` flag must be set.
#[derive(Clone, Default)]
pub struct StepAggregateQuery {
    pub by_function_name: bool,
    pub by_status: bool,
    /// Select the per-group row count.
    pub select_count: bool,
    /// Select the per-group maximum step duration (`completed_at - started_at`).
    /// Rows with no recorded timing (instantaneous markers) are ignored.
    pub select_max_duration_ms: bool,
    /// Also group by `completed_at` bucket of this size in milliseconds.
    pub time_bucket_ms: Option<i64>,
    // Filters (all ANDed; empty/`None` ignored).
    pub status: Vec<String>,
    pub function_name: Vec<String>,
    pub workflow_id_prefix: Option<String>,
    pub completed_after_ms: Option<i64>,
    pub completed_before_ms: Option<i64>,
    /// Cap on the number of group rows returned.
    pub limit: Option<i64>,
}

impl StepAggregateQuery {
    /// The enabled grouping dimensions as `(group_key, sql_expr)` pairs, in
    /// stable order. `status` maps to [`STEP_STATUS_EXPR`] rather than a column;
    /// `time_bucket` is a computed expression handled separately per backend.
    pub(crate) fn group_exprs(&self) -> Vec<(&'static str, &'static str)> {
        let mut v = Vec::new();
        if self.by_function_name {
            v.push(("function_name", "function_name"));
        }
        if self.by_status {
            v.push(("status", STEP_STATUS_EXPR));
        }
        v
    }

    /// `true` when nothing to group by — an invalid query.
    pub fn no_grouping(&self) -> bool {
        !self.by_function_name && !self.by_status && self.time_bucket_ms.is_none()
    }

    /// `true` when no aggregate is selected — an invalid query.
    pub fn no_select(&self) -> bool {
        !self.select_count && !self.select_max_duration_ms
    }
}

/// One aggregate group from [`StateProvider::get_step_aggregates`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepAggregate {
    /// Each enabled grouping dimension → its value (`function_name`, `status`,
    /// and/or `time_bucket` as the bucket start in epoch ms).
    pub group: std::collections::BTreeMap<String, Option<String>>,
    /// Row count for this group, if `select_count` was set.
    pub count: Option<i64>,
    /// Maximum step duration in ms for this group, if `select_max_duration_ms`
    /// was set; `None` when no row in the group had recorded timing.
    pub max_duration_ms: Option<i64>,
}

/// One recorded operation of a workflow.
///
/// Materialized from an `operation_outputs` row by
/// [`StateProvider::get_workflow_steps`]; durable steps, sleeps, sends, and
/// child-workflow invocations all surface here, ordered by [`step_id`](Self::step_id).
#[derive(Clone, Debug)]
pub struct StepInfo {
    /// Sequence index of the operation within the workflow (its `function_id`).
    pub step_id: i32,
    /// The step's recorded name (e.g. a step name, or `DBOS.sleep`/`DBOS.send`).
    pub name: String,
    /// The decoded output, if any (`None` for operations that record no value).
    pub output: Option<Value>,
    /// The recorded error string, if the operation failed.
    pub error: Option<String>,
    /// The child workflow this operation started, if it was a child-workflow call.
    pub child_workflow_id: Option<String>,
    /// When the operation started, if step timing was recorded.
    pub started_at: Option<DateTime<Utc>>,
    /// When the operation completed, if step timing was recorded.
    pub completed_at: Option<DateTime<Utc>>,
}

/// A registered application version (a row of `application_versions`). The
/// "latest" version is the one with the most recent [`version_timestamp`](Self::version_timestamp).
#[derive(Clone, Debug)]
pub struct VersionInfo {
    /// Stable unique id for this version row.
    pub version_id: String,
    /// The application version string (e.g. `0.1.0`).
    pub version_name: String,
    /// Recency marker; bumped by `set_latest_application_version` so the version
    /// sorts to the top. Versions are ordered newest-first by this.
    pub version_timestamp: DateTime<Utc>,
    /// When the version was first registered.
    pub created_at: DateTime<Utc>,
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
    /// For a partitioned queue, restrict the claim to this partition and scope
    /// the concurrency / rate-limit counts to it. `None` for a non-partitioned
    /// queue (matches the queue's rows regardless of partition key).
    pub partition_key: Option<String>,
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

    /// The id of the workflow currently holding the deduplication slot
    /// `(queue_name, dedup_id)`, if any. Backs
    /// [`DeduplicationPolicy::ReturnExisting`](crate::DeduplicationPolicy::ReturnExisting):
    /// on a dedup collision the enqueue returns a handle to this workflow.
    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>>;

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
    /// `started_at_ms` is when the step's work began (epoch ms); the
    /// implementation stamps `completed_at` itself as the time of the write.
    /// `None` records no start time — used for instantaneous operations (sends,
    /// event sets, sleep markers) that have no duration; such rows are excluded
    /// from step duration aggregates.
    ///
    /// Durable sleep is built on this too: the wake instant is recorded as an
    /// ordinary step (`DBOS.sleep`) in `operation_outputs` — there is no
    /// separate timers table.
    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
        started_at_ms: Option<i64>,
    ) -> Result<Value>;

    /// Run a transactional step: `body`'s SQL writes and this step's
    /// `operation_outputs` checkpoint commit in **one** database transaction, so
    /// the writes happen exactly once. Returns the step's JSON output — `body`'s
    /// on the first run, or the stored one on replay (when `body` is not run).
    /// On a `body` error the transaction rolls back (no checkpoint), so the step
    /// re-runs on replay, matching ordinary steps. SQL backends only; the
    /// in-memory provider returns an error.
    async fn run_transaction_step(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        started_at_ms: i64,
        body: TxBody<'_>,
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

    /// Distinct non-null partition keys among the `ENQUEUED` workflows on
    /// `queue_name`. The dispatcher of a partitioned queue iterates these and
    /// dequeues each partition independently.
    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>>;

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

    /// Count workflows grouped per `query` (one [`WorkflowAggregate`] per
    /// non-empty group). The engine validates that the query groups by at least
    /// one dimension before calling this.
    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>>;

    /// Aggregate step (`operation_outputs`) rows grouped per `query`, selecting
    /// count and/or max duration. The engine validates that the query groups by
    /// at least one dimension and selects at least one aggregate before calling.
    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>>;

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

    /// Route an existing row to a queue: set it `ENQUEUED` on `queue`, clearing
    /// the owning executor and start time so a dispatcher claims it fresh. Used
    /// to re-execute a resumed/forked workflow on a running engine without
    /// re-running it locally. A no-op if the id is gone.
    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()>;

    /// Cancel many workflows in one round-trip. Each existing, non-terminal id is
    /// set `CANCELLED` (same effect as [`cancel_workflow`](Self::cancel_workflow));
    /// missing or already-terminal ids are silently skipped (no error). An empty
    /// slice is a no-op.
    async fn cancel_workflows(&self, ids: &[String]) -> Result<()>;

    /// Resume many workflows in one round-trip. Each existing id that is not
    /// `SUCCESS`/`ERROR` returns to `PENDING` (same effect as
    /// [`resume_workflow`](Self::resume_workflow)). Returns the ids actually
    /// transitioned, so the caller can re-dispatch exactly those. An empty slice
    /// returns an empty vec.
    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>>;

    /// Delete workflows and (via `ON DELETE CASCADE`) their step / event / stream
    /// rows, regardless of state. When `delete_children`, every descendant by
    /// `parent_workflow_id` (transitively) is deleted too. Missing ids are
    /// skipped. An empty slice is a no-op.
    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()>;

    /// Reschedule a `DELAYED` workflow: set its `delay_until` to
    /// `delay_until_ms`. Only affects a row currently in `DELAYED` (a queue
    /// dispatcher promotes it to `ENQUEUED` once due); anything else is a no-op.
    /// Returns whether a row was updated.
    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool>;

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

    /// Idempotently record that `parent_id`'s step `seq` started child workflow
    /// `child_id`. Stored as an `operation_outputs` checkpoint carrying the child
    /// id, so a replay of the parent re-attaches to the same child instead of
    /// starting a new one. A second record for the same `(parent_id, seq)` is a
    /// no-op.
    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()>;

    /// Return the child workflow id `parent_id` started at step `seq`, if one was
    /// recorded by [`record_child_workflow`](Self::record_child_workflow).
    async fn check_child_workflow(&self, parent_id: &str, seq: i32) -> Result<Option<String>>;

    /// List a workflow's recorded operations (its `operation_outputs` rows) as
    /// [`StepInfo`], ordered by `step_id`. Outputs are decoded per each row's
    /// recorded serialization format. Returns an empty list for an unknown
    /// workflow or one that has run no steps.
    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>>;

    /// The `function_name` recorded at `(workflow_id, seq)`, if a row exists.
    /// Used by the patch system to tell a marker from a pre-patch step.
    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>>;

    /// Idempotently record a name-only marker row at `(workflow_id, seq)` (no
    /// output) — the checkpoint the patch system writes so a replay observes the
    /// same patch decision. A second record for the same key is a no-op.
    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()>;

    /// Append one entry to the append-only stream `(workflow_id, key)` at the
    /// next offset (`MAX(offset) + 1`, starting at 0), stamped with the producing
    /// step's `function_id`. `value` is the user value to encode and store;
    /// `None` writes the close sentinel instead, which seals the stream. Errors
    /// if the stream is already closed. The destination workflow's existence is
    /// enforced by the streams foreign key.
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        function_id: i32,
    ) -> Result<()>;

    /// Read stream `(workflow_id, key)` entries with `offset >= from_offset` in
    /// order, decoding each per its stored serialization. Returns the decoded
    /// values and whether the close sentinel was reached (the sentinel itself is
    /// not included). Reading never blocks — the caller polls.
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)>;

    /// Insert a schedule row. The `schedule_name` is unique, so creating one that
    /// already exists is a unique violation.
    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()>;

    /// Atomically replace each named schedule (delete-by-name then insert) in a
    /// single transaction, so the whole batch is all-or-nothing: a failure on
    /// any entry leaves every prior entry — and any pre-existing rows the batch
    /// would have replaced — untouched. The caller validates the entries and
    /// mints a fresh `schedule_id` for each beforehand.
    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()>;

    /// All schedules matching `filter` (empty filter returns every schedule),
    /// ordered by `schedule_name`.
    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>>;

    /// Set a schedule's status. Returns whether a row matched.
    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool>;

    /// Stamp `last_fired_at` (epoch ms) on a schedule. A no-op if it is gone.
    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()>;

    /// Delete a schedule by name. Returns whether a row was removed.
    async fn delete_schedule(&self, name: &str) -> Result<bool>;

    /// Register an application version, idempotently (no-op if `version_name`
    /// already exists). Stamps both timestamps with now.
    async fn create_application_version(&self, version_name: &str) -> Result<()>;

    /// All registered application versions, newest `version_timestamp` first.
    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>>;

    /// The version with the most recent `version_timestamp`, or `None` if none
    /// are registered.
    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>>;

    /// Mark a version as latest by bumping its `version_timestamp` to now.
    /// Returns whether a row matched (no-op if the name is unknown).
    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::{decode_roles, encode_roles};

    #[test]
    fn roles_round_trip_as_json_array() {
        // Empty maps to NULL (no column value) and back to an empty list.
        assert_eq!(encode_roles(&[]), None);
        assert!(decode_roles(None).is_empty());

        // A populated list is stored as a JSON array string — the shared on-disk
        // shape other SDKs read — and decodes back unchanged.
        let roles = vec!["admin".to_string(), "user".to_string()];
        let stored = encode_roles(&roles).expect("non-empty roles encode to Some");
        assert_eq!(stored, r#"["admin","user"]"#);
        assert_eq!(decode_roles(Some(&stored)), roles);
    }

    #[test]
    fn decode_roles_tolerates_garbage() {
        // A malformed column never panics; it degrades to no roles.
        assert!(decode_roles(Some("not json")).is_empty());
    }
}
