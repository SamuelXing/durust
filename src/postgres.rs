use crate::error::{Error, Result};
use crate::provider::{
    col_i64, col_str, decode_roles, dedup_or, encode_roles, group_stream_rows, is_terminal,
    nonexistent_or, ChangeWait, DequeueRequest, ExportedWorkflow, ListFilter, NotificationInfo,
    StateProvider, StepAggregate, StepAggregateQuery, StepInfo, VersionInfo, WorkflowAggregate,
    WorkflowAggregateQuery, WorkflowStatus, EXPORT_STATUS_STR_COLS, NOTIFICATIONS_CHANNEL,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS, STEP_STATUS_EXPR,
    STREAM_CLOSED_SENTINEL, WORKFLOW_EVENTS_CHANNEL,
};
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use crate::serialize::{self, Serializer};
use crate::tx::{IsolationLevel, TransactionOptions, Tx, TxBody};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions, Postgres};
use sqlx::{QueryBuilder, Row};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Columns selected when materializing a [`WorkflowStatus`] from `workflow_status`.
/// `serialization` drives how `inputs`/`output` are decoded (see [`crate::Serializer`]).
const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, queue_partition_key, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_timeout_ms, workflow_deadline_epoch_ms, \
     started_at_epoch_ms, rate_limited, delay_until_epoch_ms, completed_at, forked_from, \
     authenticated_user, assumed_role, authenticated_roles, \
     serialization, created_at, updated_at";

/// Routes inbound `LISTEN`/`NOTIFY` notifications to the `recv`/`get_event`
/// waiters parked on the matching payload. Waiters subscribe by payload; the
/// listener task wakes them when a `NOTIFY` for that payload arrives.
#[derive(Default)]
struct NotifyHub {
    waiters: Mutex<HashMap<String, WaitEntry>>,
}

/// The shared wake handle for everyone waiting on one payload, plus a refcount so
/// the entry is dropped when the last waiter leaves.
struct WaitEntry {
    notify: Arc<Notify>,
    count: usize,
}

/// An active subscription on one payload; deregisters on drop.
struct Subscription {
    hub: Arc<NotifyHub>,
    payload: String,
    notify: Arc<Notify>,
}

impl NotifyHub {
    /// Register interest in `payload`, returning a [`Subscription`] whose
    /// `notify` fires when the listener sees a matching `NOTIFY` (or a reconnect).
    fn subscribe(self: &Arc<Self>, payload: String) -> Subscription {
        let mut waiters = self.waiters.lock().unwrap();
        let entry = waiters.entry(payload.clone()).or_insert_with(|| WaitEntry {
            notify: Arc::new(Notify::new()),
            count: 0,
        });
        entry.count += 1;
        let notify = entry.notify.clone();
        Subscription {
            hub: Arc::clone(self),
            payload,
            notify,
        }
    }

    /// Wake everyone waiting on exactly `payload`.
    fn signal(&self, payload: &str) {
        if let Some(entry) = self.waiters.lock().unwrap().get(payload) {
            entry.notify.notify_waiters();
        }
    }

    /// Wake every waiter — used after a listener reconnect, when a notification
    /// may have been missed and all waiters should re-check the database.
    fn signal_all(&self) {
        for entry in self.waiters.lock().unwrap().values() {
            entry.notify.notify_waiters();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut waiters = self.hub.waiters.lock().unwrap();
        if let Some(entry) = waiters.get_mut(&self.payload) {
            entry.count -= 1;
            if entry.count == 0 {
                waiters.remove(&self.payload);
            }
        }
    }
}

/// Postgres-backed [`StateProvider`], built on sqlx and the canonical DBOS
/// schema (`workflow_status` / `operation_outputs`).
pub struct PostgresProvider {
    pool: PgPool,
    /// Format used when *encoding* stored values. Decoding always follows each
    /// row's recorded format, so this only sets what new rows are written as.
    serializer: Serializer,
    /// Wakes parked `recv`/`get_event` calls from the `LISTEN`/`NOTIFY` listener.
    /// Note: the listener (started in `init`) holds one pool connection for its
    /// lifetime, so the app effectively has `max_connections - 1` available.
    notify_hub: Arc<NotifyHub>,
    /// Cancels the background listener task when the provider is dropped.
    listener_token: CancellationToken,
    /// Ensures the listener task is spawned at most once (on the first `init`).
    listener_started: AtomicBool,
}

impl PostgresProvider {
    /// Connect to Postgres using a standard connection URL, e.g.
    /// `postgres://user:pass@localhost:5432/durust`.
    pub async fn connect(database_url: &str) -> Result<Self> {
        // One of these is held by the LISTEN/NOTIFY listener for its lifetime
        // (see `notify_hub`), leaving the rest for workflow/app queries.
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        Ok(Self::from_pool(pool))
    }

    /// Build a provider from an existing pool (useful if your app already owns one).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            serializer: Serializer::default(),
            notify_hub: Arc::new(NotifyHub::default()),
            listener_token: CancellationToken::new(),
            listener_started: AtomicBool::new(false),
        }
    }

    /// Choose the format new values are encoded with. Use [`Serializer::Portable`]
    /// when this database is shared with DBOS workers in other languages.
    pub fn with_serializer(mut self, serializer: Serializer) -> Self {
        self.serializer = serializer;
        self
    }
}

impl Drop for PostgresProvider {
    fn drop(&mut self) {
        // Stop the background listener task (it holds a pooled connection).
        self.listener_token.cancel();
    }
}

/// Initial (and post-recovery) delay between failed listener (re)connect/recv
/// attempts; doubles up to [`LISTENER_BACKOFF_MAX`].
const LISTENER_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Ceiling on the listener reconnect backoff.
const LISTENER_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Background task: hold a dedicated connection `LISTEN`ing on the notification
/// and event channels, and wake the matching waiters as notifications arrive.
/// Reconnects on failure (waking all waiters to re-poll, in case one was missed),
/// and exits when `token` is cancelled (provider dropped).
async fn run_listener(pool: PgPool, hub: Arc<NotifyHub>, token: CancellationToken) {
    let mut backoff = LISTENER_BACKOFF_MIN;
    loop {
        if token.is_cancelled() {
            return;
        }
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::debug!(error = %e, "notification listener: connect failed");
                if !sleep_or_cancel(backoff, &token).await {
                    return;
                }
                backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
                continue;
            }
        };
        if let Err(e) = listener
            .listen_all([NOTIFICATIONS_CHANNEL, WORKFLOW_EVENTS_CHANNEL])
            .await
        {
            tracing::debug!(error = %e, "notification listener: LISTEN failed");
            if !sleep_or_cancel(backoff, &token).await {
                return;
            }
            backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
            continue;
        }
        // Freshly (re)connected: a notification may have been missed while
        // disconnected, so nudge every waiter to re-check the database.
        hub.signal_all();

        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                res = listener.try_recv() => match res {
                    // A notification: wake exactly the channel+payload's waiters.
                    // Receiving anything proves the link works, so relax the backoff.
                    Ok(Some(n)) => {
                        backoff = LISTENER_BACKOFF_MIN;
                        hub.signal(&hub_key(n.channel(), n.payload()));
                    }
                    // try_recv returns None when it had to reconnect; re-poll all.
                    Ok(None) => {
                        backoff = LISTENER_BACKOFF_MIN;
                        hub.signal_all();
                    }
                    // Hard error: back off before rebuilding so a recv that keeps
                    // failing while connect succeeds can't spin (matches Go's loop,
                    // which sleeps on a notification error rather than retrying hot).
                    Err(e) => {
                        tracing::debug!(error = %e, "notification listener: recv failed");
                        if !sleep_or_cancel(backoff, &token).await {
                            return;
                        }
                        backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
                        break; // rebuild the listener
                    }
                }
            }
        }
    }
}

/// Hub key for one waiter: the channel and payload joined by a NUL (so a
/// notification payload never collides with an identical-looking event payload).
fn hub_key(channel: &str, payload: &str) -> String {
    format!("{channel}\u{0}{payload}")
}

/// Sleep for `dur`, returning `false` if cancelled first.
async fn sleep_or_cancel(dur: Duration, token: &CancellationToken) -> bool {
    tokio::select! {
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(dur) => true,
    }
}

/// Epoch-millis (as stored) → `DateTime<Utc>`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

/// Map a `workflow_status` row to a [`WorkflowStatus`], decoding `inputs` and
/// `output` per the row's recorded serialization format.
fn row_to_status(row: &sqlx::postgres::PgRow) -> WorkflowStatus {
    let fmt: Option<String> = row.try_get("serialization").ok().flatten();
    let fmt = fmt.as_deref();
    let inputs: Option<String> = row.try_get("inputs").ok().flatten();
    let output: Option<String> = row.try_get("output").ok().flatten();
    WorkflowStatus {
        id: row.get("workflow_uuid"),
        name: row.get("name"),
        status: row.get("status"),
        input: serialize::decode_input_opt(fmt, inputs.as_deref())
            .ok()
            .flatten()
            .unwrap_or(Value::Null),
        output: serialize::decode_opt(fmt, output.as_deref()).ok().flatten(),
        error: row.try_get("error").ok().flatten(),
        executor_id: row.get("executor_id"),
        app_version: row.get("application_version"),
        queue_name: row.try_get("queue_name").ok().flatten(),
        queue_partition_key: row.try_get("queue_partition_key").ok().flatten(),
        priority: row.get("priority"),
        dedup_id: row.try_get("deduplication_id").ok().flatten(),
        recovery_attempts: row.get::<i64, _>("recovery_attempts") as i32,
        parent_workflow_id: row.try_get("parent_workflow_id").ok().flatten(),
        timeout_ms: row.try_get("workflow_timeout_ms").ok().flatten(),
        deadline_ms: row.try_get("workflow_deadline_epoch_ms").ok().flatten(),
        started_at_ms: row.try_get("started_at_epoch_ms").ok().flatten(),
        rate_limited: row.get("rate_limited"),
        delay_until_ms: row.try_get("delay_until_epoch_ms").ok().flatten(),
        completed_at_ms: row.try_get("completed_at").ok().flatten(),
        forked_from: row.try_get("forked_from").ok().flatten(),
        authenticated_user: row.try_get("authenticated_user").ok().flatten(),
        assumed_role: row.try_get("assumed_role").ok().flatten(),
        authenticated_roles: decode_roles(
            row.try_get::<Option<String>, _>("authenticated_roles")
                .ok()
                .flatten()
                .as_deref(),
        ),
        created_at: ms_to_dt(row.get("created_at")),
        updated_at: ms_to_dt(row.get("updated_at")),
    }
}

#[async_trait]
impl StateProvider for PostgresProvider {
    async fn init(&self) -> Result<()> {
        // Embedded migrations (baked in at compile time from ./migrations/postgres).
        // sqlx tracks applied versions in `_sqlx_migrations` and applies only what
        // is pending, so this is safe on every startup and upgrades existing DBs.
        sqlx::migrate!("./migrations/postgres")
            .run(&self.pool)
            .await?;
        // Start the LISTEN/NOTIFY listener once (it powers await_change). Spawned
        // here so it only runs for a provider that has been brought up; cancelled
        // when the provider is dropped. `Relaxed` suffices: this is purely a
        // spawn-once guard — the task's inputs are moved in via `spawn` (which
        // carries its own happens-before), so the flag publishes no other memory.
        if !self.listener_started.swap(true, Ordering::Relaxed) {
            tokio::spawn(run_listener(
                self.pool.clone(),
                self.notify_hub.clone(),
                self.listener_token.clone(),
            ));
        }
        Ok(())
    }

    fn supports_listen_notify(&self) -> bool {
        true
    }

    async fn await_change(&self, wait: ChangeWait<'_>, within: Duration) {
        // Subscribe before awaiting so a NOTIFY arriving during the wait wakes us;
        // a NOTIFY in the gap before subscribing is covered by `within` (the
        // caller re-checks the database on return).
        let sub = self
            .notify_hub
            .subscribe(hub_key(wait.channel(), &wait.payload()));
        let _ = tokio::time::timeout(within, sub.notify.notified()).await;
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<WorkflowStatus> {
        // Idempotent create: an existing id is left untouched.
        sqlx::query(
            "INSERT INTO workflow_status
                 (workflow_uuid, name, inputs, status, executor_id, application_version,
                  queue_name, queue_partition_key, priority, deduplication_id, parent_workflow_id,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, delay_until_epoch_ms,
                  authenticated_user, assumed_role, authenticated_roles,
                  serialization, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17, $18, $19, $20)
             ON CONFLICT (workflow_uuid) DO NOTHING",
        )
        .bind(&s.id)
        .bind(&s.name)
        .bind(serialize::encode_input(self.serializer, &s.input)?)
        .bind(&s.status)
        .bind(&s.executor_id)
        .bind(&s.app_version)
        .bind(&s.queue_name)
        .bind(&s.queue_partition_key)
        .bind(s.priority)
        .bind(&s.dedup_id)
        .bind(&s.parent_workflow_id)
        .bind(s.timeout_ms)
        .bind(s.deadline_ms)
        .bind(s.delay_until_ms)
        .bind(&s.authenticated_user)
        .bind(&s.assumed_role)
        .bind(encode_roles(&s.authenticated_roles))
        .bind(self.serializer.name())
        .bind(s.created_at.timestamp_millis())
        .bind(s.updated_at.timestamp_millis())
        .execute(&self.pool)
        .await
        .map_err(|e| dedup_or(e, &s))?;

        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE workflow_uuid = $1"
        ))
        .bind(&s.id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row_to_status(&row))
    }

    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT workflow_uuid FROM workflow_status \
             WHERE queue_name = $1 AND deduplication_id = $2 LIMIT 1",
        )
        .bind(queue_name)
        .bind(dedup_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("workflow_uuid")))
    }

    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE workflow_uuid = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| row_to_status(&r)))
    }

    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<()> {
        let output_str = output.map(|v| self.serializer.encode(v)).transpose()?;
        let now = Utc::now().timestamp_millis();
        let terminal = is_terminal(status);
        let completed = terminal.then_some(now);
        // A workflow cancelled during its final step must stay cancelled: a
        // SUCCESS/ERROR completion is not allowed to overwrite a CANCELLED row.
        let is_completion = status == STATUS_SUCCESS || status == STATUS_ERROR;
        // Reaching a terminal state frees the queue-scoped deduplication slot so
        // the same deduplication id can be enqueued again.
        let res = sqlx::query(
            "UPDATE workflow_status
             SET status = $2,
                 output = COALESCE($3, output),
                 error  = COALESCE($4, error),
                 completed_at = COALESCE($5, completed_at),
                 deduplication_id = CASE WHEN $7 THEN NULL ELSE deduplication_id END,
                 updated_at = $6
             WHERE workflow_uuid = $1 AND NOT (status = $8 AND $9)",
        )
        .bind(id)
        .bind(status)
        .bind(output_str)
        .bind(error)
        .bind(completed)
        .bind(now)
        .bind(terminal)
        .bind(STATUS_CANCELLED)
        .bind(is_completion)
        .execute(&self.pool)
        .await?;
        // If the completion was blocked because the workflow was already
        // cancelled, surface the cancellation rather than reporting success.
        if is_completion && res.rows_affected() == 0 {
            if let Some(w) = self.get_workflow_status(id).await? {
                if w.status == STATUS_CANCELLED {
                    return Err(Error::Cancelled(id.to_string()));
                }
            }
        }
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT output, serialization FROM operation_outputs
             WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(r) => serialize::decode_opt(
                r.get::<Option<String>, _>("serialization").as_deref(),
                r.get::<Option<String>, _>("output").as_deref(),
            ),
            None => Ok(None),
        }
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
        started_at_ms: Option<i64>,
    ) -> Result<Value> {
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, output, serialization,
                  started_at_epoch_ms, completed_at_epoch_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(self.serializer.encode(&value)?)
        .bind(self.serializer.name())
        .bind(started_at_ms)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;

        // Read back the canonical value (ours, or a racing writer's that won).
        let row = sqlx::query(
            "SELECT output, serialization FROM operation_outputs
             WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;
        Ok(serialize::decode_opt(
            row.get::<Option<String>, _>("serialization").as_deref(),
            row.get::<Option<String>, _>("output").as_deref(),
        )?
        .unwrap_or(Value::Null))
    }

    async fn run_transaction_step(
        &self,
        workflow_id: &str,
        seq: i32,
        started_at_ms: i64,
        opts: &TransactionOptions,
        body: TxBody<'_>,
    ) -> Result<Value> {
        let name = opts.name.as_str();
        const MAX_ATTEMPTS: u32 = 10;
        let mut attempt: u32 = 0;
        loop {
            // One attempt: a fresh transaction. A serialization/deadlock conflict
            // anywhere here aborts it and we retry on a new one.
            let outcome = async {
                let mut tx = self.pool.begin().await?;
                // `SET TRANSACTION` must come before any query in the tx.
                if opts.isolation != IsolationLevel::ReadCommitted || opts.read_only {
                    let mut stmt = format!(
                        "SET TRANSACTION ISOLATION LEVEL {}",
                        opts.isolation.pg_sql()
                    );
                    if opts.read_only {
                        stmt.push_str(" READ ONLY");
                    }
                    sqlx::query(&stmt).execute(&mut *tx).await?;
                }
                // Replay: if this step already committed, return its recorded
                // output and do not run the body.
                if let Some(r) = sqlx::query(
                    "SELECT output, serialization FROM operation_outputs
                     WHERE workflow_uuid = $1 AND function_id = $2",
                )
                .bind(workflow_id)
                .bind(seq)
                .fetch_optional(&mut *tx)
                .await?
                {
                    return Ok(serialize::decode_opt(
                        r.get::<Option<String>, _>("serialization").as_deref(),
                        r.get::<Option<String>, _>("output").as_deref(),
                    )?
                    .unwrap_or(Value::Null));
                }
                // Run the user's body against this transaction. On error we
                // return, dropping `tx` (rollback), so nothing it wrote persists.
                let value = {
                    let mut h = Tx::postgres(&mut tx);
                    body(&mut h).await?
                };
                // Checkpoint in the same transaction, then commit atomically.
                sqlx::query(
                    "INSERT INTO operation_outputs
                         (workflow_uuid, function_id, function_name, output, serialization,
                          started_at_epoch_ms, completed_at_epoch_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
                )
                .bind(workflow_id)
                .bind(seq)
                .bind(name)
                .bind(self.serializer.encode(&value)?)
                .bind(self.serializer.name())
                .bind(started_at_ms)
                .bind(Utc::now().timestamp_millis())
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok::<Value, Error>(value)
            }
            .await;

            match outcome {
                Ok(v) => return Ok(v),
                Err(e) if e.is_tx_conflict() && attempt + 1 < MAX_ATTEMPTS => {
                    attempt += 1;
                    let ms = (1u64 << attempt.min(6)).min(200);
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;

        // Snapshot isolation is only required for global concurrency or rate
        // limiting, where the COUNT and the candidate scan must see a consistent
        // view across concurrent dispatchers; worker concurrency alone is
        // enforced in-process, so READ COMMITTED suffices. This must run before
        // any query in the transaction. (Mirrors Go's QueueDequeueIsolation.)
        if req.global_concurrency.is_some() || req.rate_limit_max.is_some() {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .execute(&mut *tx)
                .await?;
        }

        let mut max_tasks = req.max_tasks;

        // A partitioned queue scopes every count and the candidate scan to one
        // partition key; a non-partitioned queue (`None`) leaves them unscoped.
        let part = req.partition_key.as_deref();

        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let part_clause = if part.is_some() {
                " AND queue_partition_key = $5"
            } else {
                ""
            };
            let sql = format!(
                "SELECT COUNT(*) FROM workflow_status
                 WHERE queue_name = $1 AND rate_limited = TRUE
                   AND status NOT IN ($2, $3) AND started_at_epoch_ms > $4{part_clause}"
            );
            let mut q = sqlx::query_scalar(&sql)
                .bind(&req.queue_name)
                .bind(STATUS_ENQUEUED)
                .bind(STATUS_DELAYED)
                .bind(now_ms - period_ms);
            if let Some(p) = part {
                q = q.bind(p);
            }
            let recent: i64 = q.fetch_one(&mut *tx).await?;
            max_tasks = max_tasks.min((limit - recent).max(0));
        }

        if let Some(global) = req.global_concurrency {
            let part_clause = if part.is_some() {
                " AND queue_partition_key = $3"
            } else {
                ""
            };
            let sql = format!(
                "SELECT COUNT(*) FROM workflow_status WHERE queue_name = $1 AND status = $2{part_clause}"
            );
            let mut q = sqlx::query_scalar(&sql)
                .bind(&req.queue_name)
                .bind(STATUS_PENDING);
            if let Some(p) = part {
                q = q.bind(p);
            }
            let pending: i64 = q.fetch_one(&mut *tx).await?;
            max_tasks = max_tasks.min((global - pending).max(0));
        }

        if max_tasks <= 0 {
            return Ok(Vec::new());
        }

        // SKIP LOCKED lets concurrent dispatchers claim disjoint sets without
        // blocking; with a global concurrency cap, NOWAIT instead surfaces
        // contention so the counts above stay consistent.
        let lock = if req.global_concurrency.is_none() {
            "FOR UPDATE SKIP LOCKED"
        } else {
            "FOR UPDATE NOWAIT"
        };
        // With a partition key the clause takes $4 and LIMIT moves to $5.
        let (part_clause, limit_ph) = if part.is_some() {
            (" AND queue_partition_key = $4", "$5")
        } else {
            ("", "$4")
        };
        let sql = format!(
            "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = $1 AND status = $2
               AND (application_version = $3 OR application_version = ''){part_clause}
             ORDER BY priority ASC, created_at ASC
             {lock} LIMIT {limit_ph}"
        );
        let mut q = sqlx::query_scalar(&sql)
            .bind(&req.queue_name)
            .bind(STATUS_ENQUEUED)
            .bind(&req.app_version);
        if let Some(p) = part {
            q = q.bind(p);
        }
        let ids: Vec<String> = q.bind(max_tasks).fetch_all(&mut *tx).await?;

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(&format!(
            "UPDATE workflow_status
             SET status = $1, executor_id = $2, application_version = $3,
                 started_at_epoch_ms = $4, rate_limited = $5, updated_at = $4,
                 workflow_deadline_epoch_ms = CASE
                     WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                     THEN $4 + workflow_timeout_ms
                     ELSE workflow_deadline_epoch_ms
                 END
             WHERE workflow_uuid = ANY($6) AND status = $7
             RETURNING {SELECT_COLS}"
        ))
        .bind(STATUS_PENDING)
        .bind(&req.executor_id)
        .bind(&req.app_version)
        .bind(now_ms)
        .bind(req.rate_limit_max.is_some())
        .bind(&ids)
        .bind(STATUS_ENQUEUED)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(rows.iter().map(row_to_status).collect())
    }

    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE workflow_status
             SET status = $1, delay_until_epoch_ms = NULL, updated_at = $2
             WHERE status = $3 AND delay_until_epoch_ms <= $2",
        )
        .bind(STATUS_ENQUEUED)
        .bind(now_ms)
        .bind(STATUS_DELAYED)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>> {
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT queue_partition_key FROM workflow_status
             WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL",
        )
        .bind(queue_name)
        .bind(STATUS_ENQUEUED)
        .fetch_all(&self.pool)
        .await?;
        Ok(keys)
    }

    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
    ) -> Result<()> {
        // The FK on destination_uuid rejects sends to nonexistent workflows.
        sqlx::query(
            "INSERT INTO notifications
                 (message_uuid, destination_uuid, topic, message, serialization, created_at_epoch_ms)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(destination_id)
        .bind(topic)
        .bind(self.serializer.encode(&message)?)
        .bind(self.serializer.name())
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await
        .map_err(|e| nonexistent_or(e, destination_id))?;
        Ok(())
    }

    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>> {
        // Claim and checkpoint in one transaction: a crash between them would
        // otherwise lose the message. message_uuid pins exactly one row even
        // when several messages share a created_at millisecond.
        let mut tx = self.pool.begin().await?;

        let claimed: Option<(String, Option<String>)> = sqlx::query_as(
            "WITH oldest_entry AS (
                 SELECT message_uuid FROM notifications
                 WHERE destination_uuid = $1 AND topic = $2 AND consumed = FALSE
                 ORDER BY created_at_epoch_ms ASC
                 LIMIT 1
             )
             UPDATE notifications SET consumed = TRUE
             WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
             RETURNING message, serialization",
        )
        .bind(workflow_id)
        .bind(topic)
        .fetch_optional(&mut *tx)
        .await?;

        let Some((message, fmt)) = claimed else {
            return Ok(None);
        };

        // Checkpoint the consumed message verbatim, keeping its format so a
        // replay decodes it the same way.
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, output, serialization)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(step_name)
        .bind(&message)
        .bind(&fmt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(serialize::decode(fmt.as_deref(), &message)?))
    }

    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO workflow_events (workflow_uuid, key, value, serialization)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(self.serializer.encode(&value)?)
        .bind(self.serializer.name())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT value, serialization FROM workflow_events
             WHERE workflow_uuid = $1 AND key = $2",
        )
        .bind(workflow_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((value, fmt)) => Ok(Some(serialize::decode(fmt.as_deref(), &value)?)),
            None => Ok(None),
        }
    }

    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        let cols = list_select_cols(filter);
        let mut qb: QueryBuilder<Postgres> =
            QueryBuilder::new(format!("SELECT {cols} FROM workflow_status"));
        push_list_filters(&mut qb, filter);
        qb.push(if filter.sort_desc {
            " ORDER BY created_at DESC"
        } else {
            " ORDER BY created_at ASC"
        });
        if let Some(lim) = filter.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }
        if let Some(off) = filter.offset {
            qb.push(" OFFSET ").push_bind(off);
        }
        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows.iter().map(row_to_status).collect())
    }

    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        let cols = query.enabled_columns();
        let bucket = query.time_bucket_ms.filter(|b| *b > 0);

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        for (_, col) in &cols {
            qb.push(*col).push(", ");
        }
        if let Some(b) = bucket {
            qb.push("(created_at / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b)
                .push(" AS time_bucket, ");
        }
        qb.push("COUNT(*) AS cnt FROM workflow_status");
        push_agg_filters(&mut qb, query);
        qb.push(" GROUP BY ");
        let mut first = true;
        for (_, col) in &cols {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push(*col);
        }
        if bucket.is_some() {
            if !first {
                qb.push(", ");
            }
            qb.push("time_bucket");
        }
        if let Some(lim) = query.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }

        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| row_to_aggregate(r, &cols, bucket.is_some()))
            .collect())
    }

    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>> {
        let dims = query.group_exprs();
        let bucket = query.time_bucket_ms.filter(|b| *b > 0);

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        for (key, expr) in &dims {
            qb.push(*expr).push(" AS ").push(*key).push(", ");
        }
        if let Some(b) = bucket {
            qb.push("(completed_at_epoch_ms / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b)
                .push(" AS time_bucket, ");
        }
        let mut sel = Vec::new();
        if query.select_count {
            sel.push("COUNT(*) AS cnt");
        }
        if query.select_max_duration_ms {
            sel.push("MAX(completed_at_epoch_ms - started_at_epoch_ms) AS max_dur");
        }
        qb.push(sel.join(", "));
        qb.push(" FROM operation_outputs");
        push_step_agg_filters(&mut qb, query);
        qb.push(" GROUP BY ");
        let mut first = true;
        for (_, expr) in &dims {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push(*expr);
        }
        if let Some(b) = bucket {
            if !first {
                qb.push(", ");
            }
            qb.push("(completed_at_epoch_ms / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b);
        }
        if let Some(lim) = query.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }

        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| {
                row_to_step_aggregate(
                    r,
                    &dims,
                    bucket.is_some(),
                    query.select_count,
                    query.select_max_duration_ms,
                )
            })
            .collect())
    }

    async fn cancel_workflow(&self, id: &str) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE workflow_status
             SET status = $2, completed_at = $3, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL, updated_at = $3
             WHERE workflow_uuid = $1 AND status NOT IN ($4, $5, $6)",
        )
        .bind(id)
        .bind(STATUS_CANCELLED)
        .bind(now)
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .bind(STATUS_CANCELLED)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resume_workflow(&self, id: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workflow_status
             SET status = $2, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                 updated_at = $3
             WHERE workflow_uuid = $1 AND status NOT IN ($4, $5)",
        )
        .bind(id)
        .bind(STATUS_PENDING)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()> {
        sqlx::query(
            "UPDATE workflow_status
             SET status = $2, queue_name = $3, executor_id = '',
                 started_at_epoch_ms = NULL, updated_at = $4
             WHERE workflow_uuid = $1",
        )
        .bind(id)
        .bind(STATUS_ENQUEUED)
        .bind(queue)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE workflow_status
             SET status = $2, completed_at = $3, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL, updated_at = $3
             WHERE workflow_uuid = ANY($1) AND status NOT IN ($4, $5, $6)",
        )
        .bind(ids)
        .bind(STATUS_CANCELLED)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .bind(STATUS_CANCELLED)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let resumed: Vec<String> = sqlx::query_scalar(
            "UPDATE workflow_status
             SET status = $2, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                 updated_at = $3
             WHERE workflow_uuid = ANY($1) AND status NOT IN ($4, $5)
             RETURNING workflow_uuid",
        )
        .bind(ids)
        .bind(STATUS_PENDING)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .fetch_all(&self.pool)
        .await?;
        Ok(resumed)
    }

    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // ON DELETE CASCADE removes each workflow's step / event / stream rows.
        if delete_children {
            sqlx::query(
                "WITH RECURSIVE targets AS (
                     SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ANY($1)
                     UNION
                     SELECT w.workflow_uuid FROM workflow_status w
                       JOIN targets t ON w.parent_workflow_id = t.workflow_uuid
                 )
                 DELETE FROM workflow_status
                 WHERE workflow_uuid IN (SELECT workflow_uuid FROM targets)",
            )
            .bind(ids)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query("DELETE FROM workflow_status WHERE workflow_uuid = ANY($1)")
                .bind(ids)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workflow_status SET delay_until_epoch_ms = $2, updated_at = $3
             WHERE workflow_uuid = $1 AND status = $4",
        )
        .bind(id)
        .bind(delay_until_ms)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_DELAYED)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn fork_workflow(
        &self,
        original_id: &str,
        new_id: &str,
        start_step: i32,
        app_version: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let now = Utc::now().timestamp_millis();

        let inserted = sqlx::query(
            "INSERT INTO workflow_status
                 (workflow_uuid, status, name, inputs, serialization, executor_id,
                  application_version, forked_from, recovery_attempts,
                  authenticated_user, assumed_role, authenticated_roles, created_at, updated_at)
             SELECT $1, $2, name, inputs, serialization, '', $3, $4, 0,
                    authenticated_user, assumed_role, authenticated_roles, $5, $5
             FROM workflow_status WHERE workflow_uuid = $4",
        )
        .bind(new_id)
        .bind(STATUS_PENDING)
        .bind(app_version)
        .bind(original_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(crate::error::Error::app(format!(
                "cannot fork nonexistent workflow `{original_id}`"
            )));
        }

        sqlx::query("UPDATE workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = $1")
            .bind(original_id)
            .execute(&mut *tx)
            .await?;

        if start_step > 0 {
            sqlx::query(
                "INSERT INTO operation_outputs
                     (workflow_uuid, function_id, function_name, output, error,
                      child_workflow_id, serialization)
                 SELECT $1, function_id, function_name, output, error,
                        child_workflow_id, serialization
                 FROM operation_outputs WHERE workflow_uuid = $2 AND function_id < $3",
            )
            .bind(new_id)
            .bind(original_id)
            .bind(start_step)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn bump_recovery_attempts(&self, id: &str, max: i32) -> Result<i32> {
        let mut tx = self.pool.begin().await?;
        let attempts: Option<i64> = sqlx::query_scalar(
            "UPDATE workflow_status SET recovery_attempts = recovery_attempts + 1, updated_at = $2
             WHERE workflow_uuid = $1 RETURNING recovery_attempts",
        )
        .bind(id)
        .bind(Utc::now().timestamp_millis())
        .fetch_optional(&mut *tx)
        .await?;
        let attempts = attempts.unwrap_or(0) as i32;
        if attempts > max {
            sqlx::query(
                "UPDATE workflow_status SET status = $2, deduplication_id = NULL \
                 WHERE workflow_uuid = $1",
            )
            .bind(id)
            .bind(STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(attempts)
    }

    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, child_workflow_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(parent_id)
        .bind(seq)
        .bind(name)
        .bind(child_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn check_child_workflow(&self, parent_id: &str, seq: i32) -> Result<Option<String>> {
        let child: Option<Option<String>> = sqlx::query_scalar(
            "SELECT child_workflow_id FROM operation_outputs
             WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(parent_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(child.flatten())
    }

    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let rows = sqlx::query(
            "SELECT function_id, function_name, output, error, child_workflow_id,
                    started_at_epoch_ms, completed_at_epoch_ms, serialization
             FROM operation_outputs
             WHERE workflow_uuid = $1
             ORDER BY function_id ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_to_step).collect()
    }

    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>> {
        let name: Option<String> = sqlx::query_scalar(
            "SELECT function_name FROM operation_outputs
             WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(name)
    }

    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name)
             VALUES ($1, $2, $3)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        function_id: i32,
    ) -> Result<()> {
        // A closed entry stores the sentinel verbatim with no serialization;
        // user values are encoded and tagged with the serializer name.
        let (stored, ser): (String, Option<String>) = match value {
            Some(v) => (
                self.serializer.encode(&v)?,
                Some(self.serializer.name().to_string()),
            ),
            None => (STREAM_CLOSED_SENTINEL.to_string(), None),
        };

        // Check-then-append in one transaction so the next offset cannot be
        // claimed by a concurrent writer.
        let mut tx = self.pool.begin().await?;

        let closed: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM streams WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(STREAM_CLOSED_SENTINEL)
        .fetch_optional(&mut *tx)
        .await?;
        if closed.is_some() {
            return Err(crate::error::Error::app(format!(
                "stream `{key}` is already closed"
            )));
        }

        sqlx::query(
            "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
             SELECT $1, $2, $3, COALESCE(
                 (SELECT MAX(\"offset\") FROM streams WHERE workflow_uuid = $1 AND key = $2), -1
             ) + 1, $4, $5",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(&stored)
        .bind(function_id)
        .bind(&ser)
        .execute(&mut *tx)
        .await
        .map_err(|e| nonexistent_or(e, workflow_id))?;

        tx.commit().await?;
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT value, serialization FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3
             ORDER BY \"offset\" ASC",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(from_offset)
        .fetch_all(&self.pool)
        .await?;

        let mut values = Vec::with_capacity(rows.len());
        let mut closed = false;
        for (value, fmt) in rows {
            if value == STREAM_CLOSED_SENTINEL {
                closed = true;
                break;
            }
            values.push(serialize::decode(fmt.as_deref(), &value)?);
        }
        Ok((values, closed))
    }

    async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT key, value, serialization FROM workflow_events
             WHERE workflow_uuid = $1 ORDER BY key ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(key, value, fmt)| Ok((key, serialize::decode(fmt.as_deref(), &value)?)))
            .collect()
    }

    async fn list_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationInfo>> {
        let rows: Vec<(String, String, Option<String>, i64, bool)> = sqlx::query_as(
            "SELECT topic, message, serialization, created_at_epoch_ms, consumed
             FROM notifications WHERE destination_uuid = $1
             ORDER BY created_at_epoch_ms ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(topic, message, fmt, created_at_ms, consumed)| {
                Ok(NotificationInfo {
                    topic: (!topic.is_empty()).then_some(topic),
                    message: serialize::decode(fmt.as_deref(), &message)?,
                    created_at_ms,
                    consumed,
                })
            })
            .collect()
    }

    async fn list_workflow_streams(&self, workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT key, value, serialization FROM streams
             WHERE workflow_uuid = $1 ORDER BY key ASC, \"offset\" ASC",
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        group_stream_rows(rows)
    }

    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()> {
        sqlx::query(
            "INSERT INTO workflow_schedules (
                 schedule_id, schedule_name, workflow_name, schedule, status, context,
                 last_fired_at, automatic_backfill, cron_timezone, queue_name
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&schedule.schedule_id)
        .bind(&schedule.schedule_name)
        .bind(&schedule.workflow_name)
        .bind(&schedule.schedule)
        .bind(schedule.status.as_str())
        .bind(encode_schedule_context(&schedule.context))
        .bind(schedule.last_fired_at.map(|t| t.to_rfc3339()))
        .bind(schedule.automatic_backfill)
        .bind(&schedule.cron_timezone)
        .bind(&schedule.queue_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for s in schedules {
            sqlx::query("DELETE FROM workflow_schedules WHERE schedule_name = $1")
                .bind(&s.schedule_name)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "INSERT INTO workflow_schedules (
                     schedule_id, schedule_name, workflow_name, schedule, status, context,
                     last_fired_at, automatic_backfill, cron_timezone, queue_name
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .bind(&s.schedule_id)
            .bind(&s.schedule_name)
            .bind(&s.workflow_name)
            .bind(&s.schedule)
            .bind(s.status.as_str())
            .bind(encode_schedule_context(&s.context))
            .bind(s.last_fired_at.map(|t| t.to_rfc3339()))
            .bind(s.automatic_backfill)
            .bind(&s.cron_timezone)
            .bind(&s.queue_name)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        let mut qb = QueryBuilder::new(
            "SELECT schedule_id, schedule_name, workflow_name, schedule, status, context, \
             last_fired_at, automatic_backfill, cron_timezone, queue_name FROM workflow_schedules",
        );
        let mut sep = " WHERE ";
        if !filter.statuses.is_empty() {
            let statuses: Vec<String> = filter
                .statuses
                .iter()
                .map(|s| s.as_str().to_string())
                .collect();
            qb.push(sep)
                .push("status = ANY(")
                .push_bind(statuses)
                .push(")");
            sep = " AND ";
        }
        if !filter.workflow_names.is_empty() {
            qb.push(sep)
                .push("workflow_name = ANY(")
                .push_bind(filter.workflow_names.clone())
                .push(")");
            sep = " AND ";
        }
        if !filter.name_prefixes.is_empty() {
            let patterns: Vec<String> = filter
                .name_prefixes
                .iter()
                .map(|p| format!("{p}%"))
                .collect();
            qb.push(sep)
                .push("schedule_name LIKE ANY(")
                .push_bind(patterns)
                .push(")");
        }
        qb.push(" ORDER BY schedule_name ASC");

        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(row_to_schedule).collect()
    }

    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool> {
        let res = sqlx::query("UPDATE workflow_schedules SET status = $1 WHERE schedule_name = $2")
            .bind(status.as_str())
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()> {
        let at = DateTime::from_timestamp_millis(at_ms).map(|t| t.to_rfc3339());
        sqlx::query("UPDATE workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2")
            .bind(at)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM workflow_schedules WHERE schedule_name = $1")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn create_application_version(&self, version_name: &str) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "INSERT INTO application_versions \
             (version_id, version_name, version_timestamp, created_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (version_name) DO NOTHING",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(version_name)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let rows = sqlx::query(
            "SELECT version_id, version_name, version_timestamp, created_at \
             FROM application_versions ORDER BY version_timestamp DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_version).collect())
    }

    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        let row = sqlx::query(
            "SELECT version_id, version_name, version_timestamp, created_at \
             FROM application_versions ORDER BY version_timestamp DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_to_version))
    }

    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE application_versions SET version_timestamp = $1 WHERE version_name = $2",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(version_name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>> {
        let mut tx = self.pool.begin().await?;

        // Root first, then transitive children discovered through parent_workflow_id.
        let mut ids = vec![workflow_id.to_string()];
        if export_children {
            let mut queue = vec![workflow_id.to_string()];
            while let Some(parent) = queue.pop() {
                let children: Vec<(String,)> = sqlx::query_as(
                    "SELECT workflow_uuid FROM workflow_status \
                     WHERE parent_workflow_id = $1 ORDER BY workflow_uuid ASC",
                )
                .bind(&parent)
                .fetch_all(&mut *tx)
                .await?;
                for (id,) in children {
                    ids.push(id.clone());
                    queue.push(id);
                }
            }
        }

        let mut exported = Vec::with_capacity(ids.len());
        for id in &ids {
            let status_row = sqlx::query("SELECT * FROM workflow_status WHERE workflow_uuid = $1")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
            let Some(status_row) = status_row else {
                return Err(Error::nonexistent_workflow(id));
            };
            let workflow_status = export_status_map(&status_row);

            let op_rows = sqlx::query(
                "SELECT * FROM operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id ASC",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let operation_outputs = op_rows.iter().map(export_op_map).collect();

            let event_rows = sqlx::query(
                "SELECT * FROM workflow_events WHERE workflow_uuid = $1 ORDER BY key ASC",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let workflow_events = event_rows.iter().map(export_event_map).collect();

            let history_rows = sqlx::query(
                "SELECT * FROM workflow_events_history WHERE workflow_uuid = $1 \
                 ORDER BY function_id ASC, key ASC",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let workflow_events_history = history_rows.iter().map(export_history_map).collect();

            let stream_rows = sqlx::query(
                "SELECT * FROM streams WHERE workflow_uuid = $1 ORDER BY key ASC, \"offset\" ASC",
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let streams = stream_rows.iter().map(export_stream_map).collect();

            exported.push(ExportedWorkflow {
                workflow_status,
                operation_outputs,
                workflow_events,
                workflow_events_history,
                streams,
            });
        }

        tx.commit().await?;
        Ok(exported)
    }

    async fn import_workflow(&self, workflows: &[ExportedWorkflow]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for wf in workflows {
            let s = &wf.workflow_status;
            sqlx::query(
                "INSERT INTO workflow_status
                     (workflow_uuid, status, name, authenticated_user, assumed_role,
                      authenticated_roles, output, error, executor_id, created_at, updated_at,
                      application_version, application_id, class_name, config_name,
                      recovery_attempts, queue_name, workflow_timeout_ms,
                      workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs,
                      priority, queue_partition_key, forked_from, parent_workflow_id,
                      delay_until_epoch_ms, serialization, was_forked_from)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                         $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)",
            )
            .bind(col_str(s, "workflow_uuid"))
            .bind(col_str(s, "status"))
            .bind(col_str(s, "name"))
            .bind(col_str(s, "authenticated_user"))
            .bind(col_str(s, "assumed_role"))
            .bind(col_str(s, "authenticated_roles"))
            .bind(col_str(s, "output"))
            .bind(col_str(s, "error"))
            .bind(col_str(s, "executor_id"))
            .bind(col_i64(s, "created_at"))
            .bind(col_i64(s, "updated_at"))
            .bind(col_str(s, "application_version"))
            .bind(col_str(s, "application_id"))
            .bind(col_str(s, "class_name"))
            .bind(col_str(s, "config_name"))
            .bind(col_i64(s, "recovery_attempts"))
            .bind(col_str(s, "queue_name"))
            .bind(col_i64(s, "workflow_timeout_ms"))
            .bind(col_i64(s, "workflow_deadline_epoch_ms"))
            .bind(col_i64(s, "started_at_epoch_ms"))
            .bind(col_str(s, "deduplication_id"))
            .bind(col_str(s, "inputs"))
            .bind(col_i32(s, "priority"))
            .bind(col_str(s, "queue_partition_key"))
            .bind(col_str(s, "forked_from"))
            .bind(col_str(s, "parent_workflow_id"))
            .bind(col_i64(s, "delay_until_epoch_ms"))
            .bind(col_str(s, "serialization"))
            .bind(col_str(s, "forked_from").is_some())
            .execute(&mut *tx)
            .await?;

            for op in &wf.operation_outputs {
                sqlx::query(
                    "INSERT INTO operation_outputs
                         (workflow_uuid, function_id, function_name, output, error,
                          child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(col_str(op, "workflow_uuid"))
                .bind(col_i32(op, "function_id"))
                .bind(col_str(op, "function_name"))
                .bind(col_str(op, "output"))
                .bind(col_str(op, "error"))
                .bind(col_str(op, "child_workflow_id"))
                .bind(col_i64(op, "started_at_epoch_ms"))
                .bind(col_i64(op, "completed_at_epoch_ms"))
                .execute(&mut *tx)
                .await?;
            }

            for ev in &wf.workflow_events {
                sqlx::query(
                    "INSERT INTO workflow_events (workflow_uuid, key, value) VALUES ($1, $2, $3)",
                )
                .bind(col_str(ev, "workflow_uuid"))
                .bind(col_str(ev, "key"))
                .bind(col_str(ev, "value"))
                .execute(&mut *tx)
                .await?;
            }

            for h in &wf.workflow_events_history {
                sqlx::query(
                    "INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value)
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(col_str(h, "workflow_uuid"))
                .bind(col_i32(h, "function_id"))
                .bind(col_str(h, "key"))
                .bind(col_str(h, "value"))
                .execute(&mut *tx)
                .await?;
            }

            for st in &wf.streams {
                sqlx::query(
                    "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(col_str(st, "workflow_uuid"))
                .bind(col_str(st, "key"))
                .bind(col_str(st, "value"))
                .bind(col_i32(st, "offset"))
                .bind(col_i32(st, "function_id"))
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

/// An exported column read as an `i32` (Postgres `INTEGER`), matching the
/// narrower columns (`priority`, `function_id`, `offset`).
fn col_i32(m: &Map<String, Value>, key: &str) -> Option<i32> {
    col_i64(m, key).map(|v| v as i32)
}

/// A `String` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn s_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<String>, _>(key).ok().flatten())
}

/// A `BIGINT` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn i64_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<i64>, _>(key).ok().flatten())
}

/// An `INTEGER` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn i32_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<i32>, _>(key).ok().flatten())
}

fn export_status_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in EXPORT_STATUS_STR_COLS {
        m.insert(c.to_string(), s_col(row, c));
    }
    // All exported status integers are BIGINT except `priority` (INTEGER).
    for &c in &[
        "created_at",
        "updated_at",
        "recovery_attempts",
        "workflow_timeout_ms",
        "workflow_deadline_epoch_ms",
        "started_at_epoch_ms",
        "delay_until_epoch_ms",
    ] {
        m.insert(c.to_string(), i64_col(row, c));
    }
    m.insert("priority".to_string(), i32_col(row, "priority"));
    m
}

fn export_op_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &[
        "workflow_uuid",
        "function_name",
        "output",
        "error",
        "child_workflow_id",
    ] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m.insert("function_id".to_string(), i32_col(row, "function_id"));
    for &c in &["started_at_epoch_ms", "completed_at_epoch_ms"] {
        m.insert(c.to_string(), i64_col(row, c));
    }
    m
}

fn export_event_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m
}

fn export_history_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m.insert("function_id".to_string(), i32_col(row, "function_id"));
    m
}

fn export_stream_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    for &c in &["offset", "function_id"] {
        m.insert(c.to_string(), i32_col(row, c));
    }
    m
}

fn row_to_version(row: &sqlx::postgres::PgRow) -> VersionInfo {
    VersionInfo {
        version_id: row.get("version_id"),
        version_name: row.get("version_name"),
        version_timestamp: ms_to_dt(row.get("version_timestamp")),
        created_at: ms_to_dt(row.get("created_at")),
    }
}

/// Encode a schedule's optional context as the stored JSON text (`null` when
/// absent, matching the cross-SDK `context TEXT NOT NULL` column).
fn encode_schedule_context(context: &Option<Value>) -> String {
    context
        .as_ref()
        .and_then(|v| serde_json::to_string(v).ok())
        .unwrap_or_else(|| "null".to_string())
}

/// Decode the stored context text back to an optional value (`null` -> `None`).
fn decode_schedule_context(text: &str) -> Option<Value> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Null) => None,
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

/// Map a `workflow_schedules` row to a [`WorkflowSchedule`].
fn row_to_schedule(row: &sqlx::postgres::PgRow) -> Result<WorkflowSchedule> {
    let context: String = row
        .try_get("context")
        .unwrap_or_else(|_| "null".to_string());
    let last_fired: Option<String> = row.try_get("last_fired_at").ok().flatten();
    Ok(WorkflowSchedule {
        schedule_id: row.get("schedule_id"),
        schedule_name: row.get("schedule_name"),
        workflow_name: row.get("workflow_name"),
        schedule: row.get("schedule"),
        status: ScheduleStatus::parse(&row.get::<String, _>("status")),
        context: decode_schedule_context(&context),
        last_fired_at: last_fired
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|t| t.with_timezone(&Utc)),
        automatic_backfill: row.try_get("automatic_backfill").unwrap_or(false),
        cron_timezone: row.try_get("cron_timezone").ok().flatten(),
        queue_name: row.try_get("queue_name").ok().flatten(),
    })
}

/// Map an `operation_outputs` row to a [`StepInfo`], decoding `output` per the
/// row's recorded serialization format.
fn row_to_step(row: &sqlx::postgres::PgRow) -> Result<StepInfo> {
    let fmt: Option<String> = row.try_get("serialization").ok().flatten();
    let output: Option<String> = row.try_get("output").ok().flatten();
    Ok(StepInfo {
        step_id: row.get("function_id"),
        name: row.try_get("function_name").unwrap_or_default(),
        output: serialize::decode_opt(fmt.as_deref(), output.as_deref())?,
        error: row.try_get("error").ok().flatten(),
        child_workflow_id: row.try_get("child_workflow_id").ok().flatten(),
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")
            .ok()
            .flatten()
            .map(ms_to_dt),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_epoch_ms")
            .ok()
            .flatten()
            .map(ms_to_dt),
    })
}

/// Append the WHERE clause shared by `list_workflows` (Postgres dialect).
fn push_list_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, filter: &'a ListFilter) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    if !filter.workflow_ids.is_empty() {
        clause(qb);
        qb.push("workflow_uuid = ANY(")
            .push_bind(filter.workflow_ids.clone());
        qb.push(")");
    }
    if let Some(prefix) = &filter.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(name) = &filter.name {
        clause(qb);
        qb.push("name = ").push_bind(name.clone());
    }
    if !filter.status.is_empty() {
        clause(qb);
        qb.push("status = ANY(").push_bind(filter.status.clone());
        qb.push(")");
    }
    if let Some(q) = &filter.queue_name {
        clause(qb);
        qb.push("queue_name = ").push_bind(q.clone());
    }
    if let Some(v) = &filter.app_version {
        clause(qb);
        qb.push("application_version = ").push_bind(v.clone());
    }
    if !filter.executor_ids.is_empty() {
        clause(qb);
        qb.push("executor_id = ANY(")
            .push_bind(filter.executor_ids.clone());
        qb.push(")");
    }
    if let Some(f) = &filter.forked_from {
        clause(qb);
        qb.push("forked_from = ").push_bind(f.clone());
    }
    if let Some(t) = filter.start_time_ms {
        clause(qb);
        qb.push("created_at >= ").push_bind(t);
    }
    if let Some(t) = filter.end_time_ms {
        clause(qb);
        qb.push("created_at <= ").push_bind(t);
    }
    if let Some(t) = filter.completed_after_ms {
        clause(qb);
        qb.push("completed_at >= ").push_bind(t);
    }
    if let Some(t) = filter.completed_before_ms {
        clause(qb);
        qb.push("completed_at <= ").push_bind(t);
    }
    if let Some(t) = filter.dequeued_after_ms {
        clause(qb);
        qb.push("started_at_epoch_ms >= ").push_bind(t);
    }
    if let Some(t) = filter.dequeued_before_ms {
        clause(qb);
        qb.push("started_at_epoch_ms <= ").push_bind(t);
    }
    if let Some(hp) = filter.has_parent {
        clause(qb);
        qb.push(if hp {
            "parent_workflow_id IS NOT NULL"
        } else {
            "parent_workflow_id IS NULL"
        });
    }
    if filter.queues_only {
        clause(qb);
        qb.push("queue_name IS NOT NULL");
    }
}

/// Append the WHERE clause for `get_workflow_aggregates` (Postgres dialect).
fn push_agg_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, q: &'a WorkflowAggregateQuery) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    let mut push_in = |qb: &mut QueryBuilder<'a, Postgres>, col: &str, vals: &'a [String]| {
        if vals.is_empty() {
            return;
        }
        clause(qb);
        qb.push(col).push(" = ANY(").push_bind(vals).push(")");
    };
    push_in(qb, "status", &q.status);
    push_in(qb, "name", &q.name);
    push_in(qb, "application_version", &q.app_version);
    push_in(qb, "executor_id", &q.executor_ids);
    push_in(qb, "queue_name", &q.queue_names);
    if let Some(prefix) = &q.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(t) = q.start_time_ms {
        clause(qb);
        qb.push("created_at >= ").push_bind(t);
    }
    if let Some(t) = q.end_time_ms {
        clause(qb);
        qb.push("created_at <= ").push_bind(t);
    }
}

/// Materialize one `get_workflow_aggregates` group row: read each enabled
/// dimension column (and the computed `time_bucket`) plus the `cnt`.
fn row_to_aggregate(
    row: &sqlx::postgres::PgRow,
    cols: &[(&str, &str)],
    has_bucket: bool,
) -> WorkflowAggregate {
    let mut group: BTreeMap<String, Option<String>> = BTreeMap::new();
    for (key, col) in cols {
        let v: Option<String> = row.try_get(*col).ok().flatten();
        group.insert(key.to_string(), v);
    }
    if has_bucket {
        let b: Option<i64> = row.try_get("time_bucket").ok().flatten();
        group.insert("time_bucket".to_string(), b.map(|x| x.to_string()));
    }
    WorkflowAggregate {
        group,
        count: row.try_get("cnt").unwrap_or(0),
    }
}

/// Append the WHERE clause for `get_step_aggregates` (Postgres dialect).
fn push_step_agg_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, q: &'a StepAggregateQuery) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    if !q.status.is_empty() {
        clause(qb);
        qb.push(STEP_STATUS_EXPR)
            .push(" = ANY(")
            .push_bind(&q.status)
            .push(")");
    }
    if !q.function_name.is_empty() {
        clause(qb);
        qb.push("function_name = ANY(")
            .push_bind(&q.function_name)
            .push(")");
    }
    if let Some(prefix) = &q.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(t) = q.completed_after_ms {
        clause(qb);
        qb.push("completed_at_epoch_ms >= ").push_bind(t);
    }
    if let Some(t) = q.completed_before_ms {
        clause(qb);
        qb.push("completed_at_epoch_ms <= ").push_bind(t);
    }
}

/// Materialize one `get_step_aggregates` group row.
fn row_to_step_aggregate(
    row: &sqlx::postgres::PgRow,
    dims: &[(&str, &str)],
    has_bucket: bool,
    want_count: bool,
    want_duration: bool,
) -> StepAggregate {
    let mut group: BTreeMap<String, Option<String>> = BTreeMap::new();
    for (key, _) in dims {
        let v: Option<String> = row.try_get(*key).ok().flatten();
        group.insert(key.to_string(), v);
    }
    if has_bucket {
        let b: Option<i64> = row.try_get("time_bucket").ok().flatten();
        group.insert("time_bucket".to_string(), b.map(|x| x.to_string()));
    }
    StepAggregate {
        group,
        count: want_count.then(|| row.try_get("cnt").unwrap_or(0)),
        max_duration_ms: want_duration
            .then(|| row.try_get("max_dur").ok().flatten())
            .flatten(),
    }
}

/// The column list for `list_workflows`, substituting `NULL` for `inputs` /
/// `output` the caller opted out of loading, so those payloads are never read.
fn list_select_cols(filter: &ListFilter) -> std::borrow::Cow<'static, str> {
    if filter.load_input && filter.load_output {
        return std::borrow::Cow::Borrowed(SELECT_COLS);
    }
    let mut cols = SELECT_COLS.to_string();
    if !filter.load_input {
        cols = cols.replacen("inputs,", "NULL AS inputs,", 1);
    }
    if !filter.load_output {
        cols = cols.replacen("output,", "NULL AS output,", 1);
    }
    std::borrow::Cow::Owned(cols)
}
