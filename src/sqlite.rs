use crate::error::Result;
use crate::provider::{
    DequeueRequest, StateProvider, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_PENDING,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;

const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_timeout_ms, workflow_deadline_epoch_ms, \
     started_at_epoch_ms, rate_limited, delay_until_epoch_ms, created_at, updated_at";

/// SQLite-backed [`StateProvider`].
///
/// Gives durable, crash-recoverable state without running a database server —
/// the embedded counterpart to [`crate::PostgresProvider`], using the same
/// canonical DBOS schema. A file URL (`sqlite://durust.db`) survives process
/// restarts; `sqlite::memory:` is handy for tests within a single process.
pub struct SqliteProvider {
    pool: SqlitePool,
}

impl SqliteProvider {
    /// Connect using a sqlx SQLite URL, e.g. `sqlite://durust.db` (created if
    /// missing) or `sqlite::memory:`. Foreign keys are enabled on every
    /// connection, as the schema's `ON DELETE CASCADE` relationships require.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }

    /// Build a provider from an existing pool.
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

fn parse_opt(s: Option<String>) -> Option<Value> {
    s.and_then(|t| serde_json::from_str(&t).ok())
}

fn row_to_status(row: &sqlx::sqlite::SqliteRow) -> WorkflowStatus {
    WorkflowStatus {
        id: row.get("workflow_uuid"),
        name: row.get("name"),
        status: row.get("status"),
        input: parse_opt(row.try_get("inputs").ok().flatten()).unwrap_or(Value::Null),
        output: parse_opt(row.try_get("output").ok().flatten()),
        error: row.try_get("error").ok().flatten(),
        executor_id: row.get("executor_id"),
        app_version: row.get("application_version"),
        queue_name: row.try_get("queue_name").ok().flatten(),
        priority: row.get::<i64, _>("priority") as i32,
        dedup_id: row.try_get("deduplication_id").ok().flatten(),
        recovery_attempts: row.get::<i64, _>("recovery_attempts") as i32,
        parent_workflow_id: row.try_get("parent_workflow_id").ok().flatten(),
        timeout_ms: row.try_get("workflow_timeout_ms").ok().flatten(),
        deadline_ms: row.try_get("workflow_deadline_epoch_ms").ok().flatten(),
        started_at_ms: row.try_get("started_at_epoch_ms").ok().flatten(),
        rate_limited: row.get("rate_limited"),
        delay_until_ms: row.try_get("delay_until_epoch_ms").ok().flatten(),
        created_at: ms_to_dt(row.get("created_at")),
        updated_at: ms_to_dt(row.get("updated_at")),
    }
}

#[async_trait]
impl StateProvider for SqliteProvider {
    async fn init(&self) -> Result<()> {
        sqlx::migrate!("./migrations/sqlite")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<WorkflowStatus> {
        sqlx::query(
            "INSERT INTO workflow_status
                 (workflow_uuid, name, inputs, status, executor_id, application_version,
                  queue_name, priority, deduplication_id, parent_workflow_id,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, delay_until_epoch_ms,
                  created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (workflow_uuid) DO NOTHING",
        )
        .bind(&s.id)
        .bind(&s.name)
        .bind(serde_json::to_string(&s.input)?)
        .bind(&s.status)
        .bind(&s.executor_id)
        .bind(&s.app_version)
        .bind(&s.queue_name)
        .bind(s.priority)
        .bind(&s.dedup_id)
        .bind(&s.parent_workflow_id)
        .bind(s.timeout_ms)
        .bind(s.deadline_ms)
        .bind(s.delay_until_ms)
        .bind(s.created_at.timestamp_millis())
        .bind(s.updated_at.timestamp_millis())
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE workflow_uuid = ?"
        ))
        .bind(&s.id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row_to_status(&row))
    }

    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE workflow_uuid = ?"
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
        let output_str = output.map(serde_json::to_string).transpose()?;
        sqlx::query(
            "UPDATE workflow_status
             SET status = ?,
                 output = COALESCE(?, output),
                 error  = COALESCE(?, error),
                 updated_at = ?
             WHERE workflow_uuid = ?",
        )
        .bind(status)
        .bind(output_str)
        .bind(error)
        .bind(Utc::now().timestamp_millis())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT output FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| parse_opt(r.get::<Option<String>, _>("output"))))
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
    ) -> Result<Value> {
        sqlx::query(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(serde_json::to_string(&value)?)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT output FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;
        Ok(parse_opt(row.get::<Option<String>, _>("output")).unwrap_or(Value::Null))
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowStatus>> {
        let rows = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE status = ?"
        ))
        .bind(STATUS_PENDING)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_status).collect())
    }

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        // SQLite serializes writers, so a plain transaction gives us the same
        // claim-once guarantee Postgres gets from FOR UPDATE SKIP LOCKED.
        let mut tx = self.pool.begin().await?;

        let mut max_tasks = req.max_tasks;

        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let recent: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_status
                 WHERE queue_name = ? AND rate_limited = TRUE
                   AND status NOT IN (?, ?) AND started_at_epoch_ms > ?",
            )
            .bind(&req.queue_name)
            .bind(STATUS_ENQUEUED)
            .bind(STATUS_DELAYED)
            .bind(now_ms - period_ms)
            .fetch_one(&mut *tx)
            .await?;
            max_tasks = max_tasks.min((limit - recent).max(0));
        }

        if let Some(global) = req.global_concurrency {
            let pending: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_status WHERE queue_name = ? AND status = ?",
            )
            .bind(&req.queue_name)
            .bind(STATUS_PENDING)
            .fetch_one(&mut *tx)
            .await?;
            max_tasks = max_tasks.min((global - pending).max(0));
        }

        if max_tasks <= 0 {
            return Ok(Vec::new());
        }

        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = ? AND status = ?
               AND (application_version = ? OR application_version = '')
             ORDER BY priority ASC, created_at ASC
             LIMIT ?",
        )
        .bind(&req.queue_name)
        .bind(STATUS_ENQUEUED)
        .bind(&req.app_version)
        .bind(max_tasks)
        .fetch_all(&mut *tx)
        .await?;

        let rate_limited = req.rate_limit_max.is_some();
        let mut claimed = Vec::with_capacity(ids.len());
        for id in &ids {
            let row = sqlx::query(&format!(
                "UPDATE workflow_status
                 SET status = ?, executor_id = ?, application_version = ?,
                     started_at_epoch_ms = ?, rate_limited = ?, updated_at = ?,
                     workflow_deadline_epoch_ms = CASE
                         WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                         THEN ? + workflow_timeout_ms
                         ELSE workflow_deadline_epoch_ms
                     END
                 WHERE workflow_uuid = ? AND status = ?
                 RETURNING {SELECT_COLS}"
            ))
            .bind(STATUS_PENDING)
            .bind(&req.executor_id)
            .bind(&req.app_version)
            .bind(now_ms)
            .bind(rate_limited)
            .bind(now_ms)
            .bind(now_ms)
            .bind(id)
            .bind(STATUS_ENQUEUED)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(r) = row {
                claimed.push(row_to_status(&r));
            }
        }

        tx.commit().await?;
        Ok(claimed)
    }

    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE workflow_status
             SET status = ?, delay_until_epoch_ms = NULL, updated_at = ?
             WHERE status = ? AND delay_until_epoch_ms <= ?",
        )
        .bind(STATUS_ENQUEUED)
        .bind(now_ms)
        .bind(STATUS_DELAYED)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
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
                 (message_uuid, destination_uuid, topic, message, created_at_epoch_ms)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(destination_id)
        .bind(topic)
        .bind(serde_json::to_string(&message)?)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
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
        // otherwise lose the message.
        let mut tx = self.pool.begin().await?;

        let claimed: Option<String> = sqlx::query_scalar(
            "UPDATE notifications SET consumed = TRUE
             WHERE message_uuid = (
                 SELECT message_uuid FROM notifications
                 WHERE destination_uuid = ? AND topic = ? AND consumed = FALSE
                 ORDER BY created_at_epoch_ms ASC
                 LIMIT 1
             )
             RETURNING message",
        )
        .bind(workflow_id)
        .bind(topic)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(message) = claimed else {
            return Ok(None);
        };

        sqlx::query(
            "INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(step_name)
        .bind(&message)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(serde_json::from_str(&message).ok())
    }

    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO workflow_events (workflow_uuid, key, value) VALUES (?, ?, ?)
             ON CONFLICT (workflow_uuid, key) DO UPDATE SET value = excluded.value",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(serde_json::to_string(&value)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>> {
        let row: Option<String> = sqlx::query_scalar(
            "SELECT value FROM workflow_events WHERE workflow_uuid = ? AND key = ?",
        )
        .bind(workflow_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|v| serde_json::from_str(&v).ok()))
    }
}
