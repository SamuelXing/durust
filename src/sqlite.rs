use crate::error::Result;
use crate::provider::{StateProvider, WorkflowStatus, STATUS_PENDING};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;

const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_deadline_epoch_ms, created_at, updated_at";

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
        deadline_ms: row.try_get("workflow_deadline_epoch_ms").ok().flatten(),
        created_at: ms_to_dt(row.get("created_at")),
        updated_at: ms_to_dt(row.get("updated_at")),
    }
}

#[async_trait]
impl StateProvider for SqliteProvider {
    async fn init(&self) -> Result<()> {
        sqlx::migrate!("./migrations/sqlite").run(&self.pool).await?;
        Ok(())
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<WorkflowStatus> {
        sqlx::query(
            "INSERT INTO workflow_status
                 (workflow_uuid, name, inputs, status, executor_id, application_version,
                  queue_name, priority, deduplication_id, parent_workflow_id,
                  workflow_deadline_epoch_ms, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        .bind(s.deadline_ms)
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
}
