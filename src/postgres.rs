use crate::error::Result;
use crate::provider::{
    DequeueRequest, StateProvider, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_PENDING,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

/// Columns selected when materializing a [`WorkflowStatus`] from `workflow_status`.
const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_timeout_ms, workflow_deadline_epoch_ms, \
     started_at_epoch_ms, rate_limited, delay_until_epoch_ms, created_at, updated_at";

/// Postgres-backed [`StateProvider`], built on sqlx and the canonical DBOS
/// schema (`workflow_status` / `operation_outputs`).
pub struct PostgresProvider {
    pool: PgPool,
}

impl PostgresProvider {
    /// Connect to Postgres using a standard connection URL, e.g.
    /// `postgres://user:pass@localhost:5432/durust`.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Build a provider from an existing pool (useful if your app already owns one).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Epoch-millis (as stored) → `DateTime<Utc>`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

/// Parse a stored TEXT column into a JSON [`Value`], if present.
fn parse_opt(s: Option<String>) -> Option<Value> {
    s.and_then(|t| serde_json::from_str(&t).ok())
}

/// Map a `workflow_status` row to a [`WorkflowStatus`].
fn row_to_status(row: &sqlx::postgres::PgRow) -> WorkflowStatus {
    let inputs: Option<String> = row.try_get("inputs").ok().flatten();
    WorkflowStatus {
        id: row.get("workflow_uuid"),
        name: row.get("name"),
        status: row.get("status"),
        input: parse_opt(inputs).unwrap_or(Value::Null),
        output: parse_opt(row.try_get("output").ok().flatten()),
        error: row.try_get("error").ok().flatten(),
        executor_id: row.get("executor_id"),
        app_version: row.get("application_version"),
        queue_name: row.try_get("queue_name").ok().flatten(),
        priority: row.get("priority"),
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
impl StateProvider for PostgresProvider {
    async fn init(&self) -> Result<()> {
        // Embedded migrations (baked in at compile time from ./migrations/postgres).
        // sqlx tracks applied versions in `_sqlx_migrations` and applies only what
        // is pending, so this is safe on every startup and upgrades existing DBs.
        sqlx::migrate!("./migrations/postgres")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<WorkflowStatus> {
        // Idempotent create: an existing id is left untouched.
        sqlx::query(
            "INSERT INTO workflow_status
                 (workflow_uuid, name, inputs, status, executor_id, application_version,
                  queue_name, priority, deduplication_id, parent_workflow_id,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, delay_until_epoch_ms,
                  created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
            "SELECT {SELECT_COLS} FROM workflow_status WHERE workflow_uuid = $1"
        ))
        .bind(&s.id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row_to_status(&row))
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
        let output_str = output.map(serde_json::to_string).transpose()?;
        sqlx::query(
            "UPDATE workflow_status
             SET status = $2,
                 output = COALESCE($3, output),
                 error  = COALESCE($4, error),
                 updated_at = $5
             WHERE workflow_uuid = $1",
        )
        .bind(id)
        .bind(status)
        .bind(output_str)
        .bind(error)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT output FROM operation_outputs WHERE workflow_uuid = $1 AND function_id = $2",
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
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(serde_json::to_string(&value)?)
        .execute(&self.pool)
        .await?;

        // Read back the canonical value (ours, or a racing writer's that won).
        let row = sqlx::query(
            "SELECT output FROM operation_outputs WHERE workflow_uuid = $1 AND function_id = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;
        Ok(parse_opt(row.get::<Option<String>, _>("output")).unwrap_or(Value::Null))
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowStatus>> {
        let rows = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM workflow_status WHERE status = $1"
        ))
        .bind(STATUS_PENDING)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_status).collect())
    }

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;

        let mut max_tasks = req.max_tasks;

        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let recent: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM workflow_status
                 WHERE queue_name = $1 AND rate_limited = TRUE
                   AND status NOT IN ($2, $3) AND started_at_epoch_ms > $4",
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
                "SELECT COUNT(*) FROM workflow_status WHERE queue_name = $1 AND status = $2",
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

        // SKIP LOCKED lets concurrent dispatchers claim disjoint sets without
        // blocking; with a global concurrency cap, NOWAIT instead surfaces
        // contention so the counts above stay consistent (matches Go).
        let lock = if req.global_concurrency.is_none() {
            "FOR UPDATE SKIP LOCKED"
        } else {
            "FOR UPDATE NOWAIT"
        };
        let ids: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = $1 AND status = $2
               AND (application_version = $3 OR application_version = '')
             ORDER BY priority ASC, created_at ASC
             {lock} LIMIT $4"
        ))
        .bind(&req.queue_name)
        .bind(STATUS_ENQUEUED)
        .bind(&req.app_version)
        .bind(max_tasks)
        .fetch_all(&mut *tx)
        .await?;

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
}
