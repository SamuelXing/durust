use crate::error::Result;
use crate::provider::{StateProvider, WorkflowStatus, STATUS_PENDING};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::time::Duration;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS durust_workflows (
    id                TEXT PRIMARY KEY,
    name              TEXT        NOT NULL,
    input             JSONB       NOT NULL,
    output            JSONB,
    status            TEXT        NOT NULL DEFAULT 'PENDING',
    error             TEXT,
    executor_id       TEXT        NOT NULL DEFAULT '',
    app_version       TEXT        NOT NULL DEFAULT '',
    queue_name        TEXT,
    priority          INT         NOT NULL DEFAULT 0,
    dedup_id          TEXT,
    recovery_attempts INT         NOT NULL DEFAULT 0,
    parent_id         TEXT,
    deadline_ms       BIGINT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS durust_steps (
    workflow_id TEXT        NOT NULL REFERENCES durust_workflows(id),
    seq         INT         NOT NULL,
    name        TEXT        NOT NULL,
    result      JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, seq)
);

CREATE TABLE IF NOT EXISTS durust_timers (
    workflow_id TEXT        NOT NULL REFERENCES durust_workflows(id),
    seq         INT         NOT NULL,
    wake_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workflow_id, seq)
);

CREATE INDEX IF NOT EXISTS durust_workflows_status_idx
    ON durust_workflows(status);

-- Dispatcher lookup: enqueued rows of a queue ordered by priority (Phase 2).
CREATE INDEX IF NOT EXISTS durust_workflows_queue_idx
    ON durust_workflows(queue_name, status, priority);

-- Queue-scoped deduplication (Phase 2): at most one row per (queue, dedup_id).
CREATE UNIQUE INDEX IF NOT EXISTS durust_workflows_dedup_idx
    ON durust_workflows(queue_name, dedup_id)
    WHERE dedup_id IS NOT NULL;
"#;

/// Postgres-backed [`StateProvider`], built on sqlx.
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

/// Map a `durust_workflows` row to a [`WorkflowStatus`].
fn row_to_status(row: &sqlx::postgres::PgRow) -> WorkflowStatus {
    WorkflowStatus {
        id: row.get("id"),
        name: row.get("name"),
        status: row.get("status"),
        input: row.get("input"),
        output: row.try_get("output").ok(),
        error: row.try_get("error").ok(),
        executor_id: row.get("executor_id"),
        app_version: row.get("app_version"),
        queue_name: row.try_get("queue_name").ok(),
        priority: row.get("priority"),
        dedup_id: row.try_get("dedup_id").ok(),
        recovery_attempts: row.get("recovery_attempts"),
        parent_workflow_id: row.try_get("parent_id").ok(),
        deadline_ms: row.try_get("deadline_ms").ok(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

const SELECT_COLS: &str = "id, name, input, output, status, error, executor_id, app_version, \
     queue_name, priority, dedup_id, recovery_attempts, parent_id, deadline_ms, \
     created_at, updated_at";

#[async_trait]
impl StateProvider for PostgresProvider {
    async fn init(&self) -> Result<()> {
        // The schema is a multi-statement batch; `execute` on the pool runs it.
        sqlx::raw_sql(SCHEMA).execute(&self.pool).await?;
        Ok(())
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<WorkflowStatus> {
        // Idempotent create: an existing id is left untouched.
        sqlx::query(
            "INSERT INTO durust_workflows
                 (id, name, input, status, executor_id, app_version,
                  queue_name, priority, dedup_id, parent_id, deadline_ms)
             VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&s.id)
        .bind(&s.name)
        .bind(&s.input)
        .bind(&s.status)
        .bind(&s.executor_id)
        .bind(&s.app_version)
        .bind(&s.queue_name)
        .bind(s.priority)
        .bind(&s.dedup_id)
        .bind(&s.parent_workflow_id)
        .bind(s.deadline_ms)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM durust_workflows WHERE id = $1"
        ))
        .bind(&s.id)
        .fetch_one(&self.pool)
        .await?;

        Ok(row_to_status(&row))
    }

    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM durust_workflows WHERE id = $1"
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
        sqlx::query(
            "UPDATE durust_workflows
             SET status = $2,
                 output = COALESCE($3::jsonb, output),
                 error  = COALESCE($4, error),
                 updated_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(status)
        .bind(output.cloned())
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query("SELECT result FROM durust_steps WHERE workflow_id = $1 AND seq = $2")
            .bind(workflow_id)
            .bind(seq)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get::<Value, _>("result")))
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
    ) -> Result<Value> {
        sqlx::query(
            "INSERT INTO durust_steps (workflow_id, seq, name, result) VALUES ($1, $2, $3, $4::jsonb)
             ON CONFLICT (workflow_id, seq) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(value)
        .execute(&self.pool)
        .await?;

        // Read back the canonical value (ours, or a racing writer's that won).
        let row = sqlx::query("SELECT result FROM durust_steps WHERE workflow_id = $1 AND seq = $2")
            .bind(workflow_id)
            .bind(seq)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<Value, _>("result"))
    }

    async fn get_or_set_wakeup(
        &self,
        workflow_id: &str,
        seq: i32,
        dur: Duration,
    ) -> Result<DateTime<Utc>> {
        let proposed: DateTime<Utc> =
            Utc::now() + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero());

        sqlx::query(
            "INSERT INTO durust_timers (workflow_id, seq, wake_at) VALUES ($1, $2, $3)
             ON CONFLICT (workflow_id, seq) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(proposed)
        .execute(&self.pool)
        .await?;

        let row =
            sqlx::query("SELECT wake_at FROM durust_timers WHERE workflow_id = $1 AND seq = $2")
                .bind(workflow_id)
                .bind(seq)
                .fetch_one(&self.pool)
                .await?;
        Ok(row.get::<DateTime<Utc>, _>("wake_at"))
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowStatus>> {
        let rows = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM durust_workflows WHERE status = $1"
        ))
        .bind(STATUS_PENDING)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_status).collect())
    }
}
