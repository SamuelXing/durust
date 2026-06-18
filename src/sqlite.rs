use crate::error::Result;
use crate::provider::{
    decode_roles, dedup_or, encode_roles, is_terminal, nonexistent_or, DequeueRequest, ListFilter,
    StateProvider, StepInfo, WorkflowAggregate, WorkflowAggregateQuery, WorkflowStatus,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS, STREAM_CLOSED_SENTINEL,
};
use crate::serialize::{self, Serializer};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{Sqlite, SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{QueryBuilder, Row};
use std::collections::BTreeMap;
use std::str::FromStr;

const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, queue_partition_key, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_timeout_ms, workflow_deadline_epoch_ms, \
     started_at_epoch_ms, rate_limited, delay_until_epoch_ms, completed_at, forked_from, \
     authenticated_user, assumed_role, authenticated_roles, \
     serialization, created_at, updated_at";

/// SQLite-backed [`StateProvider`].
///
/// Gives durable, crash-recoverable state without running a database server —
/// the embedded counterpart to [`crate::PostgresProvider`], using the same
/// canonical DBOS schema. A file URL (`sqlite://durust.db`) survives process
/// restarts; `sqlite::memory:` is handy for tests within a single process.
pub struct SqliteProvider {
    pool: SqlitePool,
    /// Format used when *encoding* stored values; decoding follows each row's
    /// recorded format. See [`crate::Serializer`].
    serializer: Serializer,
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
        Ok(Self::from_pool(pool))
    }

    /// Build a provider from an existing pool.
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            serializer: Serializer::default(),
        }
    }

    /// Choose the format new values are encoded with (see [`crate::Serializer`]).
    pub fn with_serializer(mut self, serializer: Serializer) -> Self {
        self.serializer = serializer;
        self
    }
}

fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

fn row_to_status(row: &sqlx::sqlite::SqliteRow) -> WorkflowStatus {
    let fmt: Option<String> = row.try_get("serialization").ok().flatten();
    let fmt = fmt.as_deref();
    let inputs: Option<String> = row.try_get("inputs").ok().flatten();
    let output: Option<String> = row.try_get("output").ok().flatten();
    WorkflowStatus {
        id: row.get("workflow_uuid"),
        name: row.get("name"),
        status: row.get("status"),
        input: serialize::decode_opt(fmt, inputs.as_deref())
            .ok()
            .flatten()
            .unwrap_or(Value::Null),
        output: serialize::decode_opt(fmt, output.as_deref()).ok().flatten(),
        error: row.try_get("error").ok().flatten(),
        executor_id: row.get("executor_id"),
        app_version: row.get("application_version"),
        queue_name: row.try_get("queue_name").ok().flatten(),
        queue_partition_key: row.try_get("queue_partition_key").ok().flatten(),
        priority: row.get::<i64, _>("priority") as i32,
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
                  queue_name, queue_partition_key, priority, deduplication_id, parent_workflow_id,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, delay_until_epoch_ms,
                  authenticated_user, assumed_role, authenticated_roles,
                  serialization, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (workflow_uuid) DO NOTHING",
        )
        .bind(&s.id)
        .bind(&s.name)
        .bind(self.serializer.encode(&s.input)?)
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
        let output_str = output.map(|v| self.serializer.encode(v)).transpose()?;
        let now = Utc::now().timestamp_millis();
        let completed = is_terminal(status).then_some(now);
        sqlx::query(
            "UPDATE workflow_status
             SET status = ?,
                 output = COALESCE(?, output),
                 error  = COALESCE(?, error),
                 completed_at = COALESCE(?, completed_at),
                 updated_at = ?
             WHERE workflow_uuid = ?",
        )
        .bind(status)
        .bind(output_str)
        .bind(error)
        .bind(completed)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT output, serialization FROM operation_outputs
             WHERE workflow_uuid = ? AND function_id = ?",
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
    ) -> Result<Value> {
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, output, serialization)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(self.serializer.encode(&value)?)
        .bind(self.serializer.name())
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT output, serialization FROM operation_outputs
             WHERE workflow_uuid = ? AND function_id = ?",
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

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        // A plain (deferred) transaction suffices for claim-once here: each queue
        // has a single dispatch loop (one per `launch`), which iterates its
        // partitions sequentially, so two dequeue transactions never scan the
        // same queue's rows at once, and SQLite is single-process. Postgres needs
        // FOR UPDATE + snapshot isolation only because dispatchers race across
        // processes; Go's SQLite `BEGIN IMMEDIATE` is defense-in-depth for that
        // model and not required under this one.
        let mut tx = self.pool.begin().await?;

        let mut max_tasks = req.max_tasks;

        // A partitioned queue scopes every count and the candidate scan to one
        // partition key; a non-partitioned queue (`None`) leaves them unscoped.
        let part = req.partition_key.as_deref();
        let part_clause = if part.is_some() {
            " AND queue_partition_key = ?"
        } else {
            ""
        };

        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let sql = format!(
                "SELECT COUNT(*) FROM workflow_status
                 WHERE queue_name = ? AND rate_limited = TRUE
                   AND status NOT IN (?, ?) AND started_at_epoch_ms > ?{part_clause}"
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
            let sql = format!(
                "SELECT COUNT(*) FROM workflow_status WHERE queue_name = ? AND status = ?{part_clause}"
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

        let sql = format!(
            "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = ? AND status = ?
               AND (application_version = ? OR application_version = ''){part_clause}
             ORDER BY priority ASC, created_at ASC
             LIMIT ?"
        );
        let mut q = sqlx::query_scalar(&sql)
            .bind(&req.queue_name)
            .bind(STATUS_ENQUEUED)
            .bind(&req.app_version);
        if let Some(p) = part {
            q = q.bind(p);
        }
        let ids: Vec<String> = q.bind(max_tasks).fetch_all(&mut *tx).await?;

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

    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>> {
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT queue_partition_key FROM workflow_status
             WHERE queue_name = ? AND status = ? AND queue_partition_key IS NOT NULL",
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
             VALUES (?, ?, ?, ?, ?, ?)",
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
        // otherwise lose the message.
        let mut tx = self.pool.begin().await?;

        let claimed: Option<(String, Option<String>)> = sqlx::query_as(
            "UPDATE notifications SET consumed = TRUE
             WHERE message_uuid = (
                 SELECT message_uuid FROM notifications
                 WHERE destination_uuid = ? AND topic = ? AND consumed = FALSE
                 ORDER BY created_at_epoch_ms ASC
                 LIMIT 1
             )
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
             VALUES (?, ?, ?, ?, ?)
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
             VALUES (?, ?, ?, ?)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = excluded.value, serialization = excluded.serialization",
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
            "SELECT value, serialization FROM workflow_events WHERE workflow_uuid = ? AND key = ?",
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
        let mut qb: QueryBuilder<Sqlite> =
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

        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("SELECT ");
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

    async fn cancel_workflow(&self, id: &str) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE workflow_status
             SET status = ?, completed_at = ?, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL, updated_at = ?
             WHERE workflow_uuid = ? AND status NOT IN (?, ?, ?)",
        )
        .bind(STATUS_CANCELLED)
        .bind(now)
        .bind(now)
        .bind(id)
        .bind(crate::provider::STATUS_SUCCESS)
        .bind(crate::provider::STATUS_ERROR)
        .bind(STATUS_CANCELLED)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resume_workflow(&self, id: &str) -> Result<bool> {
        let now = Utc::now().timestamp_millis();
        let res = sqlx::query(
            "UPDATE workflow_status
             SET status = ?, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                 updated_at = ?
             WHERE workflow_uuid = ? AND status NOT IN (?, ?)",
        )
        .bind(STATUS_PENDING)
        .bind(now)
        .bind(id)
        .bind(crate::provider::STATUS_SUCCESS)
        .bind(crate::provider::STATUS_ERROR)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let now = Utc::now().timestamp_millis();
        let mut qb: QueryBuilder<Sqlite> =
            QueryBuilder::new("UPDATE workflow_status SET status = ");
        qb.push_bind(STATUS_CANCELLED)
            .push(", completed_at = ")
            .push_bind(now)
            .push(", started_at_epoch_ms = NULL, queue_name = NULL, deduplication_id = NULL, updated_at = ")
            .push_bind(now)
            .push(" WHERE workflow_uuid IN (");
        push_bind_list(&mut qb, ids);
        qb.push(") AND status NOT IN (");
        push_bind_list(&mut qb, &[STATUS_SUCCESS, STATUS_ERROR, STATUS_CANCELLED]);
        qb.push(")");
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now().timestamp_millis();
        let mut qb: QueryBuilder<Sqlite> =
            QueryBuilder::new("UPDATE workflow_status SET status = ");
        qb.push_bind(STATUS_PENDING)
            .push(", recovery_attempts = 0, workflow_deadline_epoch_ms = NULL, deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL, updated_at = ")
            .push_bind(now)
            .push(" WHERE workflow_uuid IN (");
        push_bind_list(&mut qb, ids);
        qb.push(") AND status NOT IN (");
        push_bind_list(&mut qb, &[STATUS_SUCCESS, STATUS_ERROR]);
        qb.push(") RETURNING workflow_uuid");
        let resumed = qb
            .build_query_scalar::<String>()
            .fetch_all(&self.pool)
            .await?;
        Ok(resumed)
    }

    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // ON DELETE CASCADE removes each workflow's step / event / stream rows.
        let mut qb: QueryBuilder<Sqlite> = if delete_children {
            let mut qb = QueryBuilder::new(
                "WITH RECURSIVE targets AS (
                     SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid IN (",
            );
            push_bind_list(&mut qb, ids);
            qb.push(
                ")
                     UNION
                     SELECT w.workflow_uuid FROM workflow_status w
                       JOIN targets t ON w.parent_workflow_id = t.workflow_uuid
                 )
                 DELETE FROM workflow_status
                 WHERE workflow_uuid IN (SELECT workflow_uuid FROM targets)",
            );
            qb
        } else {
            let mut qb = QueryBuilder::new("DELETE FROM workflow_status WHERE workflow_uuid IN (");
            push_bind_list(&mut qb, ids);
            qb.push(")");
            qb
        };
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool> {
        let res = sqlx::query(
            "UPDATE workflow_status SET delay_until_epoch_ms = ?, updated_at = ?
             WHERE workflow_uuid = ? AND status = ?",
        )
        .bind(delay_until_ms)
        .bind(Utc::now().timestamp_millis())
        .bind(id)
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
             SELECT ?, ?, name, inputs, serialization, '', ?, ?, 0,
                    authenticated_user, assumed_role, authenticated_roles, ?, ?
             FROM workflow_status WHERE workflow_uuid = ?",
        )
        .bind(new_id)
        .bind(STATUS_PENDING)
        .bind(app_version)
        .bind(original_id)
        .bind(now)
        .bind(now)
        .bind(original_id)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(crate::error::Error::app(format!(
                "cannot fork nonexistent workflow `{original_id}`"
            )));
        }

        sqlx::query("UPDATE workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = ?")
            .bind(original_id)
            .execute(&mut *tx)
            .await?;

        if start_step > 0 {
            sqlx::query(
                "INSERT INTO operation_outputs
                     (workflow_uuid, function_id, function_name, output, error,
                      child_workflow_id, serialization)
                 SELECT ?, function_id, function_name, output, error,
                        child_workflow_id, serialization
                 FROM operation_outputs WHERE workflow_uuid = ? AND function_id < ?",
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
            "UPDATE workflow_status SET recovery_attempts = recovery_attempts + 1, updated_at = ?
             WHERE workflow_uuid = ? RETURNING recovery_attempts",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let attempts = attempts.unwrap_or(0) as i32;
        if attempts > max {
            sqlx::query("UPDATE workflow_status SET status = ? WHERE workflow_uuid = ?")
                .bind(STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED)
                .bind(id)
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
             VALUES (?, ?, ?, ?)
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
             WHERE workflow_uuid = ? AND function_id = ?",
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
             WHERE workflow_uuid = ?
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
             WHERE workflow_uuid = ? AND function_id = ?",
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
             VALUES (?, ?, ?)
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

        // Check-then-append in one transaction; SQLite serializes writers, so
        // the next offset cannot be claimed concurrently.
        let mut tx = self.pool.begin().await?;

        let closed: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM streams WHERE workflow_uuid = ? AND key = ? AND value = ? LIMIT 1",
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
             SELECT ?, ?, ?, COALESCE(
                 (SELECT MAX(\"offset\") FROM streams WHERE workflow_uuid = ? AND key = ?), -1
             ) + 1, ?, ?",
        )
        .bind(workflow_id)
        .bind(key)
        .bind(&stored)
        .bind(workflow_id)
        .bind(key)
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
             WHERE workflow_uuid = ? AND key = ? AND \"offset\" >= ?
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
}

/// Map an `operation_outputs` row to a [`StepInfo`], decoding `output` per the
/// row's recorded serialization format.
fn row_to_step(row: &sqlx::sqlite::SqliteRow) -> Result<StepInfo> {
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

/// Append the WHERE clause shared by `list_workflows` (SQLite dialect).
/// Push a comma-separated `?, ?, …` of bound values for an `IN (...)` clause.
/// Values are bound as owned `String`s so the list need not outlive the builder.
fn push_bind_list<T: AsRef<str>>(qb: &mut QueryBuilder<'_, Sqlite>, items: &[T]) {
    let mut sep = qb.separated(", ");
    for it in items {
        sep.push_bind(it.as_ref().to_owned());
    }
}

fn push_list_filters<'a>(qb: &mut QueryBuilder<'a, Sqlite>, filter: &'a ListFilter) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Sqlite>| {
        qb.push(sep);
        sep = " AND ";
    };
    if !filter.workflow_ids.is_empty() {
        clause(qb);
        qb.push("workflow_uuid IN (");
        let mut sebs = qb.separated(", ");
        for id in &filter.workflow_ids {
            sebs.push_bind(id.as_str());
        }
        sebs.push_unseparated(")");
    }
    if let Some(prefix) = &filter.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(name) = &filter.name {
        clause(qb);
        qb.push("name = ").push_bind(name.as_str());
    }
    if !filter.status.is_empty() {
        clause(qb);
        qb.push("status IN (");
        let mut sebs = qb.separated(", ");
        for s in &filter.status {
            sebs.push_bind(s.as_str());
        }
        sebs.push_unseparated(")");
    }
    if let Some(q) = &filter.queue_name {
        clause(qb);
        qb.push("queue_name = ").push_bind(q.as_str());
    }
    if let Some(v) = &filter.app_version {
        clause(qb);
        qb.push("application_version = ").push_bind(v.as_str());
    }
    if !filter.executor_ids.is_empty() {
        clause(qb);
        qb.push("executor_id IN (");
        let mut sebs = qb.separated(", ");
        for e in &filter.executor_ids {
            sebs.push_bind(e.as_str());
        }
        sebs.push_unseparated(")");
    }
    if let Some(f) = &filter.forked_from {
        clause(qb);
        qb.push("forked_from = ").push_bind(f.as_str());
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

/// Append the WHERE clause for `get_workflow_aggregates` (SQLite dialect).
fn push_agg_filters<'a>(qb: &mut QueryBuilder<'a, Sqlite>, q: &'a WorkflowAggregateQuery) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Sqlite>| {
        qb.push(sep);
        sep = " AND ";
    };
    let mut push_in = |qb: &mut QueryBuilder<'a, Sqlite>, col: &str, vals: &'a [String]| {
        if vals.is_empty() {
            return;
        }
        clause(qb);
        qb.push(col).push(" IN (");
        let mut sebs = qb.separated(", ");
        for v in vals {
            sebs.push_bind(v.as_str());
        }
        sebs.push_unseparated(")");
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
    row: &sqlx::sqlite::SqliteRow,
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
