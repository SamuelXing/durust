use crate::error::{Error, Result};
use crate::provider::{
    col_i64, col_str, decode_roles, encode_roles, is_terminal, DequeueRequest, ExportedWorkflow,
    ListFilter, NotificationInfo, StateProvider, StepAggregate, StepAggregateQuery, StepInfo,
    VersionInfo, WorkflowAggregate, WorkflowAggregateQuery, WorkflowStatus, STATUS_CANCELLED,
    STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
    STATUS_PENDING, STATUS_SUCCESS, STREAM_CLOSED_SENTINEL,
};
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use crate::tx::TxBody;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use tokio::sync::Mutex;

struct NotificationRow {
    destination_id: String,
    topic: String,
    message: Value,
    consumed: bool,
    created_at_ms: i64,
}

/// One recorded operation, mirroring an `operation_outputs` row: it holds either
/// a step `output` or a `child_workflow_id` (a started child workflow), plus
/// optional start/finish timestamps (epoch ms).
#[derive(Clone, Default)]
struct StepRow {
    name: String,
    output: Option<Value>,
    child_workflow_id: Option<String>,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}

#[derive(Default)]
struct Inner {
    workflows: HashMap<String, WorkflowStatus>,
    /// Recorded operations keyed by `(workflow_id, seq)`.
    steps: HashMap<(String, i32), StepRow>,
    notifications: Vec<NotificationRow>,
    /// Workflow events keyed by `(workflow_id, key)`.
    events: HashMap<(String, String), Value>,
    /// Append-only streams keyed by `(workflow_id, key)`. Each entry's offset is
    /// its index; `None` is the close sentinel sealing the stream.
    streams: HashMap<(String, String), Vec<Option<Value>>>,
    /// Persisted cron schedules keyed by `schedule_name`.
    schedules: HashMap<String, WorkflowSchedule>,
    /// Registered application versions keyed by `version_name`.
    versions: HashMap<String, VersionInfo>,
}

/// In-memory [`StateProvider`] for tests and quick starts (no database needed).
///
/// State lives only in this process, so it demonstrates step idempotency and
/// in-process recovery, but NOT crash-recovery across process restarts — for
/// that, use [`crate::PostgresProvider`].
#[derive(Default)]
pub struct InMemoryProvider {
    inner: Mutex<Inner>,
}

impl InMemoryProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateProvider for InMemoryProvider {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn insert_workflow_status(&self, status: WorkflowStatus) -> Result<WorkflowStatus> {
        let mut g = self.inner.lock().await;
        // Queue-scoped deduplication (the SQL backends enforce this with a
        // unique index on (queue_name, deduplication_id)).
        if let (Some(queue), Some(dedup)) = (&status.queue_name, &status.dedup_id) {
            let conflict = g.workflows.values().any(|w| {
                w.id != status.id
                    && w.queue_name.as_deref() == Some(queue)
                    && w.dedup_id.as_deref() == Some(dedup)
            });
            if conflict {
                return Err(Error::queue_deduplicated(queue, dedup));
            }
        }
        let row = g
            .workflows
            .entry(status.id.clone())
            .or_insert(status)
            .clone();
        Ok(row)
    }

    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>> {
        let g = self.inner.lock().await;
        Ok(g.workflows
            .values()
            .find(|w| {
                w.queue_name.as_deref() == Some(queue_name)
                    && w.dedup_id.as_deref() == Some(dedup_id)
            })
            .map(|w| w.id.clone()))
    }

    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        let g = self.inner.lock().await;
        Ok(g.workflows.get(id).cloned())
    }

    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            // A workflow cancelled during its final step must stay cancelled: a
            // SUCCESS/ERROR completion is not allowed to overwrite a CANCELLED row.
            let is_completion = status == STATUS_SUCCESS || status == STATUS_ERROR;
            if is_completion && row.status == STATUS_CANCELLED {
                return Err(Error::Cancelled(id.to_string()));
            }
            row.status = status.to_string();
            if let Some(o) = output {
                row.output = Some(o.clone());
            }
            if let Some(e) = error {
                row.error = Some(e.to_string());
            }
            let now = Utc::now();
            if is_terminal(status) {
                row.completed_at_ms = Some(now.timestamp_millis());
                // Reaching a terminal state frees the queue-scoped deduplication
                // slot so the same deduplication id can be enqueued again.
                row.dedup_id = None;
            }
            row.updated_at = now;
        }
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let g = self.inner.lock().await;
        Ok(g.steps
            .get(&(workflow_id.to_string(), seq))
            .and_then(|r| r.output.clone()))
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
        started_at_ms: Option<i64>,
    ) -> Result<Value> {
        let mut g = self.inner.lock().await;
        let canonical = g
            .steps
            .entry((workflow_id.to_string(), seq))
            .or_insert_with(|| StepRow {
                name: name.to_string(),
                output: Some(value),
                child_workflow_id: None,
                started_at_ms,
                completed_at_ms: Some(Utc::now().timestamp_millis()),
            })
            .output
            .clone()
            .unwrap_or(Value::Null);
        Ok(canonical)
    }

    async fn run_transaction_step(
        &self,
        _workflow_id: &str,
        _seq: i32,
        _started_at_ms: i64,
        _opts: &crate::tx::TransactionOptions,
        _body: TxBody<'_>,
    ) -> Result<Value> {
        // Transactional steps need a real SQL transaction; the in-memory store
        // has none. Run such workflows on the SQLite or Postgres backend.
        Err(Error::app(
            "transactional steps require a SQL backend (Postgres or SQLite)",
        ))
    }

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut g = self.inner.lock().await;

        let mut max_tasks = req.max_tasks;

        // For a partitioned queue every count is scoped to one partition key;
        // for a non-partitioned queue (`None`) all of the queue's rows match.
        let in_partition = |w: &WorkflowStatus| {
            req.partition_key.is_none()
                || w.queue_partition_key.as_deref() == req.partition_key.as_deref()
        };

        // Rate limiter: count rate-limited starts within the trailing window.
        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let cutoff = now_ms - period_ms;
            let recent = g
                .workflows
                .values()
                .filter(|w| {
                    w.queue_name.as_deref() == Some(req.queue_name.as_str())
                        && in_partition(w)
                        && w.rate_limited
                        && w.status != STATUS_ENQUEUED
                        && w.status != STATUS_DELAYED
                        && w.started_at_ms.is_some_and(|t| t > cutoff)
                })
                .count() as i64;
            max_tasks = max_tasks.min((limit - recent).max(0));
        }

        // Global concurrency: cap by queue-wide PENDING count.
        if let Some(global) = req.global_concurrency {
            let pending = g
                .workflows
                .values()
                .filter(|w| {
                    w.queue_name.as_deref() == Some(req.queue_name.as_str())
                        && in_partition(w)
                        && w.status == STATUS_PENDING
                })
                .count() as i64;
            max_tasks = max_tasks.min((global - pending).max(0));
        }

        if max_tasks <= 0 {
            return Ok(Vec::new());
        }

        // Candidates ordered by (priority, created_at), version-gated.
        let mut ids: Vec<(i32, i64, String)> = g
            .workflows
            .values()
            .filter(|w| {
                w.queue_name.as_deref() == Some(req.queue_name.as_str())
                    && in_partition(w)
                    && w.status == STATUS_ENQUEUED
                    && (w.app_version.is_empty() || w.app_version == req.app_version)
            })
            .map(|w| (w.priority, w.created_at.timestamp_millis(), w.id.clone()))
            .collect();
        ids.sort();
        ids.truncate(max_tasks as usize);

        let rate_limited = req.rate_limit_max.is_some();
        let mut claimed = Vec::with_capacity(ids.len());
        for (_, _, id) in ids {
            let w = g.workflows.get_mut(&id).expect("candidate id must exist");
            w.status = STATUS_PENDING.to_string();
            w.executor_id = req.executor_id.clone();
            w.app_version = req.app_version.clone();
            w.started_at_ms = Some(now_ms);
            w.rate_limited = rate_limited;
            if w.deadline_ms.is_none() {
                w.deadline_ms = w.timeout_ms.map(|t| now_ms + t);
            }
            w.updated_at = Utc::now();
            claimed.push(w.clone());
        }
        Ok(claimed)
    }

    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64> {
        let mut g = self.inner.lock().await;
        let mut n = 0;
        for w in g.workflows.values_mut() {
            if w.status == STATUS_DELAYED && w.delay_until_ms.is_some_and(|t| t <= now_ms) {
                w.status = STATUS_ENQUEUED.to_string();
                w.delay_until_ms = None;
                w.updated_at = Utc::now();
                n += 1;
            }
        }
        Ok(n)
    }

    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>> {
        let g = self.inner.lock().await;
        let mut keys: Vec<String> = g
            .workflows
            .values()
            .filter(|w| w.queue_name.as_deref() == Some(queue_name) && w.status == STATUS_ENQUEUED)
            .filter_map(|w| w.queue_partition_key.clone())
            .collect();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
    ) -> Result<()> {
        let mut g = self.inner.lock().await;
        // Mirror the SQL backends' FK on destination_uuid → workflow_status.
        if !g.workflows.contains_key(destination_id) {
            return Err(Error::nonexistent_workflow(destination_id));
        }
        g.notifications.push(NotificationRow {
            destination_id: destination_id.to_string(),
            topic: topic.to_string(),
            message,
            consumed: false,
            created_at_ms: Utc::now().timestamp_millis(),
        });
        Ok(())
    }

    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>> {
        // Single mutex covers both the claim and the checkpoint, giving the
        // same atomicity the SQL backends get from a transaction.
        let mut g = self.inner.lock().await;
        let oldest = g
            .notifications
            .iter_mut()
            .filter(|n| !n.consumed && n.destination_id == workflow_id && n.topic == topic)
            .min_by_key(|n| n.created_at_ms);
        let Some(row) = oldest else {
            return Ok(None);
        };
        row.consumed = true;
        let message = row.message.clone();
        let canonical = g
            .steps
            .entry((workflow_id.to_string(), seq))
            .or_insert_with(|| StepRow {
                name: step_name.to_string(),
                output: Some(message),
                child_workflow_id: None,
                ..Default::default()
            })
            .output
            .clone()
            .unwrap_or(Value::Null);
        Ok(Some(canonical))
    }

    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()> {
        let mut g = self.inner.lock().await;
        g.events
            .insert((workflow_id.to_string(), key.to_string()), value);
        Ok(())
    }

    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>> {
        let g = self.inner.lock().await;
        Ok(g.events
            .get(&(workflow_id.to_string(), key.to_string()))
            .cloned())
    }

    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        let g = self.inner.lock().await;
        let mut rows: Vec<WorkflowStatus> = g
            .workflows
            .values()
            .filter(|w| {
                (filter.workflow_ids.is_empty() || filter.workflow_ids.contains(&w.id))
                    && filter
                        .workflow_id_prefix
                        .as_ref()
                        .is_none_or(|p| w.id.starts_with(p))
                    && filter.name.as_ref().is_none_or(|n| &w.name == n)
                    && (filter.status.is_empty() || filter.status.contains(&w.status))
                    && filter
                        .queue_name
                        .as_ref()
                        .is_none_or(|q| w.queue_name.as_deref() == Some(q.as_str()))
                    && filter
                        .app_version
                        .as_ref()
                        .is_none_or(|v| &w.app_version == v)
                    && (filter.executor_ids.is_empty()
                        || filter.executor_ids.contains(&w.executor_id))
                    && filter
                        .forked_from
                        .as_ref()
                        .is_none_or(|f| w.forked_from.as_deref() == Some(f.as_str()))
                    && filter
                        .start_time_ms
                        .is_none_or(|t| w.created_at.timestamp_millis() >= t)
                    && filter
                        .end_time_ms
                        .is_none_or(|t| w.created_at.timestamp_millis() <= t)
                    && filter
                        .completed_after_ms
                        .is_none_or(|t| w.completed_at_ms.is_some_and(|c| c >= t))
                    && filter
                        .completed_before_ms
                        .is_none_or(|t| w.completed_at_ms.is_some_and(|c| c <= t))
                    && filter
                        .dequeued_after_ms
                        .is_none_or(|t| w.started_at_ms.is_some_and(|s| s >= t))
                    && filter
                        .dequeued_before_ms
                        .is_none_or(|t| w.started_at_ms.is_some_and(|s| s <= t))
                    && filter
                        .has_parent
                        .is_none_or(|hp| w.parent_workflow_id.is_some() == hp)
                    && (!filter.queues_only || w.queue_name.is_some())
            })
            .cloned()
            .collect();

        rows.sort_by_key(|w| w.created_at);
        if filter.sort_desc {
            rows.reverse();
        }
        if let Some(off) = filter.offset {
            rows.drain(..(off.max(0) as usize).min(rows.len()));
        }
        if let Some(lim) = filter.limit {
            rows.truncate(lim.max(0) as usize);
        }
        // Honor load flags by dropping the heavy fields the caller opted out of.
        if !filter.load_input || !filter.load_output {
            for w in &mut rows {
                if !filter.load_input {
                    w.input = Value::Null;
                }
                if !filter.load_output {
                    w.output = None;
                }
            }
        }
        Ok(rows)
    }

    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        let g = self.inner.lock().await;
        let cols = query.enabled_columns();
        // Per-group accumulator: count, earliest created_at, and the running max
        // of queue-wait / total-latency (each `None` until a qualifying row).
        #[derive(Default)]
        struct Acc {
            count: i64,
            min_created: Option<i64>,
            max_queue_wait: Option<i64>,
            max_total_latency: Option<i64>,
        }
        let mut accs: HashMap<Vec<(String, Option<String>)>, Acc> = HashMap::new();

        for w in g.workflows.values() {
            // Filters (all ANDed).
            if !query.status.is_empty() && !query.status.contains(&w.status) {
                continue;
            }
            if !query.name.is_empty() && !query.name.contains(&w.name) {
                continue;
            }
            if !query.app_version.is_empty() && !query.app_version.contains(&w.app_version) {
                continue;
            }
            if !query.executor_ids.is_empty() && !query.executor_ids.contains(&w.executor_id) {
                continue;
            }
            if !query.queue_names.is_empty()
                && !query
                    .queue_names
                    .iter()
                    .any(|q| w.queue_name.as_deref() == Some(q.as_str()))
            {
                continue;
            }
            if query
                .workflow_id_prefix
                .as_ref()
                .is_some_and(|p| !w.id.starts_with(p))
            {
                continue;
            }
            let created = w.created_at.timestamp_millis();
            if query.start_time_ms.is_some_and(|t| created < t) {
                continue;
            }
            if query.end_time_ms.is_some_and(|t| created > t) {
                continue;
            }

            // Build this workflow's group key over the enabled dimensions.
            let mut key: Vec<(String, Option<String>)> = cols
                .iter()
                .map(|(dim, _)| {
                    let val = match *dim {
                        "status" => Some(w.status.clone()),
                        "name" => Some(w.name.clone()),
                        "queue_name" => w.queue_name.clone(),
                        "executor_id" => Some(w.executor_id.clone()),
                        "application_version" => Some(w.app_version.clone()),
                        _ => None,
                    };
                    (dim.to_string(), val)
                })
                .collect();
            if let Some(bucket) = query.time_bucket_ms.filter(|b| *b > 0) {
                let start = (created / bucket) * bucket;
                key.push(("time_bucket".to_string(), Some(start.to_string())));
            }

            let acc = accs.entry(key).or_default();
            acc.count += 1;
            acc.min_created = Some(acc.min_created.map_or(created, |m| m.min(created)));
            // Queue wait / total latency only count rows that started / finished.
            if let Some(qw) = w.started_at_ms.map(|s| s - created) {
                acc.max_queue_wait = Some(acc.max_queue_wait.map_or(qw, |m| m.max(qw)));
            }
            if let Some(tl) = w.completed_at_ms.map(|c| c - created) {
                acc.max_total_latency = Some(acc.max_total_latency.map_or(tl, |m| m.max(tl)));
            }
        }

        let mut out: Vec<WorkflowAggregate> = accs
            .into_iter()
            .map(|(k, acc)| WorkflowAggregate {
                group: k.into_iter().collect(),
                count: query.select_count.then_some(acc.count),
                min_created_at: query
                    .select_min_created_at
                    .then_some(acc.min_created)
                    .flatten(),
                max_queue_wait_ms: if query.select_max_queue_wait_ms {
                    acc.max_queue_wait
                } else {
                    None
                },
                max_total_latency_ms: if query.select_max_total_latency_ms {
                    acc.max_total_latency
                } else {
                    None
                },
            })
            .collect();
        // Stable order so callers (and tests) see a deterministic result.
        out.sort_by(|a, b| a.group.iter().cmp(b.group.iter()));
        if let Some(lim) = query.limit {
            out.truncate(lim.max(0) as usize);
        }
        Ok(out)
    }

    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>> {
        let g = self.inner.lock().await;
        let dims = query.group_exprs();
        // group key -> (count, max_duration_ms).
        let mut acc: HashMap<Vec<(String, Option<String>)>, (i64, Option<i64>)> = HashMap::new();

        for ((wid, _seq), row) in g.steps.iter() {
            // No error is recorded on step rows, so every step counts as SUCCESS.
            let status = "SUCCESS";
            if !query.status.is_empty() && !query.status.iter().any(|s| s == status) {
                continue;
            }
            if !query.function_name.is_empty() && !query.function_name.contains(&row.name) {
                continue;
            }
            if query
                .workflow_id_prefix
                .as_ref()
                .is_some_and(|p| !wid.starts_with(p))
            {
                continue;
            }
            // A NULL completed_at fails the bound, matching the SQL comparison.
            if query
                .completed_after_ms
                .is_some_and(|t| row.completed_at_ms.is_none_or(|c| c < t))
            {
                continue;
            }
            if query
                .completed_before_ms
                .is_some_and(|t| row.completed_at_ms.is_none_or(|c| c > t))
            {
                continue;
            }

            let mut key: Vec<(String, Option<String>)> = dims
                .iter()
                .map(|(d, _)| {
                    let val = match *d {
                        "function_name" => Some(row.name.clone()),
                        "status" => Some(status.to_string()),
                        _ => None,
                    };
                    (d.to_string(), val)
                })
                .collect();
            if let Some(bucket) = query.time_bucket_ms.filter(|b| *b > 0) {
                let tb = row
                    .completed_at_ms
                    .map(|c| ((c / bucket) * bucket).to_string());
                key.push(("time_bucket".to_string(), tb));
            }

            let entry = acc.entry(key).or_insert((0, None));
            entry.0 += 1;
            if let (Some(start), Some(end)) = (row.started_at_ms, row.completed_at_ms) {
                let dur = end - start;
                entry.1 = Some(entry.1.map_or(dur, |m| m.max(dur)));
            }
        }

        let mut out: Vec<StepAggregate> = acc
            .into_iter()
            .map(|(k, (count, max_dur))| StepAggregate {
                group: k.into_iter().collect(),
                count: query.select_count.then_some(count),
                max_duration_ms: query.select_max_duration_ms.then_some(max_dur).flatten(),
            })
            .collect();
        out.sort_by(|a, b| a.group.iter().cmp(b.group.iter()));
        if let Some(lim) = query.limit {
            out.truncate(lim.max(0) as usize);
        }
        Ok(out)
    }

    async fn cancel_workflow(&self, id: &str) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            if !is_terminal(&row.status) {
                let now = Utc::now();
                row.status = STATUS_CANCELLED.to_string();
                row.completed_at_ms = Some(now.timestamp_millis());
                row.started_at_ms = None;
                row.queue_name = None;
                row.dedup_id = None;
                row.updated_at = now;
            }
        }
        Ok(())
    }

    async fn resume_workflow(&self, id: &str) -> Result<bool> {
        let mut g = self.inner.lock().await;
        let Some(row) = g.workflows.get_mut(id) else {
            return Ok(false);
        };
        if is_terminal(&row.status) && row.status != STATUS_CANCELLED {
            return Ok(false);
        }
        row.status = STATUS_PENDING.to_string();
        row.recovery_attempts = 0;
        row.deadline_ms = None;
        row.dedup_id = None;
        row.started_at_ms = None;
        row.completed_at_ms = None;
        row.updated_at = Utc::now();
        Ok(true)
    }

    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            row.status = STATUS_ENQUEUED.to_string();
            row.queue_name = Some(queue.to_string());
            row.executor_id = String::new();
            row.started_at_ms = None;
            row.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        let mut g = self.inner.lock().await;
        let now = Utc::now();
        for id in ids {
            if let Some(row) = g.workflows.get_mut(id) {
                if !is_terminal(&row.status) {
                    row.status = STATUS_CANCELLED.to_string();
                    row.completed_at_ms = Some(now.timestamp_millis());
                    row.started_at_ms = None;
                    row.queue_name = None;
                    row.dedup_id = None;
                    row.updated_at = now;
                }
            }
        }
        Ok(())
    }

    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        let mut g = self.inner.lock().await;
        let now = Utc::now();
        let mut resumed = Vec::new();
        for id in ids {
            let Some(row) = g.workflows.get_mut(id) else {
                continue;
            };
            // Same gate as resume_workflow: skip only SUCCESS/ERROR.
            if is_terminal(&row.status) && row.status != STATUS_CANCELLED {
                continue;
            }
            row.status = STATUS_PENDING.to_string();
            row.recovery_attempts = 0;
            row.deadline_ms = None;
            row.dedup_id = None;
            row.started_at_ms = None;
            row.completed_at_ms = None;
            row.updated_at = now;
            resumed.push(id.clone());
        }
        Ok(resumed)
    }

    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        let mut g = self.inner.lock().await;
        let mut targets: Vec<String> = ids.to_vec();
        if delete_children {
            // Breadth-first over parent_workflow_id, mirroring the SQL backends'
            // recursive descendant collection.
            let mut i = 0;
            while i < targets.len() {
                let parent = targets[i].clone();
                i += 1;
                for (cid, row) in g.workflows.iter() {
                    if row.parent_workflow_id.as_deref() == Some(parent.as_str())
                        && !targets.contains(cid)
                    {
                        targets.push(cid.clone());
                    }
                }
            }
        }
        for id in &targets {
            g.workflows.remove(id);
            g.steps.retain(|(wf, _), _| wf != id);
            g.streams.retain(|(wf, _), _| wf != id);
            g.notifications.retain(|n| &n.destination_id != id);
        }
        Ok(())
    }

    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            if row.status == STATUS_DELAYED {
                row.delay_until_ms = Some(delay_until_ms);
                row.updated_at = Utc::now();
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn fork_workflow(
        &self,
        original_id: &str,
        new_id: &str,
        start_step: i32,
        app_version: &str,
    ) -> Result<()> {
        let mut g = self.inner.lock().await;
        let original = g.workflows.get(original_id).cloned().ok_or_else(|| {
            Error::app(format!("cannot fork nonexistent workflow `{original_id}`"))
        })?;

        let mut forked = WorkflowStatus::new(
            new_id,
            &original.name,
            original.input.clone(),
            STATUS_PENDING,
            "",
            app_version,
        );
        forked.forked_from = Some(original_id.to_string());
        forked.authenticated_user = original.authenticated_user.clone();
        forked.assumed_role = original.assumed_role.clone();
        forked.authenticated_roles = original.authenticated_roles.clone();
        g.workflows.insert(new_id.to_string(), forked);

        // (`was_forked_from` is tracked only by the SQL backends, for
        // observability; the in-memory provider has no such column.)

        // Copy step checkpoints with seq < start_step into the forked workflow.
        let copied: Vec<(i32, StepRow)> = g
            .steps
            .iter()
            .filter(|((wid, seq), _)| wid == original_id && *seq < start_step)
            .map(|((_, seq), v)| (*seq, v.clone()))
            .collect();
        for (seq, v) in copied {
            g.steps.insert((new_id.to_string(), seq), v);
        }
        Ok(())
    }

    async fn bump_recovery_attempts(&self, id: &str, max: i32) -> Result<i32> {
        let mut g = self.inner.lock().await;
        let Some(row) = g.workflows.get_mut(id) else {
            return Ok(0);
        };
        row.recovery_attempts += 1;
        let attempts = row.recovery_attempts;
        if attempts > max {
            row.status = STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED.to_string();
            row.dedup_id = None;
            row.updated_at = Utc::now();
        }
        Ok(attempts)
    }

    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()> {
        let mut g = self.inner.lock().await;
        g.steps
            .entry((parent_id.to_string(), seq))
            .or_insert_with(|| StepRow {
                name: name.to_string(),
                output: None,
                child_workflow_id: Some(child_id.to_string()),
                ..Default::default()
            });
        Ok(())
    }

    async fn check_child_workflow(&self, parent_id: &str, seq: i32) -> Result<Option<String>> {
        let g = self.inner.lock().await;
        Ok(g.steps
            .get(&(parent_id.to_string(), seq))
            .and_then(|r| r.child_workflow_id.clone()))
    }

    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let g = self.inner.lock().await;
        let mut steps: Vec<StepInfo> = g
            .steps
            .iter()
            .filter(|((wid, _), _)| wid == workflow_id)
            .map(|((_, seq), row)| StepInfo {
                step_id: *seq,
                name: row.name.clone(),
                output: row.output.clone(),
                error: None,
                child_workflow_id: row.child_workflow_id.clone(),
                started_at: row.started_at_ms.and_then(DateTime::from_timestamp_millis),
                completed_at: row
                    .completed_at_ms
                    .and_then(DateTime::from_timestamp_millis),
            })
            .collect();
        steps.sort_by_key(|s| s.step_id);
        Ok(steps)
    }

    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>> {
        let g = self.inner.lock().await;
        Ok(g.steps
            .get(&(workflow_id.to_string(), seq))
            .map(|r| r.name.clone()))
    }

    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()> {
        let mut g = self.inner.lock().await;
        g.steps
            .entry((workflow_id.to_string(), seq))
            .or_insert_with(|| StepRow {
                name: name.to_string(),
                output: None,
                child_workflow_id: None,
                ..Default::default()
            });
        Ok(())
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        _function_id: i32,
    ) -> Result<()> {
        let mut g = self.inner.lock().await;
        let entries = g
            .streams
            .entry((workflow_id.to_string(), key.to_string()))
            .or_default();
        if entries.iter().any(|e| e.is_none()) {
            return Err(Error::app(format!("stream `{key}` is already closed")));
        }
        entries.push(value);
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        let g = self.inner.lock().await;
        let mut values = Vec::new();
        let mut closed = false;
        if let Some(entries) = g.streams.get(&(workflow_id.to_string(), key.to_string())) {
            for entry in entries.iter().skip(from_offset.max(0) as usize) {
                match entry {
                    Some(v) => values.push(v.clone()),
                    None => {
                        closed = true;
                        break;
                    }
                }
            }
        }
        Ok((values, closed))
    }

    async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        let g = self.inner.lock().await;
        let mut out: Vec<(String, Value)> = g
            .events
            .iter()
            .filter(|((wid, _), _)| wid == workflow_id)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn list_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationInfo>> {
        let g = self.inner.lock().await;
        let mut rows: Vec<&NotificationRow> = g
            .notifications
            .iter()
            .filter(|n| n.destination_id == workflow_id)
            .collect();
        rows.sort_by_key(|n| n.created_at_ms);
        Ok(rows
            .into_iter()
            .map(|n| NotificationInfo {
                topic: (!n.topic.is_empty()).then(|| n.topic.clone()),
                message: n.message.clone(),
                created_at_ms: n.created_at_ms,
                consumed: n.consumed,
            })
            .collect())
    }

    async fn list_workflow_streams(&self, workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>> {
        let g = self.inner.lock().await;
        let mut out: Vec<(String, Vec<Value>)> = g
            .streams
            .iter()
            .filter(|((wid, _), _)| wid == workflow_id)
            .map(|((_, key), entries)| {
                // Stop at the close sentinel (`None`); include values before it.
                let values = entries
                    .iter()
                    .take_while(|e| e.is_some())
                    .filter_map(|e| e.clone())
                    .collect();
                (key.clone(), values)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()> {
        let mut g = self.inner.lock().await;
        if g.schedules.contains_key(&schedule.schedule_name) {
            return Err(Error::app(format!(
                "schedule `{}` already exists",
                schedule.schedule_name
            )));
        }
        g.schedules
            .insert(schedule.schedule_name.clone(), schedule.clone());
        Ok(())
    }

    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()> {
        // The lock is held for the whole batch, so the delete-then-create of
        // every entry is atomic (all-or-nothing). Schedules are keyed by name,
        // so inserting replaces any existing row of that name.
        let mut g = self.inner.lock().await;
        for s in schedules {
            g.schedules.insert(s.schedule_name.clone(), s.clone());
        }
        Ok(())
    }

    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        let g = self.inner.lock().await;
        let mut out: Vec<WorkflowSchedule> = g
            .schedules
            .values()
            .filter(|s| filter.statuses.is_empty() || filter.statuses.contains(&s.status))
            .filter(|s| {
                filter.workflow_names.is_empty() || filter.workflow_names.contains(&s.workflow_name)
            })
            .filter(|s| {
                filter.name_prefixes.is_empty()
                    || filter
                        .name_prefixes
                        .iter()
                        .any(|p| s.schedule_name.starts_with(p))
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.schedule_name.cmp(&b.schedule_name));
        Ok(out)
    }

    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool> {
        let mut g = self.inner.lock().await;
        match g.schedules.get_mut(name) {
            Some(s) => {
                s.status = status;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(s) = g.schedules.get_mut(name) {
            s.last_fired_at = DateTime::from_timestamp_millis(at_ms);
        }
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<bool> {
        let mut g = self.inner.lock().await;
        Ok(g.schedules.remove(name).is_some())
    }

    async fn create_application_version(&self, version_name: &str) -> Result<()> {
        let mut g = self.inner.lock().await;
        let now = Utc::now();
        g.versions
            .entry(version_name.to_string())
            .or_insert_with(|| VersionInfo {
                version_id: uuid::Uuid::new_v4().to_string(),
                version_name: version_name.to_string(),
                version_timestamp: now,
                created_at: now,
            });
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let g = self.inner.lock().await;
        let mut out: Vec<VersionInfo> = g.versions.values().cloned().collect();
        out.sort_by(|a, b| b.version_timestamp.cmp(&a.version_timestamp));
        Ok(out)
    }

    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        let g = self.inner.lock().await;
        Ok(g.versions
            .values()
            .max_by_key(|v| v.version_timestamp)
            .cloned())
    }

    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        let mut g = self.inner.lock().await;
        match g.versions.get_mut(version_name) {
            Some(v) => {
                v.version_timestamp = Utc::now();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>> {
        let g = self.inner.lock().await;

        // Root first, then transitive children discovered through parent_workflow_id.
        let mut ids = vec![workflow_id.to_string()];
        if export_children {
            let mut queue = vec![workflow_id.to_string()];
            while let Some(parent) = queue.pop() {
                let mut children: Vec<String> = g
                    .workflows
                    .values()
                    .filter(|w| w.parent_workflow_id.as_deref() == Some(parent.as_str()))
                    .map(|w| w.id.clone())
                    .collect();
                children.sort();
                for c in children {
                    ids.push(c.clone());
                    queue.push(c);
                }
            }
        }

        let mut exported = Vec::with_capacity(ids.len());
        for id in &ids {
            let Some(w) = g.workflows.get(id) else {
                return Err(Error::nonexistent_workflow(id));
            };

            let mut ops: Vec<(i32, &StepRow)> = g
                .steps
                .iter()
                .filter(|((wid, _), _)| wid == id)
                .map(|((_, seq), r)| (*seq, r))
                .collect();
            ops.sort_by_key(|(seq, _)| *seq);
            let operation_outputs = ops
                .iter()
                .map(|(seq, r)| step_to_map(id, *seq, r))
                .collect();

            let mut evs: Vec<(&String, &Value)> = g
                .events
                .iter()
                .filter(|((wid, _), _)| wid == id)
                .map(|((_, k), v)| (k, v))
                .collect();
            evs.sort_by(|a, b| a.0.cmp(b.0));
            let workflow_events = evs.iter().map(|(k, v)| event_to_map(id, k, v)).collect();

            let mut strms: Vec<(&String, &Vec<Option<Value>>)> = g
                .streams
                .iter()
                .filter(|((wid, _), _)| wid == id)
                .map(|((_, k), vs)| (k, vs))
                .collect();
            strms.sort_by(|a, b| a.0.cmp(b.0));
            let mut streams = Vec::new();
            for (key, entries) in strms {
                for (offset, entry) in entries.iter().enumerate() {
                    streams.push(stream_to_map(id, key, offset as i64, entry));
                }
            }

            exported.push(ExportedWorkflow {
                workflow_status: status_to_map(w),
                operation_outputs,
                workflow_events,
                // No events-history table in the in-memory backend.
                workflow_events_history: Vec::new(),
                streams,
            });
        }
        Ok(exported)
    }

    async fn import_workflow(&self, workflows: &[ExportedWorkflow]) -> Result<()> {
        let mut g = self.inner.lock().await;
        // Validate up front so the whole import is all-or-nothing (the lock is
        // held throughout): importing never overwrites an existing workflow.
        for wf in workflows {
            if let Some(id) = col_str(&wf.workflow_status, "workflow_uuid") {
                if g.workflows.contains_key(&id) {
                    return Err(Error::app(format!("workflow {id} already exists")));
                }
            }
        }
        for wf in workflows {
            let id = col_str(&wf.workflow_status, "workflow_uuid").unwrap_or_default();
            g.workflows
                .insert(id.clone(), map_to_status(&wf.workflow_status));

            for op in &wf.operation_outputs {
                let seq = col_i64(op, "function_id").unwrap_or(0) as i32;
                g.steps.insert(
                    (id.clone(), seq),
                    StepRow {
                        name: col_str(op, "function_name").unwrap_or_default(),
                        output: col_str(op, "output").and_then(|v| serde_json::from_str(&v).ok()),
                        child_workflow_id: col_str(op, "child_workflow_id"),
                        started_at_ms: col_i64(op, "started_at_epoch_ms"),
                        completed_at_ms: col_i64(op, "completed_at_epoch_ms"),
                    },
                );
            }

            for ev in &wf.workflow_events {
                let key = col_str(ev, "key").unwrap_or_default();
                let value = col_str(ev, "value")
                    .and_then(|v| serde_json::from_str(&v).ok())
                    .unwrap_or(Value::Null);
                g.events.insert((id.clone(), key), value);
            }

            // Reassemble each stream's offset-indexed buffer (`None` == close sentinel).
            let mut by_key: HashMap<String, Vec<(usize, Option<Value>)>> = HashMap::new();
            for st in &wf.streams {
                let key = col_str(st, "key").unwrap_or_default();
                let offset = col_i64(st, "offset").unwrap_or(0) as usize;
                let entry = match col_str(st, "value") {
                    Some(v) if v == STREAM_CLOSED_SENTINEL => None,
                    Some(v) => serde_json::from_str(&v).ok(),
                    None => None,
                };
                by_key.entry(key).or_default().push((offset, entry));
            }
            for (key, rows) in by_key {
                let len = rows.iter().map(|(o, _)| *o + 1).max().unwrap_or(0);
                let mut buf = vec![None; len];
                for (offset, entry) in rows {
                    buf[offset] = entry;
                }
                g.streams.insert((id.clone(), key), buf);
            }
        }
        Ok(())
    }
}

/// Epoch-millis (or now if absent) → `DateTime<Utc>`.
fn ms_to_dt(ms: Option<i64>) -> DateTime<Utc> {
    ms.and_then(DateTime::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

/// A stored payload rendered as the portable JSON string a SQL backend keeps in
/// its `inputs`/`output`/`value` TEXT column. A JSON `null` *value* becomes the
/// string `"null"` — it is a present payload, not an absent column, which is what
/// the SQL backends store. Absence is a `null` column instead, which callers
/// produce directly (e.g. a `None` output → `Value::Null`).
fn payload_str(v: &Value) -> Value {
    json!(v.to_string())
}

/// A [`WorkflowStatus`] as a portable `workflow_status` row. Columns this backend
/// does not track (`application_id`/`class_name`/`config_name`/`serialization`)
/// are emitted as null.
fn status_to_map(w: &WorkflowStatus) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("workflow_uuid".into(), json!(w.id));
    m.insert("status".into(), json!(w.status));
    m.insert("name".into(), json!(w.name));
    m.insert("authenticated_user".into(), json!(w.authenticated_user));
    m.insert("assumed_role".into(), json!(w.assumed_role));
    m.insert(
        "authenticated_roles".into(),
        json!(encode_roles(&w.authenticated_roles)),
    );
    m.insert(
        "output".into(),
        w.output.as_ref().map_or(Value::Null, payload_str),
    );
    m.insert("error".into(), json!(w.error));
    m.insert("executor_id".into(), json!(w.executor_id));
    m.insert("created_at".into(), json!(w.created_at.timestamp_millis()));
    m.insert("updated_at".into(), json!(w.updated_at.timestamp_millis()));
    m.insert("application_version".into(), json!(w.app_version));
    m.insert("application_id".into(), Value::Null);
    m.insert("class_name".into(), Value::Null);
    m.insert("config_name".into(), Value::Null);
    m.insert("recovery_attempts".into(), json!(w.recovery_attempts));
    m.insert("queue_name".into(), json!(w.queue_name));
    m.insert("workflow_timeout_ms".into(), json!(w.timeout_ms));
    m.insert("workflow_deadline_epoch_ms".into(), json!(w.deadline_ms));
    m.insert("started_at_epoch_ms".into(), json!(w.started_at_ms));
    m.insert("deduplication_id".into(), json!(w.dedup_id));
    m.insert("inputs".into(), payload_str(&w.input));
    m.insert("priority".into(), json!(w.priority));
    m.insert("queue_partition_key".into(), json!(w.queue_partition_key));
    m.insert("forked_from".into(), json!(w.forked_from));
    m.insert("parent_workflow_id".into(), json!(w.parent_workflow_id));
    m.insert("delay_until_epoch_ms".into(), json!(w.delay_until_ms));
    m.insert("serialization".into(), Value::Null);
    m
}

/// Rebuild a [`WorkflowStatus`] from a portable `workflow_status` row.
fn map_to_status(s: &Map<String, Value>) -> WorkflowStatus {
    // Decode the error per the row's recorded format, so a portable error
    // imported from any SDK surfaces its structured `name`/`code`/`data`.
    let (error, error_info) = crate::serialize::decode_error_opt(
        col_str(s, "serialization").as_deref(),
        col_str(s, "error").as_deref(),
    );
    WorkflowStatus {
        id: col_str(s, "workflow_uuid").unwrap_or_default(),
        name: col_str(s, "name").unwrap_or_default(),
        status: col_str(s, "status").unwrap_or_default(),
        input: col_str(s, "inputs")
            .and_then(|v| serde_json::from_str(&v).ok())
            .unwrap_or(Value::Null),
        output: col_str(s, "output").and_then(|v| serde_json::from_str(&v).ok()),
        error,
        error_info,
        executor_id: col_str(s, "executor_id").unwrap_or_default(),
        app_version: col_str(s, "application_version").unwrap_or_default(),
        queue_name: col_str(s, "queue_name"),
        queue_partition_key: col_str(s, "queue_partition_key"),
        priority: col_i64(s, "priority").unwrap_or(0) as i32,
        dedup_id: col_str(s, "deduplication_id"),
        recovery_attempts: col_i64(s, "recovery_attempts").unwrap_or(0) as i32,
        parent_workflow_id: col_str(s, "parent_workflow_id"),
        timeout_ms: col_i64(s, "workflow_timeout_ms"),
        deadline_ms: col_i64(s, "workflow_deadline_epoch_ms"),
        started_at_ms: col_i64(s, "started_at_epoch_ms"),
        rate_limited: false,
        delay_until_ms: col_i64(s, "delay_until_epoch_ms"),
        completed_at_ms: None,
        forked_from: col_str(s, "forked_from"),
        authenticated_user: col_str(s, "authenticated_user"),
        assumed_role: col_str(s, "assumed_role"),
        authenticated_roles: decode_roles(col_str(s, "authenticated_roles").as_deref()),
        created_at: ms_to_dt(col_i64(s, "created_at")),
        updated_at: ms_to_dt(col_i64(s, "updated_at")),
    }
}

/// An `operation_outputs` row in portable form. The in-memory backend records no
/// step error, so `error` is always null.
fn step_to_map(wf_id: &str, seq: i32, r: &StepRow) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("workflow_uuid".into(), json!(wf_id));
    m.insert("function_id".into(), json!(seq));
    m.insert("function_name".into(), json!(r.name));
    m.insert(
        "output".into(),
        r.output.as_ref().map_or(Value::Null, payload_str),
    );
    m.insert("error".into(), Value::Null);
    m.insert("child_workflow_id".into(), json!(r.child_workflow_id));
    m.insert("started_at_epoch_ms".into(), json!(r.started_at_ms));
    m.insert("completed_at_epoch_ms".into(), json!(r.completed_at_ms));
    m
}

/// A `workflow_events` row in portable form.
fn event_to_map(wf_id: &str, key: &str, value: &Value) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("workflow_uuid".into(), json!(wf_id));
    m.insert("key".into(), json!(key));
    m.insert("value".into(), payload_str(value));
    m
}

/// A `streams` row in portable form (`offset` is the entry index; a `None` entry
/// is the close sentinel). The in-memory backend does not track `function_id`.
fn stream_to_map(wf_id: &str, key: &str, offset: i64, entry: &Option<Value>) -> Map<String, Value> {
    let value = match entry {
        Some(v) => payload_str(v),
        None => json!(STREAM_CLOSED_SENTINEL),
    };
    let mut m = Map::new();
    m.insert("workflow_uuid".into(), json!(wf_id));
    m.insert("key".into(), json!(key));
    m.insert("value".into(), value);
    m.insert("offset".into(), json!(offset));
    m.insert("function_id".into(), Value::Null);
    m
}
