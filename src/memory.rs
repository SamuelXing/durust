use crate::error::{Error, Result};
use crate::provider::{
    is_terminal, DequeueRequest, ListFilter, StateProvider, StepAggregate, StepAggregateQuery,
    StepInfo, WorkflowAggregate, WorkflowAggregateQuery, WorkflowStatus, STATUS_CANCELLED,
    STATUS_DELAYED, STATUS_ENQUEUED, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
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
        let mut counts: HashMap<Vec<(String, Option<String>)>, i64> = HashMap::new();

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
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut out: Vec<WorkflowAggregate> = counts
            .into_iter()
            .map(|(k, count)| WorkflowAggregate {
                group: k.into_iter().collect(),
                count,
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
}
