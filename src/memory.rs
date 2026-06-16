use crate::error::{Error, Result};
use crate::provider::{
    is_terminal, DequeueRequest, ListFilter, StateProvider, StepInfo, WorkflowStatus,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
    STATUS_PENDING,
};
use async_trait::async_trait;
use chrono::Utc;
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
/// a step `output` or a `child_workflow_id` (a started child workflow).
#[derive(Clone, Default)]
struct StepRow {
    name: String,
    output: Option<Value>,
    child_workflow_id: Option<String>,
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
    ) -> Result<Value> {
        let mut g = self.inner.lock().await;
        let canonical = g
            .steps
            .entry((workflow_id.to_string(), seq))
            .or_insert_with(|| StepRow {
                name: name.to_string(),
                output: Some(value),
                child_workflow_id: None,
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

        // Rate limiter: count rate-limited starts within the trailing window.
        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let cutoff = now_ms - period_ms;
            let recent = g
                .workflows
                .values()
                .filter(|w| {
                    w.queue_name.as_deref() == Some(req.queue_name.as_str())
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
        Ok(rows)
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
                started_at: None,
                completed_at: None,
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
