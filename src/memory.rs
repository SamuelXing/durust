use crate::error::{Error, Result};
use crate::provider::{
    DequeueRequest, StateProvider, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_PENDING,
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

#[derive(Default)]
struct Inner {
    workflows: HashMap<String, WorkflowStatus>,
    steps: HashMap<(String, i32), Value>,
    notifications: Vec<NotificationRow>,
    /// Workflow events keyed by `(workflow_id, key)`.
    events: HashMap<(String, String), Value>,
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
                return Err(Error::app(format!(
                    "deduplication id `{dedup}` already in use on queue `{queue}`"
                )));
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
            row.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let g = self.inner.lock().await;
        Ok(g.steps.get(&(workflow_id.to_string(), seq)).cloned())
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        _name: &str,
        value: Value,
    ) -> Result<Value> {
        let mut g = self.inner.lock().await;
        let canonical = g
            .steps
            .entry((workflow_id.to_string(), seq))
            .or_insert(value)
            .clone();
        Ok(canonical)
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowStatus>> {
        let g = self.inner.lock().await;
        Ok(g.workflows
            .values()
            .filter(|r| r.status == STATUS_PENDING)
            .cloned()
            .collect())
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
                        && w.started_at_ms.map_or(false, |t| t > cutoff)
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

        // Candidates ordered by (priority, created_at), version-gated like Go.
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
            if w.status == STATUS_DELAYED && w.delay_until_ms.map_or(false, |t| t <= now_ms) {
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
            return Err(Error::app(format!(
                "cannot send to nonexistent workflow `{destination_id}`"
            )));
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
        _step_name: &str,
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
            .or_insert(message)
            .clone();
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
}
