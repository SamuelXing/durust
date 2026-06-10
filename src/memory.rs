use crate::error::Result;
use crate::provider::{StateProvider, WorkflowStatus, STATUS_PENDING};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[derive(Default)]
struct Inner {
    workflows: HashMap<String, WorkflowStatus>,
    steps: HashMap<(String, i32), Value>,
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
}
