use crate::error::{Error, Result};
use crate::provider::{is_terminal, StateProvider, WorkflowStatus, STATUS_CANCELLED, STATUS_ERROR};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// A reference to a workflow execution.
///
/// Returned by [`crate::DurableEngine::run_workflow`] (and, later,
/// `enqueue` / `retrieve_workflow`). It lets the caller await the workflow's
/// result without blocking the call that started it.
///
/// Two flavors:
/// - **Local**: started on this process, so we hold its [`JoinHandle`] and
///   `get_result` awaits the task directly.
/// - **Polling**: obtained for a workflow running elsewhere (or already
///   persisted); `get_result` polls the status row until it reaches a terminal
///   state. This is how DBOS handles cross-process / post-restart waits.
pub struct WorkflowHandle<O> {
    id: String,
    provider: Arc<dyn StateProvider>,
    join: Option<JoinHandle<Result<Value>>>,
    poll_interval: Duration,
    _marker: PhantomData<O>,
}

impl<O> WorkflowHandle<O> {
    /// Handle for a workflow whose task this process owns.
    pub(crate) fn local(
        id: String,
        provider: Arc<dyn StateProvider>,
        join: JoinHandle<Result<Value>>,
    ) -> Self {
        Self {
            id,
            provider,
            join: Some(join),
            poll_interval: Duration::from_millis(100),
            _marker: PhantomData,
        }
    }

    /// Handle for a workflow this process does not own; results are observed by
    /// polling the state backend.
    pub(crate) fn polling(id: String, provider: Arc<dyn StateProvider>) -> Self {
        Self {
            id,
            provider,
            join: None,
            poll_interval: Duration::from_millis(100),
            _marker: PhantomData,
        }
    }

    /// The workflow id (its idempotency key).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The current persisted status row.
    pub async fn get_status(&self) -> Result<WorkflowStatus> {
        self.provider
            .get_workflow_status(&self.id)
            .await?
            .ok_or_else(|| Error::UnknownWorkflow(self.id.clone()))
    }
}

impl<O: DeserializeOwned> WorkflowHandle<O> {
    /// Wait for the workflow to finish and return its typed output.
    ///
    /// For a local handle this awaits the in-process task. For a polling handle
    /// it reads the status row every `poll_interval` until the workflow reaches a
    /// terminal state, then deserializes the stored output. A `CANCELLED`
    /// workflow yields [`Error::Cancelled`]; an `ERROR` workflow yields the
    /// recorded application error.
    pub async fn get_result(&mut self) -> Result<O> {
        if let Some(join) = self.join.take() {
            // Own the task: await it directly, surfacing panics as errors.
            return match join.await {
                Ok(res) => {
                    let value = res?;
                    Ok(serde_json::from_value(value)?)
                }
                Err(e) => Err(Error::app(format!("workflow task failed: {e}"))),
            };
        }

        // No task: poll the status row until terminal. A polling handle may name
        // a workflow that does not exist yet (e.g. a debounced run the collector
        // has not started); treat that as "not ready" and keep waiting.
        loop {
            match self.get_status().await {
                Ok(status) if is_terminal(&status.status) => {
                    return self.terminal_to_result(status);
                }
                Ok(_) | Err(Error::UnknownWorkflow(_)) => {}
                Err(e) => return Err(e),
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Convert a terminal status row into a typed `Result`.
    fn terminal_to_result(&self, status: WorkflowStatus) -> Result<O> {
        match status.status.as_str() {
            STATUS_CANCELLED => Err(Error::Cancelled(self.id.clone())),
            STATUS_ERROR => Err(Error::app(
                status
                    .error
                    .unwrap_or_else(|| "workflow failed".to_string()),
            )),
            _ => {
                let output = status.output.unwrap_or(Value::Null);
                Ok(serde_json::from_value(output)?)
            }
        }
    }
}
