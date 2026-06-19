//! Out-of-process [`Client`]: a control-plane handle over a [`StateProvider`]
//! that enqueues work and observes it **without** registering or running any
//! workflows. Use it from an API server, a CLI, or any process that produces
//! work for a separate fleet of executors (running [`DurableEngine`]s) to pick
//! up and run.
//!
//! [`DurableEngine`]: crate::DurableEngine

use crate::engine::WorkflowOptions;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    ListFilter, StateProvider, StepInfo, VersionInfo, WorkflowStatus, STATUS_DELAYED,
    STATUS_ENQUEUED, STATUS_PENDING,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// How often [`Client::read_stream`] re-checks for new stream entries while the
/// producing workflow is still active.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// A registry-less, out-of-process handle over the system database.
///
/// Unlike [`DurableEngine`](crate::DurableEngine), a `Client` holds no workflow
/// registry and runs nothing: it [`enqueue`](Self::enqueue)s workflows for
/// executors to claim, [`send`](Self::send)s messages, reads events, and queries
/// workflow state. It is the producer/observer half of the split between
/// enqueueing work and executing it.
pub struct Client {
    provider: Arc<dyn StateProvider>,
    app_version: String,
}

impl Client {
    /// Build a client over an existing [`StateProvider`] (e.g. a
    /// [`PostgresProvider`](crate::PostgresProvider) connected to the system
    /// database). The database must already be initialized by the application.
    ///
    /// Enqueued work is left version-agnostic (empty application version), so any
    /// executor claims it regardless of its version. Use
    /// [`with_app_version`](Self::with_app_version) to gate it to one version.
    pub fn new(provider: Arc<dyn StateProvider>) -> Self {
        Self {
            provider,
            app_version: String::new(),
        }
    }

    /// Stamp enqueued workflows with this application version, so only an
    /// executor of the same version claims them (version-gated dispatch).
    pub fn with_app_version(mut self, version: impl Into<String>) -> Self {
        self.app_version = version.into();
        self
    }

    /// The underlying state provider.
    pub fn provider(&self) -> &Arc<dyn StateProvider> {
        &self.provider
    }

    /// Enqueue `workflow_name` on `queue_name` for an executor to claim and run.
    /// The workflow need not be registered in this process. Returns a polling
    /// [`WorkflowHandle`] over the result.
    ///
    /// The row is persisted `ENQUEUED` (or `DELAYED` when `opts.delay` is set);
    /// `opts.queue` is ignored — the queue is the first argument.
    pub async fn enqueue<I, O>(
        &self,
        queue_name: &str,
        workflow_name: &str,
        input: I,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        if queue_name.is_empty() {
            return Err(Error::app("queue name is required"));
        }
        if workflow_name.is_empty() {
            return Err(Error::app("workflow name is required"));
        }
        if opts.partition_key.is_some() && opts.dedup_id.is_some() {
            return Err(Error::app(
                "partition key and deduplication id cannot be used together",
            ));
        }

        let id = opts
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let now_ms = Utc::now().timestamp_millis();
        let status = if opts.delay.is_some() {
            STATUS_DELAYED
        } else {
            STATUS_ENQUEUED
        };

        // Unowned (empty executor) until a dispatcher claims it.
        let mut row = WorkflowStatus::new(
            &id,
            workflow_name,
            serde_json::to_value(input)?,
            status,
            "",
            &self.app_version,
        );
        row.queue_name = Some(queue_name.to_string());
        row.priority = opts.priority;
        row.queue_partition_key = opts.partition_key.clone();
        row.dedup_id = opts.dedup_id.clone();
        row.authenticated_user = opts.authenticated_user.clone();
        row.assumed_role = opts.assumed_role.clone();
        row.authenticated_roles = opts.authenticated_roles.clone();
        row.timeout_ms = opts.timeout.map(|d| d.as_millis() as i64);
        row.delay_until_ms = opts.delay.map(|d| now_ms + d.as_millis() as i64);

        self.provider.insert_workflow_status(row).await?;
        Ok(WorkflowHandle::polling(id, self.provider.clone()))
    }

    /// Send a message to a workflow (e.g. to nudge one waiting in
    /// [`DurableContext::recv`](crate::DurableContext::recv)). Not durable —
    /// there is no calling workflow to checkpoint into.
    pub async fn send<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
    ) -> Result<()> {
        self.provider
            .insert_notification(destination_id, topic, serde_json::to_value(message)?)
            .await
    }

    /// Read event `key` of a workflow, waiting up to `timeout` for it to be set.
    /// Returns `None` on timeout.
    pub async fn get_event<T: DeserializeOwned>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(value) = self
                .provider
                .get_event_value(target_workflow_id, key)
                .await?
            {
                return Ok(Some(serde_json::from_value(value)?));
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(25))).await;
        }
    }

    /// A polling [`WorkflowHandle`] for an existing workflow. Errors if no
    /// workflow exists under `id`.
    pub async fn retrieve_workflow<O>(&self, id: &str) -> Result<WorkflowHandle<O>> {
        self.provider
            .get_workflow_status(id)
            .await?
            .ok_or_else(|| Error::UnknownWorkflow(id.to_string()))?;
        Ok(WorkflowHandle::polling(
            id.to_string(),
            self.provider.clone(),
        ))
    }

    /// List workflows matching `filter`.
    pub async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        self.provider.list_workflows(filter).await
    }

    /// The recorded steps of a workflow, ordered by step id.
    pub async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.provider.get_workflow_steps(workflow_id).await
    }

    /// Cancel a workflow: a non-terminal one is set `CANCELLED` and removed from
    /// its queue; a running workflow stops at its next step.
    pub async fn cancel_workflow(&self, id: &str) -> Result<()> {
        self.provider.cancel_workflow(id).await
    }

    /// Cancel many workflows in one round-trip. Missing or already-terminal ids
    /// are skipped; an empty slice is a no-op.
    pub async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        self.provider.cancel_workflows(ids).await
    }

    /// Delete many workflows and (via `ON DELETE CASCADE`) their step / event /
    /// stream rows. When `delete_children`, every descendant by
    /// `parent_workflow_id` is removed too. Missing ids are skipped.
    pub async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        self.provider.delete_workflows(ids, delete_children).await
    }

    /// Reschedule a `DELAYED` workflow to become eligible `delay` from now. A
    /// running executor's dispatcher promotes it once due. Returns `false` (no
    /// error) if no `DELAYED` row matched.
    pub async fn set_workflow_delay(&self, id: &str, delay: Duration) -> Result<bool> {
        let until = Utc::now().timestamp_millis() + delay.as_millis() as i64;
        self.provider.set_workflow_delay(id, until).await
    }

    /// Like [`set_workflow_delay`](Self::set_workflow_delay) but with an absolute
    /// instant rather than an offset from now.
    pub async fn set_workflow_delay_until(&self, id: &str, at: DateTime<Utc>) -> Result<bool> {
        self.provider
            .set_workflow_delay(id, at.timestamp_millis())
            .await
    }

    /// Read a workflow's stream `key` in order, blocking until the stream closes
    /// or the producing workflow goes inactive. Returns the values and whether
    /// the stream is closed.
    pub async fn read_stream<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<(Vec<T>, bool)> {
        let mut all = Vec::new();
        let mut offset = 0_i32;
        loop {
            let (values, closed) = self.provider.read_stream(workflow_id, key, offset).await?;
            offset += values.len() as i32;
            for v in values {
                all.push(serde_json::from_value(v)?);
            }
            if closed {
                return Ok((all, true));
            }
            match self.provider.get_workflow_status(workflow_id).await? {
                None => return Err(Error::nonexistent_workflow(workflow_id)),
                Some(s) if s.status != STATUS_PENDING && s.status != STATUS_ENQUEUED => {
                    return Ok((all, true));
                }
                _ => {}
            }
            tokio::time::sleep(STREAM_POLL_INTERVAL).await;
        }
    }

    /// Read the currently-available values of stream `key` from `from_offset`
    /// without blocking. Returns the values in order and whether the stream is
    /// closed. Pass the count read so far as the next `from_offset` to poll.
    pub async fn read_stream_snapshot<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<T>, bool)> {
        let (values, closed) = self
            .provider
            .read_stream(workflow_id, key, from_offset)
            .await?;
        let out = values
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<T>, _>>()?;
        Ok((out, closed))
    }

    /// Every registered application version, newest first.
    pub async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.provider.list_application_versions().await
    }

    /// The latest registered application version, or `None` if none are
    /// registered.
    pub async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        self.provider.get_latest_application_version().await
    }

    /// Mark a registered version as latest (bumps its `version_timestamp`).
    /// Returns whether a matching version existed; rejects an empty name.
    pub async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        if version_name.is_empty() {
            return Err(Error::app("version_name is required"));
        }
        self.provider
            .set_latest_application_version(version_name)
            .await
    }
}
