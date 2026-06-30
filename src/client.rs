//! Out-of-process [`Client`]: a control-plane handle over a [`StateProvider`]
//! that enqueues work and observes it **without** registering or running any
//! workflows. Use it from an API server, a CLI, or any process that produces
//! work for a separate fleet of executors (running [`DurableEngine`]s) to pick
//! up and run.
//!
//! [`DurableEngine`]: crate::DurableEngine

use crate::engine::{
    cron_ticks_between, parse_cron, parse_timezone, DeduplicationPolicy, WorkflowOptions,
    INTERNAL_QUEUE,
};
use crate::error::{Error, ErrorCode, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    ListFilter, StateProvider, StepInfo, VersionInfo, WorkflowStatus, STATUS_DELAYED,
    STATUS_ENQUEUED,
};
use crate::schedule::{
    ApplySchedule, ScheduleFilter, ScheduleOptions, ScheduleStatus, WorkflowSchedule,
};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

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
        if opts.dedup_policy != DeduplicationPolicy::Reject && opts.dedup_id.is_none() {
            return Err(Error::app(
                "a deduplication policy requires a deduplication id",
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
        // A per-enqueue version override, else the client default (empty ⇒ any
        // executor claims it).
        let app_version = opts.app_version.as_deref().unwrap_or(&self.app_version);

        // Unowned (empty executor) until a dispatcher claims it.
        let mut row = WorkflowStatus::new(
            &id,
            workflow_name,
            serde_json::to_value(input)?,
            status,
            "",
            app_version,
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

        // On a dedup collision under `ReturnExisting`, return a handle to the
        // workflow already holding the slot; retry if it was freed in between.
        loop {
            match self.provider.insert_workflow_status(row.clone()).await {
                Ok(_) => return Ok(WorkflowHandle::polling(id, self.provider.clone())),
                Err(e)
                    if opts.dedup_policy == DeduplicationPolicy::ReturnExisting
                        && e.code() == ErrorCode::QueueDeduplicated =>
                {
                    if let Some(existing) = self
                        .provider
                        .get_deduplicated_workflow(
                            queue_name,
                            opts.dedup_id.as_deref().unwrap_or(""),
                        )
                        .await?
                    {
                        return Ok(WorkflowHandle::polling(existing, self.provider.clone()));
                    }
                }
                Err(e) => return Err(e),
            }
        }
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

    /// Resume a cancelled (or otherwise non-terminal) workflow: re-queue it onto
    /// the internal queue so a dispatcher on a live engine re-runs it from its
    /// checkpoints. Returns a polling handle. Errors if the workflow is missing or
    /// already `SUCCESS`/`ERROR`.
    pub async fn resume_workflow<O>(&self, id: &str) -> Result<WorkflowHandle<O>> {
        if !self.provider.resume_workflow(id).await? {
            return Err(Error::app(format!(
                "workflow `{id}` cannot be resumed (missing or already completed)"
            )));
        }
        self.requeue_for_rerun(id).await?;
        Ok(WorkflowHandle::polling(
            id.to_string(),
            self.provider.clone(),
        ))
    }

    /// Resume many workflows in one round-trip; returns a polling handle for each
    /// id actually transitioned (skipped ids yield no handle).
    pub async fn resume_workflows<O>(&self, ids: &[String]) -> Result<Vec<WorkflowHandle<O>>> {
        let resumed = self.provider.resume_workflows(ids).await?;
        let mut handles = Vec::with_capacity(resumed.len());
        for id in resumed {
            self.requeue_for_rerun(&id).await?;
            handles.push(WorkflowHandle::polling(id, self.provider.clone()));
        }
        Ok(handles)
    }

    /// Fork a workflow from `start_step`: a new workflow reuses the original's
    /// checkpoints for steps `< start_step` and re-executes from there. Queued
    /// onto the internal queue for a dispatcher to run; returns a polling handle.
    /// The new id comes from `opts.workflow_id` or is generated.
    pub async fn fork_workflow<O>(
        &self,
        original_id: &str,
        start_step: i32,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>> {
        let new_id = opts
            .workflow_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        self.provider
            .fork_workflow(original_id, &new_id, start_step, "")
            .await?;
        self.requeue_for_rerun(&new_id).await?;
        Ok(WorkflowHandle::polling(new_id, self.provider.clone()))
    }

    /// Put an existing row onto the internal queue so a dispatcher on a live
    /// engine re-runs it. Re-execution always uses the internal queue (it is
    /// always dispatched), so it makes progress regardless of which user queues
    /// the executors listen to.
    async fn requeue_for_rerun(&self, id: &str) -> Result<()> {
        self.provider.enqueue_existing(id, INTERNAL_QUEUE).await
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
        crate::provider::drain_stream(self.provider.as_ref(), workflow_id, key).await
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
        crate::provider::snapshot_stream(self.provider.as_ref(), workflow_id, key, from_offset)
            .await
    }

    /// Read a workflow's stream `key` as an asynchronous
    /// [`Stream`](futures_util::Stream), yielding each value in order as it is
    /// committed — the incremental counterpart to [`read_stream`](Self::read_stream),
    /// which blocks and returns the whole stream at once. The stream ends when the
    /// producer closes it or goes inactive; a decode or backend failure (or a
    /// missing workflow) is the final `Err` item. Consume it with
    /// [`StreamExt::next`](futures_util::StreamExt::next).
    pub fn read_stream_values<T: DeserializeOwned + 'static>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> impl futures_util::Stream<Item = Result<T>> + '_ {
        crate::provider::stream_values(self.provider.as_ref(), workflow_id, key)
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

    /// Create a durable cron schedule firing `workflow_name` on each tick of
    /// `cron`. Validates the cron and timezone, but — unlike the engine — does
    /// not require the workflow to be registered here: the executor that runs the
    /// schedule owns that. Errors on an invalid spec or a duplicate name.
    pub async fn create_schedule(
        &self,
        schedule_name: &str,
        workflow_name: &str,
        cron: &str,
        opts: ScheduleOptions,
    ) -> Result<()> {
        if schedule_name.is_empty() {
            return Err(Error::app("schedule_name is required"));
        }
        parse_cron(cron)?;
        if let Some(tz) = &opts.cron_timezone {
            parse_timezone(tz)?;
        }
        let schedule = WorkflowSchedule {
            schedule_id: uuid::Uuid::new_v4().to_string(),
            schedule_name: schedule_name.to_string(),
            workflow_name: workflow_name.to_string(),
            schedule: cron.to_string(),
            status: ScheduleStatus::Active,
            context: opts.context,
            last_fired_at: None,
            automatic_backfill: opts.automatic_backfill,
            cron_timezone: opts.cron_timezone,
            queue_name: opts.queue_name,
        };
        self.provider.create_schedule(&schedule).await
    }

    /// Create or replace each schedule by name, in one call (validated whole
    /// before any write). Like [`create_schedule`](Self::create_schedule), the
    /// workflow need not be registered here.
    pub async fn apply_schedules(&self, schedules: Vec<ApplySchedule>) -> Result<()> {
        for req in &schedules {
            if req.schedule_name.is_empty() {
                return Err(Error::app("schedule_name is required"));
            }
            parse_cron(&req.schedule)?;
            if let Some(tz) = &req.options.cron_timezone {
                parse_timezone(tz)?;
            }
        }
        // Build the replacement set up front, then apply the whole batch in one
        // transaction so it is all-or-nothing (a mid-batch failure rolls back,
        // leaving any schedules the batch would have replaced untouched).
        let built: Vec<WorkflowSchedule> = schedules
            .into_iter()
            .map(|req| WorkflowSchedule {
                schedule_id: uuid::Uuid::new_v4().to_string(),
                schedule_name: req.schedule_name,
                workflow_name: req.workflow_name,
                schedule: req.schedule,
                status: ScheduleStatus::Active,
                context: req.options.context,
                last_fired_at: None,
                automatic_backfill: req.options.automatic_backfill,
                cron_timezone: req.options.cron_timezone,
                queue_name: req.options.queue_name,
            })
            .collect();
        self.provider.apply_schedules(&built).await
    }

    /// The schedule named `schedule_name`, or `None` if there is none.
    pub async fn get_schedule(&self, schedule_name: &str) -> Result<Option<WorkflowSchedule>> {
        let schedules = self
            .provider
            .list_schedules(&ScheduleFilter {
                name_prefixes: vec![schedule_name.to_string()],
                ..Default::default()
            })
            .await?;
        Ok(schedules
            .into_iter()
            .find(|s| s.schedule_name == schedule_name))
    }

    /// All schedules matching `filter` (a default filter returns every
    /// schedule), ordered by name.
    pub async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        self.provider.list_schedules(filter).await
    }

    /// Pause a schedule so it stops firing. Returns whether a schedule matched.
    pub async fn pause_schedule(&self, schedule_name: &str) -> Result<bool> {
        self.provider
            .set_schedule_status(schedule_name, ScheduleStatus::Paused)
            .await
    }

    /// Resume a paused schedule. Returns whether a schedule matched.
    pub async fn resume_schedule(&self, schedule_name: &str) -> Result<bool> {
        self.provider
            .set_schedule_status(schedule_name, ScheduleStatus::Active)
            .await
    }

    /// Delete a schedule. Returns whether a schedule was removed.
    pub async fn delete_schedule(&self, schedule_name: &str) -> Result<bool> {
        self.provider.delete_schedule(schedule_name).await
    }

    /// Fire a schedule's workflow once, immediately, for an engine to run.
    /// Returns a polling [`WorkflowHandle`] over the run. The tick uses a
    /// distinct `sched-{name}-trigger-{time}` id, so it never collides with or
    /// replaces a regular cron tick.
    ///
    /// A schedule with a queue routes the run there; a direct (queue-less)
    /// schedule routes to the internal queue — the client runs nothing, so a
    /// live engine's always-on internal dispatcher executes it.
    pub async fn trigger_schedule<O>(&self, schedule_name: &str) -> Result<WorkflowHandle<O>> {
        let schedule = self
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| Error::app(format!("schedule not found: {schedule_name}")))?;
        let stamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let id = format!("sched-{schedule_name}-trigger-{stamp}");
        let queue = schedule.queue_name.as_deref().unwrap_or(INTERNAL_QUEUE);
        self.enqueue(
            queue,
            &schedule.workflow_name,
            stamp,
            WorkflowOptions::with_id(id),
        )
        .await
    }

    /// Enqueue a schedule's ticks for every cron instant in `(start, end)` (both
    /// bounds exclusive) for an engine to run, under the same deterministic
    /// per-tick ids the live loop uses — so a tick that already ran is skipped,
    /// not duplicated. Returns the id of every tick in the range, in order
    /// (including skipped ones).
    ///
    /// Like [`trigger_schedule`](Self::trigger_schedule), a direct (queue-less)
    /// schedule routes its ticks to the internal queue.
    pub async fn backfill_schedule(
        &self,
        schedule_name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<String>> {
        let schedule = self
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| Error::app(format!("schedule not found: {schedule_name}")))?;
        let cron = parse_cron(&schedule.schedule)?;
        let queue = schedule.queue_name.as_deref().unwrap_or(INTERNAL_QUEUE);
        let mut ids = Vec::new();
        for instant in cron_ticks_between(&cron, schedule.cron_timezone.as_deref(), start, end) {
            let stamp = instant.to_rfc3339();
            let id = format!("sched-{schedule_name}-{stamp}");
            self.enqueue::<_, serde_json::Value>(
                queue,
                &schedule.workflow_name,
                stamp,
                WorkflowOptions::with_id(id.clone()),
            )
            .await?;
            ids.push(id);
        }
        Ok(ids)
    }
}
