use crate::context::{AuthContext, DurableContext};
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    is_terminal, DequeueRequest, ListFilter, StateProvider, StepInfo, WorkflowStatus,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR, STATUS_PENDING,
    STATUS_SUCCESS,
};
use crate::queue::WorkflowQueue;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// How often a blocking [`DurableEngine::read_stream`] re-checks the backend for
/// newly written stream entries while the producer is still active.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// A type-erased workflow handler: takes a context + JSON input, returns JSON output.
pub type WorkflowFn = Arc<
    dyn Fn(DurableContext, Value) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>>
        + Send
        + Sync,
>;

/// Erase a typed `async fn(DurableContext, Input) -> Result<Output>` into the
/// JSON-in / JSON-out [`WorkflowFn`] the engine stores.
///
/// This is the single place input/output (de)serialization happens. Both
/// [`DurableEngine::register`] and the `#[durust::workflow]` macro funnel through
/// it, so the manual and auto-registered paths behave identically.
pub fn erase<I, O, F, Fut>(f: F) -> WorkflowFn
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |ctx, input_json| {
        let f = f.clone();
        Box::pin(async move {
            let input: I = serde_json::from_value(input_json)?;
            let output: O = f(ctx, input).await?;
            Ok(serde_json::to_value(output)?)
        })
    })
}

/// A compile-time workflow registration emitted by `#[durust::workflow]`.
///
/// Collected via the `inventory` crate: every annotated workflow in the binary
/// submits one of these, and [`DurableEngine::new`] iterates them so no manual
/// `register` call is needed.
pub struct WorkflowRegistration {
    /// The name the workflow is registered (and persisted) under.
    pub name: &'static str,
    /// Builds the type-erased handler. Typically `|| durust::erase(my_fn)`.
    pub builder: fn() -> WorkflowFn,
    /// A cron spec (6-field, second precision) if this is a scheduled workflow;
    /// emitted by `#[workflow(schedule = "...")]`. `None` otherwise.
    pub schedule: Option<&'static str>,
}

inventory::collect!(WorkflowRegistration);

/// Per-workflow start options.
///
/// `timeout` fixes a deadline when the workflow starts (at claim time for
/// queued workflows); a run that overruns it is cancelled.
#[derive(Clone, Default)]
pub struct WorkflowOptions {
    /// Explicit idempotency key. If `None`, a uuid is generated.
    pub workflow_id: Option<String>,
    /// Queue-scoped deduplication key.
    pub dedup_id: Option<String>,
    /// Route through this queue instead of running inline.
    pub queue: Option<String>,
    /// Dispatch priority within a queue; lower runs first.
    pub priority: i32,
    /// Partition key for a partitioned queue (see
    /// [`WorkflowQueue::partitioned`](crate::WorkflowQueue::partitioned)). Each
    /// partition gets its own concurrency / rate-limit budget. Ignored by
    /// non-partitioned queues and direct runs.
    pub partition_key: Option<String>,
    /// Wall-clock deadline for the whole workflow.
    pub timeout: Option<Duration>,
    /// Delay before the workflow becomes eligible to run (queued workflows
    /// only): it sits in `DELAYED` until the dispatcher transitions it.
    pub delay: Option<Duration>,
    /// User on whose behalf the workflow runs; persisted and readable from the
    /// workflow via [`DurableContext::authenticated_user`].
    pub authenticated_user: Option<String>,
    /// Role assumed for this run.
    pub assumed_role: Option<String>,
    /// Roles available to the authenticated user.
    pub authenticated_roles: Vec<String>,
}

impl WorkflowOptions {
    /// Convenience: options that only pin the workflow id.
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            workflow_id: Some(id.into()),
            ..Default::default()
        }
    }

    /// Set the user the workflow runs on behalf of.
    pub fn authenticated_user(mut self, user: impl Into<String>) -> Self {
        self.authenticated_user = Some(user.into());
        self
    }

    /// Set the role assumed for this run.
    pub fn assumed_role(mut self, role: impl Into<String>) -> Self {
        self.assumed_role = Some(role.into());
        self
    }

    /// Set the partition key for a partitioned queue.
    pub fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Set the roles available to the authenticated user.
    pub fn authenticated_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.authenticated_roles = roles.into_iter().map(Into::into).collect();
        self
    }
}

/// The durable execution engine.
///
/// Holds the state backend, a registry of workflow functions by name, and this
/// process's identity (`executor_id`, `app_version`). There is no separate
/// server process: the engine is a library that lives in your worker and talks
/// directly to the [`StateProvider`].
pub struct DurableEngine {
    provider: Arc<dyn StateProvider>,
    workflows: HashMap<String, WorkflowFn>,
    queues: HashMap<String, Arc<WorkflowQueue>>,
    /// If set, only these registered queues get a dispatcher at
    /// [`launch`](Self::launch); `None` dispatches every registered queue. Set
    /// by [`listen_queues`](Self::listen_queues).
    listen_filter: Option<std::collections::HashSet<String>>,
    /// `(workflow_name, cron_spec)` for `#[workflow(schedule = …)]` workflows;
    /// each gets a scheduler task in [`launch`](Self::launch).
    scheduled: Vec<(String, String)>,
    executor_id: String,
    app_version: String,
    /// Recovery re-dispatches beyond this count park the workflow in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
    max_recovery_attempts: i32,
    /// Flipped by [`shutdown`](Self::shutdown); background loops observe it.
    shutting_down: Arc<AtomicBool>,
    /// Count of workflow tasks this process is currently running, so
    /// [`shutdown`](Self::shutdown) can drain before returning.
    inflight: Arc<AtomicUsize>,
    /// Per-queue dispatcher tasks spawned by [`launch`](Self::launch).
    dispatchers: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Shared execution core, built once from the registrations on first run and
    /// reused by every run path (and by workflows that start child workflows).
    runtime: std::sync::OnceLock<Arc<Runtime>>,
}

impl DurableEngine {
    /// Create an engine with a generated executor id and a default app version.
    ///
    /// Every workflow annotated with `#[durust::workflow]` anywhere in the binary
    /// is auto-registered here (via `inventory`).
    pub async fn new(provider: Arc<dyn StateProvider>) -> Result<Self> {
        Self::new_with_version(provider, "0.1.0").await
    }

    /// Like [`new`](Self::new) but pins the application version used to stamp and
    /// version-gate workflows (recovery only re-runs rows of a matching version).
    pub async fn new_with_version(
        provider: Arc<dyn StateProvider>,
        app_version: impl Into<String>,
    ) -> Result<Self> {
        provider.init().await?;
        let mut workflows = HashMap::new();
        let mut scheduled = Vec::new();
        for reg in inventory::iter::<WorkflowRegistration> {
            workflows.insert(reg.name.to_string(), (reg.builder)());
            if let Some(spec) = reg.schedule {
                scheduled.push((reg.name.to_string(), spec.to_string()));
            }
        }
        Ok(Self {
            provider,
            workflows,
            queues: HashMap::new(),
            listen_filter: None,
            scheduled,
            executor_id: uuid::Uuid::new_v4().to_string(),
            app_version: app_version.into(),
            max_recovery_attempts: 100,
            shutting_down: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
            dispatchers: std::sync::Mutex::new(Vec::new()),
            runtime: std::sync::OnceLock::new(),
        })
    }

    /// The shared execution core, built once from the current registrations.
    ///
    /// Built lazily on the first run so all `register`/`register_queue` calls are
    /// captured; registering after the first workflow starts is not reflected
    /// (register before running, as you would before `launch`).
    fn runtime(&self) -> Arc<Runtime> {
        self.runtime
            .get_or_init(|| {
                Arc::new(Runtime {
                    provider: self.provider.clone(),
                    workflows: self.workflows.clone(),
                    queues: self.queues.clone(),
                    executor_id: self.executor_id.clone(),
                    app_version: self.app_version.clone(),
                    inflight: self.inflight.clone(),
                })
            })
            .clone()
    }

    /// This process's unique executor id.
    pub fn executor_id(&self) -> &str {
        &self.executor_id
    }

    /// The application version stamped onto workflows started here.
    pub fn app_version(&self) -> &str {
        &self.app_version
    }

    /// Register a workflow under `name`.
    ///
    /// The handler is a plain async function `(DurableContext, Input) -> Result<Output>`.
    /// `Input` and `Output` only need to be serde-serializable.
    pub fn register<I, O, F, Fut>(&mut self, name: &str, f: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.workflows.insert(name.to_string(), erase(f));
    }

    /// Register a durable queue. Must be called before [`launch`](Self::launch);
    /// enqueueing to an unregistered queue is an error.
    pub fn register_queue(&mut self, queue: WorkflowQueue) {
        self.queues.insert(queue.name.clone(), Arc::new(queue));
    }

    /// Restrict which registered queues this process dispatches at
    /// [`launch`](Self::launch) to the named subset. By default every registered
    /// queue gets a dispatcher; call this (before `launch`) to have a process
    /// that *enqueues* to many queues but only *runs* work from some of them.
    /// Names that are not registered are ignored. Must be called before `launch`.
    pub fn listen_queues<I, S>(&mut self, names: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.listen_filter = Some(names.into_iter().map(Into::into).collect());
    }

    /// All queues registered on this engine, sorted by name. Includes queues this
    /// process does not dispatch (see [`listen_queues`](Self::listen_queues)).
    pub fn list_registered_queues(&self) -> Vec<WorkflowQueue> {
        let mut queues: Vec<WorkflowQueue> = self.queues.values().map(|q| (**q).clone()).collect();
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        queues
    }

    /// Start background processing: one dispatcher task per registered queue and
    /// one scheduler task per `#[workflow(schedule = …)]` workflow. Workflow
    /// timeouts are enforced inline per run, so they need no separate sweep. Call
    /// once per launch; safe to call again after `shutdown`.
    pub async fn launch(&self) -> Result<()> {
        self.shutting_down.store(false, Ordering::Relaxed);
        let rt = self.runtime();
        let mut tasks = self.dispatchers.lock().expect("dispatcher lock poisoned");
        for queue in self.queues.values() {
            // Skip queues this process is configured not to listen to.
            if let Some(listen) = &self.listen_filter {
                if !listen.contains(&queue.name) {
                    continue;
                }
            }
            tasks.push(tokio::spawn(queue_dispatch_loop(
                queue.clone(),
                rt.clone(),
                self.shutting_down.clone(),
            )));
        }
        for (name, spec) in &self.scheduled {
            let Some(handler) = rt.workflows.get(name).cloned() else {
                continue;
            };
            tasks.push(tokio::spawn(schedule_loop(
                name.clone(),
                spec.clone(),
                handler,
                rt.clone(),
                self.shutting_down.clone(),
            )));
        }
        Ok(())
    }

    /// Stop the queue dispatchers and wait for in-flight workflow tasks started
    /// here to drain (up to `timeout`).
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.shutting_down.store(true, Ordering::Relaxed);
        // Stop claiming new work first, then drain what is already running. An
        // aborted dispatcher can leave a freshly claimed workflow PENDING; the
        // next launch's recover() re-runs it from its checkpoints.
        for d in self
            .dispatchers
            .lock()
            .expect("dispatcher lock poisoned")
            .drain(..)
        {
            d.abort();
        }
        let deadline = std::time::Instant::now() + timeout;
        while self.inflight.load(Ordering::Acquire) > 0 {
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Start (or attach to) a workflow and return a [`WorkflowHandle`] **without
    /// blocking** on its completion.
    ///
    /// The status row is created idempotently under the resolved id. Without a
    /// queue, the workflow runs immediately on a spawned task. With
    /// `opts.queue` set, the row is persisted `ENQUEUED` (or `DELAYED` when
    /// `opts.delay` is set) and a polling handle is returned — a dispatcher on
    /// any executor claims and runs it. If the id already exists in a terminal
    /// state, a polling handle over the stored result is returned instead of
    /// re-running.
    pub async fn run_workflow<I, O>(
        &self,
        name: &str,
        input: I,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        let rt = self.runtime();
        let id = opts
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let input_json = serde_json::to_value(input)?;
        let handler = rt
            .workflows
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(name.to_string()))?;

        // A top-level run takes its identity from the options.
        let auth = AuthContext {
            authenticated_user: opts.authenticated_user.clone(),
            assumed_role: opts.assumed_role.clone(),
            authenticated_roles: opts.authenticated_roles.clone(),
        };
        let (canonical, queued) = rt
            .insert_run(&id, name, input_json, &opts, None, &auth)
            .await?;

        // Terminal already, or owned by a queue: observe via polling.
        if queued || is_terminal(&canonical.status) {
            return Ok(WorkflowHandle::polling(id, self.provider.clone()));
        }

        let join = rt.spawn_owned(
            id.clone(),
            handler,
            canonical.input,
            canonical.deadline_ms,
            auth,
        );
        Ok(WorkflowHandle::local(id, self.provider.clone(), join))
    }

    /// Enqueue a workflow on a registered queue.
    /// Sugar for [`run_workflow`](Self::run_workflow) with
    /// `opts.queue` set; the returned handle observes the workflow by polling,
    /// since any executor may claim and run it.
    pub async fn enqueue<I, O>(
        &self,
        queue_name: &str,
        workflow_name: &str,
        input: I,
        mut opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        opts.queue = Some(queue_name.to_string());
        self.run_workflow(workflow_name, input, opts).await
    }

    /// Send a message to a workflow from **outside** any workflow (e.g. an API
    /// handler nudging a waiting workflow). Not durable — there is no calling
    /// workflow to checkpoint into; from workflow code use
    /// [`DurableContext::send`] instead.
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

    /// Read event `key` of a workflow from **outside** any workflow, waiting up
    /// to `timeout` for it to be set. Returns `None` on timeout. From workflow
    /// code use [`DurableContext::get_event`], which is durable.
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

    /// Start a workflow under `id` and **block** until it returns its JSON
    /// output. Back-compat shim over [`run_workflow`](Self::run_workflow).
    pub async fn start<I>(&self, name: &str, id: &str, input: I) -> Result<Value>
    where
        I: Serialize,
    {
        let mut handle: WorkflowHandle<Value> = self
            .run_workflow(name, input, WorkflowOptions::with_id(id))
            .await?;
        handle.get_result().await
    }

    /// Like [`start`](Self::start) but deserializes the output into `O`.
    pub async fn start_typed<I, O>(&self, name: &str, id: &str, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let mut handle: WorkflowHandle<O> = self
            .run_workflow(name, input, WorkflowOptions::with_id(id))
            .await?;
        handle.get_result().await
    }

    /// Get a [`WorkflowHandle`] for an existing workflow. The handle observes the
    /// workflow by polling, so it works regardless of which executor is running
    /// it. Errors if no workflow exists under `id`.
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

    /// List a workflow's recorded operations. Returns each durable step / sleep /
    /// send / child invocation as a [`StepInfo`], ordered by step id. Empty for
    /// an unknown workflow or one that has run no steps.
    pub async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.provider.get_workflow_steps(workflow_id).await
    }

    /// Read the durable stream `key` produced by `workflow_id`, blocking until it
    /// is closed (a producer called [`close_stream`](crate::DurableContext::close_stream))
    /// or the producing workflow becomes inactive (no longer `PENDING`/`ENQUEUED`).
    /// Returns every value written, in order, and whether the stream is closed.
    ///
    /// Values are drained as the producer writes them; this polls the backend
    /// until the end condition is met. For a non-blocking read of what is
    /// currently available, use [`read_stream_snapshot`](Self::read_stream_snapshot).
    /// Errors if the workflow does not exist.
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
            // No close sentinel yet: keep reading only while the producer is
            // still active. Once it is gone, no more values can arrive.
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

    /// Read the currently-available values of stream `key` on `workflow_id`
    /// starting at `from_offset`, without blocking. Returns the values in order
    /// and whether the close sentinel has been reached. Use this to poll a stream
    /// incrementally; pass the count read so far as the next `from_offset`.
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

    /// Cancel a workflow. A non-terminal workflow is set `CANCELLED` and removed
    /// from its queue; a running workflow stops at its next step (cooperative
    /// cancellation).
    pub async fn cancel_workflow(&self, id: &str) -> Result<()> {
        self.provider.cancel_workflow(id).await
    }

    /// Resume a cancelled (or otherwise non-terminal) workflow. The workflow is
    /// returned to `PENDING` and re-run from its checkpoints; the returned handle
    /// tracks the new run. Errors if the workflow does not exist or is already
    /// `SUCCESS`/`ERROR`.
    pub async fn resume_workflow<O>(&self, id: &str) -> Result<WorkflowHandle<O>> {
        if !self.provider.resume_workflow(id).await? {
            return Err(Error::app(format!(
                "workflow `{id}` cannot be resumed (missing or already completed)"
            )));
        }
        let row = self
            .provider
            .get_workflow_status(id)
            .await?
            .ok_or_else(|| Error::UnknownWorkflow(id.to_string()))?;
        let rt = self.runtime();
        let handler = rt
            .workflows
            .get(&row.name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(row.name.clone()))?;
        let auth = AuthContext::from_status(&row);
        let join = rt.spawn_owned(id.to_string(), handler, row.input, row.deadline_ms, auth);
        Ok(WorkflowHandle::local(
            id.to_string(),
            self.provider.clone(),
            join,
        ))
    }

    /// Cancel many workflows in one round-trip. Each that exists and is not
    /// terminal is set `CANCELLED` and removed from its queue; missing or
    /// already-terminal ids are silently skipped (no error). An empty slice is a
    /// no-op.
    pub async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        self.provider.cancel_workflows(ids).await
    }

    /// Resume many workflows in one round-trip. Each that exists and is not
    /// `SUCCESS`/`ERROR` returns to `PENDING` and is re-dispatched here; the
    /// returned handles track exactly those runs (skipped ids yield no handle, so
    /// the result may be shorter than `ids`).
    pub async fn resume_workflows<O>(&self, ids: &[String]) -> Result<Vec<WorkflowHandle<O>>> {
        let resumed = self.provider.resume_workflows(ids).await?;
        let rt = self.runtime();
        let mut handles = Vec::with_capacity(resumed.len());
        for id in resumed {
            let row = self
                .provider
                .get_workflow_status(&id)
                .await?
                .ok_or_else(|| Error::UnknownWorkflow(id.clone()))?;
            let handler = rt
                .workflows
                .get(&row.name)
                .cloned()
                .ok_or_else(|| Error::UnknownWorkflow(row.name.clone()))?;
            let auth = AuthContext::from_status(&row);
            let join = rt.spawn_owned(id.clone(), handler, row.input, row.deadline_ms, auth);
            handles.push(WorkflowHandle::local(id, self.provider.clone(), join));
        }
        Ok(handles)
    }

    /// Delete many workflows and (via `ON DELETE CASCADE`) their step / event /
    /// stream rows, regardless of state. When `delete_children`, every descendant
    /// by `parent_workflow_id` (transitively) is deleted too. Missing ids are
    /// skipped. An empty slice is a no-op.
    pub async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        self.provider.delete_workflows(ids, delete_children).await
    }

    /// Reschedule a `DELAYED` workflow to become eligible `delay` from now. Only
    /// affects a workflow currently `DELAYED` (e.g. enqueued with
    /// [`WorkflowOptions::delay`]); the queue dispatcher promotes it once due.
    /// Returns `false` (no error) if no `DELAYED` row matched. See
    /// [`set_workflow_delay_until`](Self::set_workflow_delay_until) for an
    /// absolute time.
    pub async fn set_workflow_delay(&self, id: &str, delay: Duration) -> Result<bool> {
        let until = chrono::Utc::now().timestamp_millis() + delay.as_millis() as i64;
        self.provider.set_workflow_delay(id, until).await
    }

    /// Reschedule a `DELAYED` workflow to become eligible at the absolute time
    /// `at`. Like [`set_workflow_delay`](Self::set_workflow_delay) but with a
    /// fixed instant rather than an offset from now.
    pub async fn set_workflow_delay_until(
        &self,
        id: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool> {
        self.provider
            .set_workflow_delay(id, at.timestamp_millis())
            .await
    }

    /// Fork a workflow from `start_step`. Creates a new workflow that reuses the
    /// original's checkpoints for steps `< start_step` and re-executes from
    /// there. The new id comes from `opts.workflow_id` or is generated; the
    /// returned handle tracks the forked run.
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
            .fork_workflow(original_id, &new_id, start_step, &self.app_version)
            .await?;
        let row = self
            .provider
            .get_workflow_status(&new_id)
            .await?
            .ok_or_else(|| Error::UnknownWorkflow(new_id.clone()))?;
        let rt = self.runtime();
        let handler = rt
            .workflows
            .get(&row.name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(row.name.clone()))?;
        let auth = AuthContext::from_status(&row);
        let join = rt.spawn_owned(new_id.clone(), handler, row.input, row.deadline_ms, auth);
        Ok(WorkflowHandle::local(new_id, self.provider.clone(), join))
    }

    /// Re-run every incomplete workflow of this engine's application version,
    /// resuming each from its checkpoints. Workflows of a different version are
    /// left alone (version-gated recovery), and a workflow recovered more than
    /// `max_recovery_attempts` times is parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. Queued workflows are returned to their
    /// queue for re-dispatch; the rest are re-run inline. Call once on startup.
    ///
    /// Returns the number of workflows that were recovered.
    pub async fn recover(&self) -> Result<usize> {
        let filter = ListFilter {
            status: vec![STATUS_PENDING.to_string()],
            app_version: Some(self.app_version.clone()),
            ..Default::default()
        };
        let pending = self.provider.list_workflows(&filter).await?;
        let rt = self.runtime();
        let mut resumed = 0;
        for record in pending {
            let attempts = self
                .provider
                .bump_recovery_attempts(&record.id, self.max_recovery_attempts)
                .await?;
            if attempts > self.max_recovery_attempts {
                tracing::warn!(
                    id = %record.id,
                    attempts,
                    "workflow parked: exceeded max recovery attempts"
                );
                continue;
            }

            // A workflow claimed off a queue before the crash goes back to the
            // queue so the dispatcher (and its concurrency limits) re-runs it.
            if record.queue_name.is_some() {
                self.provider
                    .set_workflow_status(&record.id, STATUS_ENQUEUED, None, None)
                    .await?;
                resumed += 1;
                continue;
            }

            if let Some(handler) = rt.workflows.get(&record.name).cloned() {
                // Best-effort: a workflow that fails again is marked ERROR by
                // `run_to_completion`; we keep going with the rest.
                let _ = run_to_completion(
                    rt.clone(),
                    handler,
                    record.id.clone(),
                    record.input.clone(),
                    record.deadline_ms,
                    AuthContext::from_status(&record),
                )
                .await;
                resumed += 1;
            } else {
                tracing::warn!(
                    workflow = %record.name,
                    id = %record.id,
                    "skipping recovery: no handler registered for this workflow name"
                );
            }
        }
        Ok(resumed)
    }
}

/// The shared execution core: everything needed to create and run a workflow.
///
/// Reachable both from [`DurableEngine`] methods and from inside a running
/// workflow through [`DurableContext`], so a workflow can start child workflows
/// using the same registry, queues, and identity as the engine. Held behind an
/// `Arc`; see [`DurableEngine::runtime`].
pub(crate) struct Runtime {
    provider: Arc<dyn StateProvider>,
    workflows: HashMap<String, WorkflowFn>,
    queues: HashMap<String, Arc<WorkflowQueue>>,
    executor_id: String,
    app_version: String,
    inflight: Arc<AtomicUsize>,
}

impl Runtime {
    pub(crate) fn provider(&self) -> &Arc<dyn StateProvider> {
        &self.provider
    }

    /// Build a new run's status row from `opts` and persist it idempotently,
    /// returning the canonical row and whether it was routed to a queue. Shared
    /// by top-level runs and child workflows; `parent_id`/`auth` are stamped on
    /// the row.
    async fn insert_run(
        &self,
        id: &str,
        name: &str,
        input_json: Value,
        opts: &WorkflowOptions,
        parent_id: Option<&str>,
        auth: &AuthContext,
    ) -> Result<(WorkflowStatus, bool)> {
        let queued = opts.queue.is_some();
        if let Some(q) = &opts.queue {
            if !self.queues.contains_key(q) {
                return Err(Error::UnknownQueue(q.clone()));
            }
        }
        if opts.delay.is_some() && !queued {
            return Err(Error::app(
                "WorkflowOptions.delay requires a queue; direct runs start immediately",
            ));
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        let status = match (queued, opts.delay) {
            (false, _) => STATUS_PENDING,
            (true, None) => STATUS_ENQUEUED,
            (true, Some(_)) => STATUS_DELAYED,
        };
        // A queued row is unowned until a dispatcher claims it.
        let executor = if queued {
            ""
        } else {
            self.executor_id.as_str()
        };

        let mut row =
            WorkflowStatus::new(id, name, input_json, status, executor, &self.app_version);
        row.queue_name = opts.queue.clone();
        row.priority = opts.priority;
        row.queue_partition_key = opts.partition_key.clone();
        row.dedup_id = opts.dedup_id.clone();
        row.parent_workflow_id = parent_id.map(|s| s.to_string());
        row.authenticated_user = auth.authenticated_user.clone();
        row.assumed_role = auth.assumed_role.clone();
        row.authenticated_roles = auth.authenticated_roles.clone();
        row.timeout_ms = opts.timeout.map(|d| d.as_millis() as i64);
        row.delay_until_ms = opts.delay.map(|d| now_ms + d.as_millis() as i64);
        if !queued {
            // Direct runs start now, so the deadline is fixed here; for queued
            // runs it is computed when a dispatcher claims the workflow.
            row.started_at_ms = Some(now_ms);
            row.deadline_ms = row.timeout_ms.map(|t| now_ms + t);
        }

        let canonical = self.provider.insert_workflow_status(row).await?;
        Ok((canonical, queued))
    }

    /// Spawn a run on a task this caller owns (for a local [`WorkflowHandle`]).
    /// The task holds a drain guard so [`DurableEngine::shutdown`] waits it out.
    fn spawn_owned(
        self: &Arc<Self>,
        id: String,
        handler: WorkflowFn,
        input: Value,
        deadline_ms: Option<i64>,
        auth: AuthContext,
    ) -> JoinHandle<Result<Value>> {
        let rt = self.clone();
        self.inflight.fetch_add(1, Ordering::Relaxed);
        let guard = InflightGuard(self.inflight.clone());
        tokio::spawn(async move {
            let _guard = guard;
            run_to_completion(rt, handler, id, input, deadline_ms, auth).await
        })
    }

    /// Spawn a run on a self-owned, detached task; the result is observed by
    /// polling the status row. Used for queue claims, recovery, schedules, and
    /// child workflows.
    fn spawn_detached(
        self: &Arc<Self>,
        id: String,
        handler: WorkflowFn,
        input: Value,
        deadline_ms: Option<i64>,
        auth: AuthContext,
    ) {
        let join = self.spawn_owned(id, handler, input, deadline_ms, auth);
        // Detach: the inflight guard inside the task keeps shutdown correct.
        drop(join);
    }

    /// Start a child workflow under the deterministic `child_id`, stamping the
    /// parent link and the inherited identity. A queued or already-terminal
    /// child is left for polling; otherwise it runs now on a detached task.
    pub(crate) async fn spawn_child(
        self: &Arc<Self>,
        child_id: &str,
        name: &str,
        input_json: Value,
        opts: WorkflowOptions,
        parent_id: &str,
        auth: AuthContext,
    ) -> Result<()> {
        let handler = self
            .workflows
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(name.to_string()))?;
        let (canonical, queued) = self
            .insert_run(child_id, name, input_json, &opts, Some(parent_id), &auth)
            .await?;
        if !queued && !is_terminal(&canonical.status) {
            self.spawn_detached(
                child_id.to_string(),
                handler,
                canonical.input,
                canonical.deadline_ms,
                auth,
            );
        }
        Ok(())
    }
}

/// Decrements the in-flight counter when a workflow task ends (even on panic).
struct InflightGuard(Arc<AtomicUsize>);
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// Per-queue dispatcher: polls for due work and runs claimed workflows.
///
/// Each polling iteration first transitions due `DELAYED` rows, then claims up
/// to a worker-concurrency-adjusted batch (global concurrency and rate limits
/// are enforced inside [`StateProvider::dequeue_workflows`]), and spawns each
/// claim. The polling
/// interval backs off exponentially on dequeue errors, scales back toward the
/// base on success, and is jittered so multiple executors don't poll in step.
///
/// For a partitioned queue, each iteration claims from every active partition
/// independently — worker concurrency is tracked per partition, and the DB-side
/// global/rate limits are scoped to the partition (see [`DequeueRequest`]).
async fn queue_dispatch_loop(
    queue: Arc<WorkflowQueue>,
    rt: Arc<Runtime>,
    shutting_down: Arc<AtomicBool>,
) {
    let provider = rt.provider.clone();
    let executor_id = rt.executor_id.clone();
    let app_version = rt.app_version.clone();
    let inflight = rt.inflight.clone();
    // Local running count per partition key (`""` for a non-partitioned queue).
    let local_running: std::sync::Mutex<HashMap<String, Arc<AtomicUsize>>> = Default::default();
    let mut interval = queue.base_polling_interval;

    loop {
        if shutting_down.load(Ordering::Relaxed) {
            return;
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Err(e) = provider.transition_delayed_workflows(now_ms).await {
            tracing::warn!(queue = %queue.name, error = %e, "failed to transition delayed workflows");
        }

        let mut had_error = false;

        // The partitions to claim from this iteration: each active key for a
        // partitioned queue, or a single unscoped pass otherwise.
        let partition_keys: Vec<Option<String>> = if queue.partitioned {
            match provider.queue_partitions(&queue.name).await {
                Ok(keys) => keys.into_iter().map(Some).collect(),
                Err(e) => {
                    had_error = true;
                    tracing::warn!(queue = %queue.name, error = %e, "listing partitions failed; backing off");
                    Vec::new()
                }
            }
        } else {
            vec![None]
        };

        for pkey in partition_keys {
            // Worker concurrency is enforced here, against this process's running
            // count for the partition; the DB-side checks handle cross-executor
            // limits.
            let counter = {
                let mut map = local_running.lock().expect("local running lock poisoned");
                map.entry(pkey.clone().unwrap_or_default())
                    .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                    .clone()
            };
            let local = counter.load(Ordering::Relaxed);
            let max_tasks = (match queue.worker_concurrency {
                Some(wc) => wc.saturating_sub(local),
                None => queue.max_tasks_per_iteration,
            })
            .min(queue.max_tasks_per_iteration) as i64;

            if max_tasks <= 0 {
                continue;
            }
            let req = DequeueRequest {
                queue_name: queue.name.clone(),
                executor_id: executor_id.clone(),
                app_version: app_version.clone(),
                partition_key: pkey.clone(),
                max_tasks,
                global_concurrency: queue.global_concurrency,
                rate_limit_max: queue.rate_limit.as_ref().map(|r| r.limit),
                rate_limit_period_ms: queue
                    .rate_limit
                    .as_ref()
                    .map(|r| r.period.as_millis() as i64),
            };
            match provider.dequeue_workflows(&req).await {
                Ok(claimed) => {
                    for wf in claimed {
                        let Some(handler) = rt.workflows.get(&wf.name).cloned() else {
                            tracing::error!(
                                workflow = %wf.name,
                                id = %wf.id,
                                "dequeued workflow has no registered handler"
                            );
                            continue;
                        };
                        inflight.fetch_add(1, Ordering::Relaxed);
                        counter.fetch_add(1, Ordering::Relaxed);
                        let rt = rt.clone();
                        let inflight_guard = InflightGuard(inflight.clone());
                        let local_guard = InflightGuard(counter.clone());
                        let auth = AuthContext::from_status(&wf);
                        tokio::spawn(async move {
                            let _inflight = inflight_guard;
                            let _local = local_guard;
                            // Terminal state is recorded by run_to_completion;
                            // a handle observing this workflow polls it.
                            let _ = run_to_completion(
                                rt,
                                handler,
                                wf.id,
                                wf.input,
                                wf.deadline_ms,
                                auth,
                            )
                            .await;
                        });
                    }
                }
                Err(e) => {
                    had_error = true;
                    tracing::warn!(queue = %queue.name, error = %e, "dequeue failed; backing off");
                }
            }
        }

        interval = if had_error {
            (interval * 2).min(queue.max_polling_interval)
        } else {
            interval.mul_f64(0.9).max(queue.base_polling_interval)
        };
        // Cheap 0.95–1.05 jitter from the clock; no rand dependency needed.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let jitter = 0.95 + (nanos % 1000) as f64 / 10_000.0;
        tokio::time::sleep(interval.mul_f64(jitter)).await;
    }
}

/// Run a workflow handler to completion and record its terminal state.
///
/// Free function (not a method) so it can run inside a spawned task without
/// borrowing the engine. Carries the [`Runtime`] so the workflow can start child
/// workflows through its [`DurableContext`].
async fn run_to_completion(
    rt: Arc<Runtime>,
    handler: WorkflowFn,
    id: String,
    input: Value,
    deadline_ms: Option<i64>,
    auth: AuthContext,
) -> Result<Value> {
    let provider = rt.provider().clone();
    let ctx = DurableContext::new(id.clone(), rt, auth);
    let run = handler(ctx, input);

    // Enforce a workflow deadline if one was set: when it elapses, the run
    // future is dropped (cancelled at its next await) and the workflow is
    // marked CANCELLED.
    let result = match deadline_ms {
        Some(dl) => {
            let remaining = (dl - chrono::Utc::now().timestamp_millis()).max(0) as u64;
            match tokio::time::timeout(Duration::from_millis(remaining), run).await {
                Ok(r) => r,
                Err(_elapsed) => {
                    provider
                        .set_workflow_status(&id, STATUS_CANCELLED, None, Some("deadline exceeded"))
                        .await?;
                    return Err(Error::Timeout);
                }
            }
        }
        None => run.await,
    };

    match result {
        Ok(output) => {
            provider
                .set_workflow_status(&id, STATUS_SUCCESS, Some(&output), None)
                .await?;
            Ok(output)
        }
        Err(Error::Cancelled(_)) => {
            // The workflow stopped because it was cancelled; reflect that
            // terminal state rather than ERROR.
            provider
                .set_workflow_status(&id, STATUS_CANCELLED, None, Some("cancelled"))
                .await?;
            Err(Error::Cancelled(id))
        }
        Err(e) => {
            provider
                .set_workflow_status(&id, STATUS_ERROR, None, Some(&e.to_string()))
                .await?;
            Err(e)
        }
    }
}

/// Per-schedule cron loop: at each tick, start the workflow under a
/// deterministic id derived from the tick time, so the run happens exactly once
/// even across multiple executors (the idempotent status insert is the
/// arbiter). The per-tick id has the form `sched-{name}-{time}`.
async fn schedule_loop(
    name: String,
    spec: String,
    handler: WorkflowFn,
    rt: Arc<Runtime>,
    shutting_down: Arc<AtomicBool>,
) {
    let provider = rt.provider.clone();
    let executor_id = rt.executor_id.clone();
    let app_version = rt.app_version.clone();
    let inflight = rt.inflight.clone();
    use std::str::FromStr;
    let schedule = match cron::Schedule::from_str(&spec) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                workflow = %name, schedule = %spec, error = %e,
                "invalid cron schedule; scheduler not started for this workflow"
            );
            return;
        }
    };

    loop {
        if shutting_down.load(Ordering::Relaxed) {
            return;
        }
        let Some(next) = schedule.after(&chrono::Utc::now()).next() else {
            return;
        };
        let wait = (next - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        tokio::time::sleep(wait).await;
        if shutting_down.load(Ordering::Relaxed) {
            return;
        }

        // Deterministic per-tick id; the scheduled time is the workflow input.
        let wf_id = format!("sched-{name}-{}", next.to_rfc3339());
        let input = Value::String(next.to_rfc3339());
        let mut row = WorkflowStatus::new(
            &wf_id,
            &name,
            input,
            STATUS_PENDING,
            &executor_id,
            &app_version,
        );
        row.started_at_ms = Some(chrono::Utc::now().timestamp_millis());

        match provider.insert_workflow_status(row).await {
            Ok(canonical) => {
                // We run the tick only if our insert created the row (its
                // executor_id is ours) and it is not already finished. A
                // different executor that won the insert runs it instead.
                if canonical.executor_id == executor_id && !is_terminal(&canonical.status) {
                    inflight.fetch_add(1, Ordering::Relaxed);
                    let rt = rt.clone();
                    let handler = handler.clone();
                    let guard = InflightGuard(inflight.clone());
                    let auth = AuthContext::from_status(&canonical);
                    tokio::spawn(async move {
                        let _guard = guard;
                        let _ = run_to_completion(
                            rt,
                            handler,
                            wf_id,
                            canonical.input,
                            canonical.deadline_ms,
                            auth,
                        )
                        .await;
                    });
                }
            }
            Err(e) => {
                tracing::warn!(workflow = %name, error = %e, "failed to persist scheduled tick");
            }
        }
    }
}
