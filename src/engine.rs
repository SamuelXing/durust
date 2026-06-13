use crate::context::DurableContext;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    is_terminal, DequeueRequest, ListFilter, StateProvider, WorkflowStatus, STATUS_CANCELLED,
    STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR, STATUS_PENDING, STATUS_SUCCESS,
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

/// Per-workflow start options — the Rust analog of Go's `WithWorkflowID`,
/// `WithDeduplicationID`, `WithQueue`, `WithPriority`, `WithTimeout`.
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
    /// Wall-clock deadline for the whole workflow.
    pub timeout: Option<Duration>,
    /// Delay before the workflow becomes eligible to run (queued workflows
    /// only): it sits in `DELAYED` until the dispatcher transitions it.
    pub delay: Option<Duration>,
}

impl WorkflowOptions {
    /// Convenience: options that only pin the workflow id.
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            workflow_id: Some(id.into()),
            ..Default::default()
        }
    }
}

/// The durable execution engine — the Rust analog of the Go SDK's `DBOSContext`.
///
/// Holds the state backend, a registry of workflow functions by name, and this
/// process's identity (`executor_id`, `app_version`). There is no separate
/// server process: the engine is a library that lives in your worker and talks
/// directly to the [`StateProvider`].
pub struct DurableEngine {
    provider: Arc<dyn StateProvider>,
    workflows: HashMap<String, WorkflowFn>,
    queues: HashMap<String, Arc<WorkflowQueue>>,
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
            scheduled,
            executor_id: uuid::Uuid::new_v4().to_string(),
            app_version: app_version.into(),
            max_recovery_attempts: 100,
            shutting_down: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
            dispatchers: std::sync::Mutex::new(Vec::new()),
        })
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

    /// Start background processing: one dispatcher task per registered queue and
    /// one scheduler task per `#[workflow(schedule = …)]` workflow. Workflow
    /// timeouts are enforced inline per run, so they need no separate sweep. Call
    /// once per launch; safe to call again after `shutdown`.
    pub async fn launch(&self) -> Result<()> {
        self.shutting_down.store(false, Ordering::SeqCst);
        let bg = Background {
            provider: self.provider.clone(),
            executor_id: self.executor_id.clone(),
            app_version: self.app_version.clone(),
            shutting_down: self.shutting_down.clone(),
            inflight: self.inflight.clone(),
        };
        let mut tasks = self.dispatchers.lock().expect("dispatcher lock poisoned");
        for queue in self.queues.values() {
            tasks.push(tokio::spawn(queue_dispatch_loop(
                queue.clone(),
                self.workflows.clone(),
                bg.clone(),
            )));
        }
        for (name, spec) in &self.scheduled {
            let Some(handler) = self.workflows.get(name).cloned() else {
                continue;
            };
            tasks.push(tokio::spawn(schedule_loop(
                name.clone(),
                spec.clone(),
                handler,
                bg.clone(),
            )));
        }
        Ok(())
    }

    /// Stop the queue dispatchers and wait for in-flight workflow tasks started
    /// here to drain (up to `timeout`).
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.shutting_down.store(true, Ordering::SeqCst);
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
        while self.inflight.load(Ordering::SeqCst) > 0 {
            if std::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Start (or attach to) a workflow and return a [`WorkflowHandle`] **without
    /// blocking** on its completion — the Rust analog of Go's `RunWorkflow`.
    ///
    /// The status row is created idempotently under the resolved id. Without a
    /// queue, the workflow runs immediately on a spawned task. With
    /// `opts.queue` set, the row is persisted `ENQUEUED` (or `DELAYED` when
    /// `opts.delay` is set) and a polling handle is returned — a dispatcher on
    /// any executor claims and runs it, exactly like Go's `WithQueue`. If the
    /// id already exists in a terminal state, a polling handle over the stored
    /// result is returned instead of re-running.
    pub async fn run_workflow<I, O>(
        &self,
        name: &str,
        input: I,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        let id = opts
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let input_json = serde_json::to_value(input)?;

        let handler = self
            .workflows
            .get(name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(name.to_string()))?;

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
            WorkflowStatus::new(&id, name, input_json, status, executor, &self.app_version);
        row.queue_name = opts.queue.clone();
        row.priority = opts.priority;
        row.dedup_id = opts.dedup_id.clone();
        row.timeout_ms = opts.timeout.map(|d| d.as_millis() as i64);
        row.delay_until_ms = opts.delay.map(|d| now_ms + d.as_millis() as i64);
        if !queued {
            // Direct runs start now, so the deadline is fixed here; for queued
            // runs it is computed when a dispatcher claims the workflow.
            row.started_at_ms = Some(now_ms);
            row.deadline_ms = row.timeout_ms.map(|t| now_ms + t);
        }

        let canonical = self.provider.insert_workflow_status(row).await?;

        // Terminal already, or owned by a queue: observe via polling.
        if queued || is_terminal(&canonical.status) {
            return Ok(WorkflowHandle::polling(id, self.provider.clone()));
        }

        Ok(self.spawn_local(id, handler, canonical.input, canonical.deadline_ms))
    }

    /// Spawn a workflow run on a task and return a handle owning it. Each task
    /// holds a drain guard so [`shutdown`](Self::shutdown) can wait it out.
    fn spawn_local<O>(
        &self,
        id: String,
        handler: WorkflowFn,
        input: Value,
        deadline_ms: Option<i64>,
    ) -> WorkflowHandle<O> {
        let provider = self.provider.clone();
        let inflight = self.inflight.clone();
        inflight.fetch_add(1, Ordering::SeqCst);
        let task_id = id.clone();
        let join = tokio::spawn(async move {
            let _guard = InflightGuard(inflight);
            run_to_completion(handler, provider, task_id, input, deadline_ms).await
        });
        WorkflowHandle::local(id, self.provider.clone(), join)
    }

    /// Enqueue a workflow on a registered queue — the Rust analog of Go's
    /// `Enqueue`. Sugar for [`run_workflow`](Self::run_workflow) with
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

    /// Get a [`WorkflowHandle`] for an existing workflow — the Rust analog of
    /// Go's `RetrieveWorkflow`. The handle observes the workflow by polling, so
    /// it works regardless of which executor is running it. Errors if no
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

    /// List workflows matching `filter` — the Rust analog of Go's
    /// `ListWorkflows`.
    pub async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        self.provider.list_workflows(filter).await
    }

    /// Cancel a workflow — the Rust analog of Go's `CancelWorkflow`. A
    /// non-terminal workflow is set `CANCELLED` and removed from its queue; a
    /// running workflow stops at its next step (cooperative cancellation).
    pub async fn cancel_workflow(&self, id: &str) -> Result<()> {
        self.provider.cancel_workflow(id).await
    }

    /// Resume a cancelled (or otherwise non-terminal) workflow — the Rust analog
    /// of Go's `ResumeWorkflow`. The workflow is returned to `PENDING` and
    /// re-run from its checkpoints; the returned handle tracks the new run.
    /// Errors if the workflow does not exist or is already `SUCCESS`/`ERROR`.
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
        let handler = self
            .workflows
            .get(&row.name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(row.name.clone()))?;
        Ok(self.spawn_local(id.to_string(), handler, row.input, row.deadline_ms))
    }

    /// Fork a workflow from `start_step` — the Rust analog of Go's
    /// `ForkWorkflow`. Creates a new workflow that reuses the original's
    /// checkpoints for steps `< start_step` and re-executes from there. The new
    /// id comes from `opts.workflow_id` or is generated; the returned handle
    /// tracks the forked run.
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
        let handler = self
            .workflows
            .get(&row.name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(row.name.clone()))?;
        Ok(self.spawn_local(new_id, handler, row.input, row.deadline_ms))
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

            if let Some(handler) = self.workflows.get(&record.name).cloned() {
                // Best-effort: a workflow that fails again is marked ERROR by
                // `run_to_completion`; we keep going with the rest.
                let _ = run_to_completion(
                    handler,
                    self.provider.clone(),
                    record.id.clone(),
                    record.input.clone(),
                    record.deadline_ms,
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

/// Shared context handed to the background loops (queue dispatch, scheduler):
/// the state backend, this process's identity, and the lifecycle signals.
#[derive(Clone)]
struct Background {
    provider: Arc<dyn StateProvider>,
    executor_id: String,
    app_version: String,
    shutting_down: Arc<AtomicBool>,
    inflight: Arc<AtomicUsize>,
}

/// Decrements the in-flight counter when a workflow task ends (even on panic).
struct InflightGuard(Arc<AtomicUsize>);
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Per-queue dispatcher: polls for due work and runs claimed workflows.
///
/// Mirrors the Go SDK's `queueRunner.runQueue` loop: each iteration first
/// transitions due `DELAYED` rows, then claims up to a worker-concurrency-
/// adjusted batch (global concurrency and rate limits are enforced inside
/// [`StateProvider::dequeue_workflows`]), and spawns each claim. The polling
/// interval backs off exponentially on dequeue errors, scales back toward the
/// base on success, and is jittered so multiple executors don't poll in step.
async fn queue_dispatch_loop(
    queue: Arc<WorkflowQueue>,
    workflows: HashMap<String, WorkflowFn>,
    bg: Background,
) {
    let Background {
        provider,
        executor_id,
        app_version,
        shutting_down,
        inflight,
    } = bg;
    let local_running = Arc::new(AtomicUsize::new(0));
    let mut interval = queue.base_polling_interval;

    loop {
        if shutting_down.load(Ordering::SeqCst) {
            return;
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Err(e) = provider.transition_delayed_workflows(now_ms).await {
            tracing::warn!(queue = %queue.name, error = %e, "failed to transition delayed workflows");
        }

        // Worker concurrency is enforced here, against this process's running
        // count; the DB-side checks handle the cross-executor limits.
        let local = local_running.load(Ordering::SeqCst);
        let max_tasks = (match queue.worker_concurrency {
            Some(wc) => wc.saturating_sub(local),
            None => queue.max_tasks_per_iteration,
        })
        .min(queue.max_tasks_per_iteration) as i64;

        let mut had_error = false;
        if max_tasks > 0 {
            let req = DequeueRequest {
                queue_name: queue.name.clone(),
                executor_id: executor_id.clone(),
                app_version: app_version.clone(),
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
                        let Some(handler) = workflows.get(&wf.name).cloned() else {
                            tracing::error!(
                                workflow = %wf.name,
                                id = %wf.id,
                                "dequeued workflow has no registered handler"
                            );
                            continue;
                        };
                        inflight.fetch_add(1, Ordering::SeqCst);
                        local_running.fetch_add(1, Ordering::SeqCst);
                        let provider = provider.clone();
                        let inflight_guard = InflightGuard(inflight.clone());
                        let local_guard = InflightGuard(local_running.clone());
                        tokio::spawn(async move {
                            let _inflight = inflight_guard;
                            let _local = local_guard;
                            // Terminal state is recorded by run_to_completion;
                            // a handle observing this workflow polls it.
                            let _ = run_to_completion(
                                handler,
                                provider,
                                wf.id,
                                wf.input,
                                wf.deadline_ms,
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
/// borrowing the engine.
async fn run_to_completion(
    handler: WorkflowFn,
    provider: Arc<dyn StateProvider>,
    id: String,
    input: Value,
    deadline_ms: Option<i64>,
) -> Result<Value> {
    let ctx = DurableContext::new(id.clone(), provider.clone());
    let run = handler(ctx, input);

    // Enforce a workflow deadline if one was set: when it elapses, the run
    // future is dropped (cancelled at its next await) and the workflow is
    // marked CANCELLED, mirroring Go's deadline-driven cancellation.
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
/// arbiter). Mirrors the Go SDK's `sched-{name}-{time}` scheme.
async fn schedule_loop(name: String, spec: String, handler: WorkflowFn, bg: Background) {
    let Background {
        provider,
        executor_id,
        app_version,
        shutting_down,
        inflight,
    } = bg;
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
        if shutting_down.load(Ordering::SeqCst) {
            return;
        }
        let Some(next) = schedule.after(&chrono::Utc::now()).next() else {
            return;
        };
        let wait = (next - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        tokio::time::sleep(wait).await;
        if shutting_down.load(Ordering::SeqCst) {
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
                    inflight.fetch_add(1, Ordering::SeqCst);
                    let provider = provider.clone();
                    let handler = handler.clone();
                    let guard = InflightGuard(inflight.clone());
                    tokio::spawn(async move {
                        let _guard = guard;
                        let _ = run_to_completion(
                            handler,
                            provider,
                            wf_id,
                            canonical.input,
                            canonical.deadline_ms,
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
