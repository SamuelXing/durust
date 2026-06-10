use crate::context::DurableContext;
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    is_terminal, StateProvider, WorkflowStatus, STATUS_CANCELLED, STATUS_ERROR, STATUS_SUCCESS,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
}

inventory::collect!(WorkflowRegistration);

/// Per-workflow start options — the Rust analog of Go's `WithWorkflowID`,
/// `WithDeduplicationID`, `WithQueue`, `WithPriority`, `WithTimeout`.
///
/// `queue`/`priority` are persisted now and acted on by the queue dispatcher in
/// Phase 2; `timeout` is persisted for the deadline enforcement added in Phase 5.
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
    executor_id: String,
    app_version: String,
    /// Flipped by [`shutdown`](Self::shutdown); background loops observe it.
    shutting_down: Arc<AtomicBool>,
    /// Count of workflow tasks this process is currently running, so
    /// [`shutdown`](Self::shutdown) can drain before returning.
    inflight: Arc<AtomicUsize>,
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
        for reg in inventory::iter::<WorkflowRegistration> {
            workflows.insert(reg.name.to_string(), (reg.builder)());
        }
        Ok(Self {
            provider,
            workflows,
            executor_id: uuid::Uuid::new_v4().to_string(),
            app_version: app_version.into(),
            shutting_down: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
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

    /// Start background processing (queue dispatch, scheduler, deadline sweeps).
    ///
    /// In Phase 1 there are no background loops yet, so this only arms the
    /// lifecycle flags; later phases hook their tasks in here. Idempotent.
    pub async fn launch(&self) -> Result<()> {
        self.shutting_down.store(false, Ordering::SeqCst);
        Ok(())
    }

    /// Stop accepting new background work and wait for in-flight workflow tasks
    /// started here to drain (up to `timeout`).
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.shutting_down.store(true, Ordering::SeqCst);
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
    /// The status row is created idempotently under the resolved id, then the
    /// workflow runs on a spawned task. If the id already exists in a terminal
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

        // Build and idempotently persist the status row.
        let mut row = WorkflowStatus::new(
            &id,
            name,
            input_json,
            crate::provider::STATUS_PENDING,
            &self.executor_id,
            &self.app_version,
        );
        row.queue_name = opts.queue.clone();
        row.priority = opts.priority;
        row.dedup_id = opts.dedup_id.clone();
        row.deadline_ms = opts
            .timeout
            .map(|d| (chrono::Utc::now() + chrono::Duration::from_std(d).unwrap_or_default()).timestamp_millis());

        let canonical = self.provider.insert_workflow_status(row).await?;

        // Already finished under this id: hand back a read-only polling handle.
        if is_terminal(&canonical.status) {
            return Ok(WorkflowHandle::polling(id, self.provider.clone()));
        }

        // Spawn the run. Each task holds a drain guard so `shutdown` can wait it
        // out.
        let provider = self.provider.clone();
        let inflight = self.inflight.clone();
        inflight.fetch_add(1, Ordering::SeqCst);
        let task_id = id.clone();
        let join = tokio::spawn(async move {
            let _guard = InflightGuard(inflight);
            run_to_completion(handler, provider, task_id, canonical.input).await
        });

        Ok(WorkflowHandle::local(id, self.provider.clone(), join))
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

    /// Re-run every workflow that is not in a terminal state. Completed steps are
    /// served from their checkpoints, so recovery resumes exactly where the
    /// previous run left off. Call this once on worker startup.
    ///
    /// Returns the number of workflows that were resumed.
    pub async fn recover(&self) -> Result<usize> {
        let pending = self.provider.list_incomplete_workflows().await?;
        let mut resumed = 0;
        for record in pending {
            if let Some(handler) = self.workflows.get(&record.name).cloned() {
                // Best-effort: a workflow that fails again is marked ERROR by
                // `run_to_completion`; we keep going with the rest.
                let _ = run_to_completion(
                    handler,
                    self.provider.clone(),
                    record.id.clone(),
                    record.input.clone(),
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

/// Decrements the in-flight counter when a workflow task ends (even on panic).
struct InflightGuard(Arc<AtomicUsize>);
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
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
) -> Result<Value> {
    let ctx = DurableContext::new(id.clone(), provider.clone());
    match handler(ctx, input).await {
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
