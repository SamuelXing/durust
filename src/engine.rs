use crate::context::{AuthContext, DurableContext};
use crate::error::{panic_message, Error, ErrorCode, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{
    is_terminal, DequeueRequest, ForkParams, ListFilter, StateProvider, StepAggregate,
    StepAggregateQuery, StepInfo, VersionInfo, WorkflowAggregate, WorkflowAggregateQuery,
    WorkflowStatus, STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_PENDING, STATUS_SUCCESS,
};
use crate::queue::WorkflowQueue;
use crate::schedule::{
    ApplySchedule, ScheduleFilter, ScheduleOptions, ScheduleStatus, WorkflowSchedule,
};
use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::Instrument;

/// How often the schedule reconciler lists persisted schedules and installs or
/// retires per-schedule firing loops. Short so a freshly created or paused
/// schedule takes effect promptly.
const SCHEDULE_RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// Name of the always-on internal queue every engine dispatches. Out-of-process
/// callers (a [`Client`](crate::Client)) and the engine itself route re-executed
/// work with no user queue (resume / fork of a direct run) here, so any live
/// engine claims and runs it — rather than relying on a restart's recovery.
pub(crate) const INTERNAL_QUEUE: &str = "_dbos_internal_queue";

/// The internal queue, polled faster than the 1s default so re-executed work
/// starts promptly.
fn internal_queue() -> WorkflowQueue {
    let mut q = WorkflowQueue::new(INTERNAL_QUEUE);
    q.base_polling_interval = Duration::from_millis(100);
    q
}

/// A type-erased workflow handler: takes a context + JSON input, returns JSON output.
pub type WorkflowFn = Arc<
    dyn Fn(DurableContext, Value) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>>
        + Send
        + Sync,
>;

/// The registry key for a workflow: plain `name`, or an instance-qualified
/// `name/config` when a non-empty config name is present. Keeps un-configured
/// registrations keyed exactly by name (backward compatible) while letting
/// several configured instances share one workflow name.
pub(crate) fn registry_key(name: &str, config_name: Option<&str>) -> String {
    match config_name {
        Some(c) if !c.is_empty() => format!("{name}/{c}"),
        _ => name.to_string(),
    }
}

/// Erase a typed `async fn(DurableContext, Input) -> Result<Output>` into the
/// JSON-in / JSON-out [`WorkflowFn`] the engine stores.
///
/// This is the single place input/output (de)serialization happens. Both
/// [`DurableEngine::register`] and the `#[durare::workflow]` macro funnel through
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

/// A compile-time workflow registration emitted by `#[durare::workflow]`.
///
/// Collected via the `inventory` crate: every annotated workflow in the binary
/// submits one of these, and [`DurableEngine::new`] iterates them so no manual
/// `register` call is needed.
pub struct WorkflowRegistration {
    /// The name the workflow is registered (and persisted) under.
    pub name: &'static str,
    /// Builds the type-erased handler. Typically `|| durare::erase(my_fn)`.
    pub builder: fn() -> WorkflowFn,
    /// A cron spec (6-field, second precision) if this is a scheduled workflow;
    /// emitted by `#[workflow(schedule = "...")]`. `None` otherwise.
    pub schedule: Option<&'static str>,
}

inventory::collect!(WorkflowRegistration);

/// A compile-time, typed reference to a registered workflow.
///
/// `#[durare::workflow]` emits one of these for every annotated function: a
/// zero-sized marker named by the function in `UpperCamelCase` (so
/// `process_order` yields a `ProcessOrder` marker). Passing the marker to
/// [`DurableEngine::start_with`] fixes the input and output types from the
/// function's own signature, so the call is checked without a turbofish and a
/// wrong input type is a compile error:
///
/// ```
/// use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
/// use std::sync::Arc;
///
/// #[durare::workflow]
/// async fn process_order(ctx: DurableContext, order_id: String) -> Result<String> {
///     Ok(format!("receipt for {order_id}"))
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<()> {
/// # let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
/// // input checked as String, output inferred as String:
/// let handle = engine.start_with(ProcessOrder, "1001".into(), WorkflowOptions::default()).await?;
/// let receipt: String = handle.await?;
/// # assert_eq!(receipt, "receipt for 1001");
/// # Ok(())
/// # }
/// ```
pub trait WorkflowDef {
    /// The workflow's input type — its second parameter.
    type Input;
    /// The workflow's output type — the `Ok` type of its returned `Result`.
    type Output;
    /// The name the workflow is registered and persisted under.
    const NAME: &'static str;
}

/// Projects the `Ok` type out of a `Result` at the type level. Lets the
/// `#[durare::workflow]` macro name a workflow's output as
/// `<ReturnType as WorkflowResult>::Ok` instead of parsing the return type's
/// tokens — the compiler does the extraction, through any `Result` alias.
/// Macro plumbing, not public API.
#[doc(hidden)]
pub trait WorkflowResult {
    /// The `Ok` type of the `Result`.
    type Ok;
}

impl<T, E> WorkflowResult for std::result::Result<T, E> {
    type Ok = T;
}

/// A workflow registered on a [`DurableEngine`], as reported by
/// [`DurableEngine::list_registered_workflows`]. The `name` is the identifier
/// the workflow is registered and persisted under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredWorkflow {
    /// The name the workflow is registered under.
    pub name: String,
    /// The cron schedule (6-field, second precision) for a
    /// `#[workflow(schedule = "...")]` workflow; `None` for an unscheduled one.
    pub cron_schedule: Option<String>,
}

/// Engine-level identity settings, resolved at construction.
///
/// Each field follows the same precedence, matching the platform convention
/// shared by every DBOS SDK:
///
/// 1. the environment variable, when set and non-empty
///    (`DBOS__APPVERSION` / `DBOS__VMID` — set by the hosting platform);
/// 2. the explicit value configured here;
/// 3. the default — a sha-256 of the running executable for the application
///    version (distinct builds get distinct versions, so version-gated queue
///    dispatch and recovery never mix incompatible code), and `"local"` for
///    the executor id (stable across restarts of a local process, so its
///    crashed runs are attributable to it).
#[derive(Clone, Debug, Default)]
pub struct EngineConfig {
    /// Application version stamped on runs and used to gate dequeue/recovery.
    pub app_version: Option<String>,
    /// This process's executor id, recorded as the owner of claimed runs.
    pub executor_id: Option<String>,
    /// Whether [`launch`](DurableEngine::launch) recovers this executor's
    /// pending workflows in the background on startup. `None` resolves to the
    /// default, **off** — recovery is opt-in because it is only sound when each
    /// live process has a *unique* executor id (recovering "this executor's"
    /// pending work assumes the previous owner is gone, not running concurrently).
    /// Enable it for a single-process app, or when you set a distinct
    /// `DBOS__VMID` per process; otherwise drive recovery yourself with
    /// [`recover`](DurableEngine::recover).
    pub recover_on_launch: Option<bool>,
}

impl EngineConfig {
    /// Set the application version (still overridden by `DBOS__APPVERSION`).
    pub fn app_version(mut self, version: impl Into<String>) -> Self {
        self.app_version = Some(version.into());
        self
    }

    /// Set the executor id (still overridden by `DBOS__VMID`).
    pub fn executor_id(mut self, id: impl Into<String>) -> Self {
        self.executor_id = Some(id.into());
        self
    }

    /// Enable [`launch`](DurableEngine::launch) recovering this executor's
    /// pending workflows on startup (off by default — see the field docs for why
    /// it is opt-in). Leave it off to drive recovery yourself via
    /// [`recover`](DurableEngine::recover).
    pub fn recover_on_launch(mut self, yes: bool) -> Self {
        self.recover_on_launch = Some(yes);
        self
    }

    pub(crate) fn resolve_app_version(&self) -> String {
        if let Ok(v) = std::env::var("DBOS__APPVERSION") {
            if !v.is_empty() {
                return v;
            }
        }
        if let Some(v) = &self.app_version {
            return v.clone();
        }
        binary_version().to_string()
    }

    pub(crate) fn resolve_executor_id(&self) -> String {
        if let Ok(v) = std::env::var("DBOS__VMID") {
            if !v.is_empty() {
                return v;
            }
        }
        if let Some(v) = &self.executor_id {
            return v.clone();
        }
        "local".to_string()
    }

    pub(crate) fn resolve_recover_on_launch(&self) -> bool {
        self.recover_on_launch.unwrap_or(false)
    }
}

/// The default application version: a hex sha-256 of the running executable,
/// computed once per process. Distinct builds hash differently, so two
/// deployments sharing a system database never claim each other's queued or
/// recovering work. Falls back to `""` (matches-any in dequeue gating) if the
/// executable cannot be read, with a warning.
fn binary_version() -> &'static str {
    static HASH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    HASH.get_or_init(|| {
        let hash = || -> std::io::Result<String> {
            use sha2::{Digest, Sha256};
            let exe = std::env::current_exe()?.canonicalize()?;
            let mut file = std::fs::File::open(exe)?;
            let mut hasher = Sha256::new();
            std::io::copy(&mut file, &mut hasher)?;
            Ok(format!("{:x}", hasher.finalize()))
        };
        match hash() {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "failed to hash the executable for the default application version");
                String::new()
            }
        }
    })
}

/// Per-workflow start options.
///
/// `timeout` fixes a deadline when the workflow starts (at claim time for
/// queued workflows); a run that overruns it is cancelled.
/// How a queue-scoped deduplication-id collision is handled on enqueue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeduplicationPolicy {
    /// Reject the enqueue with a
    /// [`QueueDeduplicated`](crate::ErrorCode::QueueDeduplicated) error — the
    /// default.
    #[default]
    Reject,
    /// Instead of erroring, return a handle to the workflow already enqueued
    /// under this deduplication id. Requires a deduplication id.
    ReturnExisting,
}

/// Per-invocation options for starting or enqueuing a workflow — its
/// idempotency key, queue and priority, deduplication, timeout, app version, and
/// authenticated identity. Build with the chained setters (e.g.
/// [`WorkflowOptions::with_id`] then [`queue`](WorkflowOptions::queue)); all
/// fields default to unset.
#[derive(Clone, Default)]
pub struct WorkflowOptions {
    /// Explicit idempotency key. If `None`, a uuid is generated.
    pub workflow_id: Option<String>,
    /// Queue-scoped deduplication key.
    pub dedup_id: Option<String>,
    /// How a deduplication-id collision is handled (queued runs only); requires
    /// `dedup_id` when not [`Reject`](DeduplicationPolicy::Reject).
    pub dedup_policy: DeduplicationPolicy,
    /// Application version to stamp on this run, overriding the engine/client
    /// default. `None` uses the default (an empty default lets any executor
    /// claim the work).
    pub app_version: Option<String>,
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
    /// Config / instance name: routes this run to the handler registered for that
    /// instance under the same workflow name (see
    /// [`DurableEngine::register_configured`]), and is recorded so recovery
    /// re-dispatches to the same instance.
    pub config_name: Option<String>,
    /// Class / namespace name recorded on the row (cross-SDK metadata). Not used
    /// for routing on its own.
    pub class_name: Option<String>,
}

impl WorkflowOptions {
    /// Convenience: options that only pin the workflow id.
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            workflow_id: Some(id.into()),
            ..Default::default()
        }
    }

    /// Enqueue onto the named queue instead of running in-process: the row is
    /// persisted `ENQUEUED` and a dispatcher on any executor claims it. Pair with
    /// [`start`](DurableEngine::start)/[`start_with`](DurableEngine::start_with).
    pub fn queue(mut self, name: impl Into<String>) -> Self {
        self.queue = Some(name.into());
        self
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

    /// Set the queue-scoped deduplication id.
    pub fn dedup_id(mut self, id: impl Into<String>) -> Self {
        self.dedup_id = Some(id.into());
        self
    }

    /// Set how a deduplication-id collision is handled (see
    /// [`DeduplicationPolicy`]).
    pub fn dedup_policy(mut self, policy: DeduplicationPolicy) -> Self {
        self.dedup_policy = policy;
        self
    }

    /// Route this run to the handler registered for a configured instance under
    /// the same workflow name (see [`DurableEngine::register_configured`]).
    pub fn config_name(mut self, name: impl Into<String>) -> Self {
        self.config_name = Some(name.into());
        self
    }

    /// Set the class / namespace name recorded on the row (cross-SDK metadata).
    pub fn class_name(mut self, name: impl Into<String>) -> Self {
        self.class_name = Some(name.into());
        self
    }

    /// Stamp a specific application version on this run, overriding the default.
    pub fn app_version(mut self, version: impl Into<String>) -> Self {
        self.app_version = Some(version.into());
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

/// A point-in-time readiness report from [`DurableEngine::health`].
///
/// Each axis is `None` when healthy and carries the failure reason otherwise,
/// so a probe handler can render the whole story without re-deriving it.
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// The state backend: `None` when it is reachable and its dbos system
    /// schema is present and current; the reason otherwise (connection error,
    /// schema missing, schema behind this binary).
    pub database: Option<String>,
    /// Work dispatch: `None` when this process is claiming work — launched,
    /// not deactivated, not shut down, every dispatcher task alive; the
    /// reason otherwise.
    pub dispatch: Option<String>,
}

impl HealthReport {
    /// Ready to serve: every axis is healthy.
    pub fn is_ready(&self) -> bool {
        self.database.is_none() && self.dispatch.is_none()
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
    /// Whether [`launch`](Self::launch) recovers this executor's pending
    /// workflows in the background on startup (off by default; opt-in). See
    /// [`EngineConfig::recover_on_launch`].
    recover_on_launch: bool,
    /// Cancelled by [`shutdown`](Self::shutdown) to stop the background loops.
    /// [`launch`](Self::launch) installs a fresh token after a shutdown, since a
    /// cancelled token can't be reset.
    shutdown_token: std::sync::Mutex<CancellationToken>,
    /// Set by [`deactivate`](Self::deactivate): this process stops claiming new
    /// work (dispatchers/scheduler aborted) but keeps serving in-flight runs and
    /// the admin server. Idempotent.
    deactivated: Arc<AtomicBool>,
    /// Tracks every in-flight workflow-run task so [`shutdown`](Self::shutdown)
    /// can drain them before returning. A run is spawned *through* the tracker,
    /// so it is counted from the instant it is created — no separate guard to
    /// keep in sync.
    tasks: TaskTracker,
    /// Per-queue dispatcher tasks spawned by [`launch`](Self::launch).
    dispatchers: std::sync::Mutex<Vec<JoinHandle<()>>>,
    /// Shared execution core, built once from the registrations on first run and
    /// reused by every run path (and by workflows that start child workflows).
    runtime: std::sync::OnceLock<Arc<Runtime>>,
}

/// Builds a [`DurableEngine`] with a complete, conflict-checked registry.
///
/// Obtained from [`DurableEngine::builder`] or [`DurableEngine::connect`].
/// Collect all registrations, then call [`build`](Self::build) once. Registering
/// is only possible on the builder — so, unlike the [`new`](DurableEngine::new) +
/// `&mut` [`register`](DurableEngine::register) path, a registration after the
/// engine is running cannot be expressed, and a duplicate name is a build-time
/// [`Error::ConflictingRegistration`] instead of a silent overwrite.
pub struct DurableEngineBuilder {
    provider: Arc<dyn StateProvider>,
    config: EngineConfig,
    /// Explicit registrations in call order (keyed like the engine's map: plain
    /// `name`, or `name/config` for a configured instance).
    workflows: Vec<(String, WorkflowFn)>,
    queues: Vec<WorkflowQueue>,
    listen_filter: Option<std::collections::HashSet<String>>,
    max_recovery_attempts: i32,
}

impl DurableEngineBuilder {
    /// Register a workflow handler under `name`. See [`DurableEngine::register`].
    pub fn register<I, O, F, Fut>(&mut self, name: &str, f: F) -> &mut Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.workflows.push((name.to_string(), erase(f)));
        self
    }

    /// Register a configured-instance handler. See
    /// [`DurableEngine::register_configured`].
    pub fn register_configured<I, O, F, Fut>(
        &mut self,
        name: &str,
        config_name: &str,
        f: F,
    ) -> &mut Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.workflows
            .push((registry_key(name, Some(config_name)), erase(f)));
        self
    }

    /// Register a durable queue. See [`DurableEngine::register_queue`].
    pub fn register_queue(&mut self, queue: WorkflowQueue) -> &mut Self {
        self.queues.push(queue);
        self
    }

    /// Restrict which registered queues this process dispatches. See
    /// [`DurableEngine::listen_queues`].
    pub fn listen_queues<I, S>(&mut self, names: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.listen_filter = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// Set the application version (still overridden by `DBOS__APPVERSION`). See
    /// [`EngineConfig`].
    pub fn app_version(&mut self, version: impl Into<String>) -> &mut Self {
        self.config.app_version = Some(version.into());
        self
    }

    /// Set this process's executor id (still overridden by `DBOS__VMID`).
    pub fn executor_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.config.executor_id = Some(id.into());
        self
    }

    /// Enable [`launch`](DurableEngine::launch) recovering this executor's
    /// pending workflows on startup (off by default; opt-in). See
    /// [`EngineConfig::recover_on_launch`].
    pub fn recover_on_launch(&mut self, yes: bool) -> &mut Self {
        self.config.recover_on_launch = Some(yes);
        self
    }

    /// Set the recovery-attempt cap before a workflow is parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED` (default 100).
    pub fn max_recovery_attempts(&mut self, max: i32) -> &mut Self {
        self.max_recovery_attempts = max;
        self
    }

    /// Initialize the backend and build the engine, collecting every
    /// `#[durare::workflow]` in the binary plus the explicit registrations into
    /// one registry. Errors with [`Error::ConflictingRegistration`] if any name
    /// (or configured-instance key) is registered twice.
    pub async fn build(self) -> Result<DurableEngine> {
        let app_version = self.config.resolve_app_version();
        let executor_id = self.config.resolve_executor_id();
        self.provider.init().await?;

        let mut workflows: HashMap<String, WorkflowFn> = HashMap::new();
        let mut scheduled = Vec::new();
        // Reserve the internal debouncer workflow first, so a user workflow
        // colliding with its reserved name is rejected by the checks below
        // rather than silently overwriting — or being overwritten by — it.
        workflows.insert(
            crate::debounce::DEBOUNCER_WF.to_string(),
            erase(crate::debounce::internal_debouncer),
        );
        // Auto-registered `#[durare::workflow]`s.
        for reg in inventory::iter::<WorkflowRegistration> {
            if workflows
                .insert(reg.name.to_string(), (reg.builder)())
                .is_some()
            {
                return Err(Error::conflicting_registration(reg.name));
            }
            if let Some(spec) = reg.schedule {
                scheduled.push((reg.name.to_string(), spec.to_string()));
            }
        }
        // Explicit registrations — strict: no silent overwrite.
        for (key, f) in self.workflows {
            if workflows.insert(key.clone(), f).is_some() {
                return Err(Error::conflicting_registration(key));
            }
        }

        // Reserve the internal queue first, then add user queues strictly: a
        // duplicate queue name — or a collision with the reserved internal
        // queue that resume/fork/debouncer route through — is rejected, not
        // silently merged.
        let mut queues = HashMap::new();
        queues.insert(INTERNAL_QUEUE.to_string(), Arc::new(internal_queue()));
        for q in self.queues {
            let name = q.name.clone();
            if queues.insert(name.clone(), Arc::new(q)).is_some() {
                return Err(Error::conflicting_registration(name));
            }
        }

        Ok(DurableEngine {
            provider: self.provider,
            workflows,
            queues,
            listen_filter: self.listen_filter,
            scheduled,
            executor_id,
            app_version,
            max_recovery_attempts: self.max_recovery_attempts,
            recover_on_launch: self.config.resolve_recover_on_launch(),
            shutdown_token: std::sync::Mutex::new(CancellationToken::new()),
            deactivated: Arc::new(AtomicBool::new(false)),
            tasks: TaskTracker::new(),
            dispatchers: std::sync::Mutex::new(Vec::new()),
            runtime: std::sync::OnceLock::new(),
        })
    }
}

impl DurableEngine {
    /// Create an engine with a generated executor id and a default app version.
    ///
    /// Every workflow annotated with `#[durare::workflow]` anywhere in the binary
    /// is auto-registered here (via `inventory`).
    ///
    /// The application version and executor id resolve as
    /// [`EngineConfig`] documents: `DBOS__APPVERSION` / `DBOS__VMID` environment
    /// overrides, then a sha-256 of the running executable / `"local"`.
    pub async fn new(provider: Arc<dyn StateProvider>) -> Result<Self> {
        Self::with_config(provider, EngineConfig::default()).await
    }

    /// Like [`new`](Self::new) but pins the application version used to stamp and
    /// version-gate workflows (recovery only re-runs rows of a matching version).
    /// A non-empty `DBOS__APPVERSION` environment variable still wins.
    pub async fn new_with_version(
        provider: Arc<dyn StateProvider>,
        app_version: impl Into<String>,
    ) -> Result<Self> {
        Self::with_config(
            provider,
            EngineConfig {
                app_version: Some(app_version.into()),
                ..Default::default()
            },
        )
        .await
    }

    /// Build an engine with explicit [`EngineConfig`] settings; anything left
    /// `None` resolves from the environment, then to the documented default.
    pub async fn with_config(
        provider: Arc<dyn StateProvider>,
        config: EngineConfig,
    ) -> Result<Self> {
        let app_version = config.resolve_app_version();
        let executor_id = config.resolve_executor_id();
        provider.init().await?;
        let mut workflows = HashMap::new();
        let mut scheduled = Vec::new();
        for reg in inventory::iter::<WorkflowRegistration> {
            workflows.insert(reg.name.to_string(), (reg.builder)());
            if let Some(spec) = reg.schedule {
                scheduled.push((reg.name.to_string(), spec.to_string()));
            }
        }
        // The internal debouncer workflow is always available (operates over
        // JSON, so one registration serves every debounced target).
        workflows.insert(
            crate::debounce::DEBOUNCER_WF.to_string(),
            erase(crate::debounce::internal_debouncer),
        );
        let mut queues = HashMap::new();
        queues.insert(INTERNAL_QUEUE.to_string(), Arc::new(internal_queue()));
        Ok(Self {
            provider,
            workflows,
            queues,
            listen_filter: None,
            scheduled,
            executor_id,
            app_version,
            max_recovery_attempts: 100,
            recover_on_launch: config.resolve_recover_on_launch(),
            shutdown_token: std::sync::Mutex::new(CancellationToken::new()),
            deactivated: Arc::new(AtomicBool::new(false)),
            tasks: TaskTracker::new(),
            dispatchers: std::sync::Mutex::new(Vec::new()),
            runtime: std::sync::OnceLock::new(),
        })
    }

    /// Start building an engine over `provider`, sealing registration behind
    /// [`build`](DurableEngineBuilder::build).
    ///
    /// This is the recommended construction path. Unlike [`new`](Self::new) +
    /// [`register`](Self::register), the builder collects every registration up
    /// front and `build()` rejects a duplicate workflow name with
    /// [`Error::ConflictingRegistration`] — so an ambiguous name→function
    /// registry (which recovery cannot dispatch correctly) is a build-time error
    /// rather than a silent last-writer-wins overwrite. Every
    /// `#[durare::workflow]` in the binary is collected automatically, as with
    /// [`new`](Self::new).
    ///
    /// ```no_run
    /// # use durare::{DurableEngine, PostgresProvider};
    /// # async fn f() -> durare::Result<()> {
    /// # let provider = std::sync::Arc::new(PostgresProvider::connect("postgres://localhost/db").await?);
    /// let mut b = DurableEngine::builder(provider);
    /// b.app_version("1.2.0");
    /// let engine = b.build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder(provider: Arc<dyn StateProvider>) -> DurableEngineBuilder {
        DurableEngineBuilder {
            provider,
            config: EngineConfig::default(),
            workflows: Vec::new(),
            queues: Vec::new(),
            listen_filter: None,
            max_recovery_attempts: 100,
        }
    }

    /// Connect to a state backend from a URL and return a [builder](DurableEngineBuilder).
    ///
    /// The scheme selects the provider: `postgres://…` / `postgresql://…` →
    /// [`PostgresProvider`](crate::PostgresProvider), `sqlite://…` /
    /// `sqlite::memory:` → [`SqliteProvider`](crate::SqliteProvider), and
    /// `memory:` / `memory://` → [`InMemoryProvider`](crate::InMemoryProvider).
    /// For a custom pool or system-DB schema, construct the provider yourself and
    /// use [`builder`](Self::builder).
    ///
    /// ```no_run
    /// # use durare::DurableEngine;
    /// # async fn f() -> durare::Result<()> {
    /// let engine = DurableEngine::connect("postgres://localhost/db").await?.build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect(url: &str) -> Result<DurableEngineBuilder> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            #[cfg(feature = "postgres")]
            return Ok(Self::builder(Arc::new(
                crate::PostgresProvider::connect(url).await?,
            )));
            #[cfg(not(feature = "postgres"))]
            return Err(crate::error::Error::app(format!(
                "`{url}` is a Postgres URL but the `postgres` feature is not enabled"
            )));
        }
        if url.starts_with("sqlite:") {
            #[cfg(feature = "sqlite")]
            return Ok(Self::builder(Arc::new(
                crate::SqliteProvider::connect(url).await?,
            )));
            #[cfg(not(feature = "sqlite"))]
            return Err(crate::error::Error::app(format!(
                "`{url}` is a SQLite URL but the `sqlite` feature is not enabled"
            )));
        }
        if url == "memory:" || url == "memory://" {
            return Ok(Self::builder(Arc::new(crate::InMemoryProvider::new())));
        }
        Err(crate::error::Error::app(format!(
            "unrecognized state-backend URL scheme in `{url}` \
             (expected postgres://, sqlite://, or memory:)"
        )))
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
                    tasks: self.tasks.clone(),
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

    /// The shared state backend.
    pub(crate) fn provider(&self) -> &Arc<dyn StateProvider> {
        &self.provider
    }

    /// A [`Debouncer`](crate::Debouncer) for `target_workflow`: coalesce rapid
    /// repeated triggers (grouped by key) into a single delayed run with the
    /// latest input. The target must be registered, and the engine must be
    /// launched so the internal queue runs the collector.
    pub fn debouncer(&self, target_workflow: &str) -> crate::debounce::Debouncer<'_> {
        crate::debounce::Debouncer::new(self, target_workflow)
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

    /// Register a workflow handler for a **configured instance** under `name`.
    ///
    /// Several instances can share one workflow `name`, each distinguished by its
    /// `config_name`. A run started or enqueued with a matching
    /// [`WorkflowOptions::config_name`] is dispatched to that instance's handler,
    /// and because the config name is persisted on the row, recovery re-dispatches
    /// to the same one. Register every instance (with the same config name) on
    /// each process start, before [`launch`](Self::launch). The instance's state
    /// is simply captured by the handler closure.
    pub fn register_configured<I, O, F, Fut>(&mut self, name: &str, config_name: &str, f: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.workflows
            .insert(registry_key(name, Some(config_name)), erase(f));
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
        let mut queues: Vec<WorkflowQueue> = self
            .queues
            .values()
            .filter(|q| q.name != INTERNAL_QUEUE)
            .map(|q| (**q).clone())
            .collect();
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        queues
    }

    /// Every queue persisted in the shared `queues` table, sorted by name — the
    /// database-backed, fleet-wide registry (queues any executor registered
    /// against this database persist on `launch`). Unlike
    /// [`list_registered_queues`](Self::list_registered_queues), which returns
    /// only the queues registered in *this* process, this reads the table the
    /// conductor and the DBOS control plane use.
    pub async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        self.provider.list_queues().await
    }

    /// All workflows registered on this engine — both `#[durare::workflow]`
    /// auto-registrations and manual [`register`](Self::register) calls — sorted
    /// by name. Each entry carries its cron schedule if it is a scheduled
    /// workflow.
    pub fn list_registered_workflows(&self) -> Vec<RegisteredWorkflow> {
        let schedules: HashMap<&str, &str> = self
            .scheduled
            .iter()
            .map(|(name, spec)| (name.as_str(), spec.as_str()))
            .collect();
        let mut out: Vec<RegisteredWorkflow> = self
            .workflows
            .keys()
            .map(|name| RegisteredWorkflow {
                name: name.clone(),
                cron_schedule: schedules.get(name.as_str()).map(|s| s.to_string()),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The subset of [`list_registered_workflows`](Self::list_registered_workflows)
    /// that are scheduled (have a cron schedule), sorted by name.
    pub fn list_scheduled_workflows(&self) -> Vec<RegisteredWorkflow> {
        let mut out = self.list_registered_workflows();
        out.retain(|w| w.cron_schedule.is_some());
        out
    }

    /// Create a durable cron schedule that fires `workflow_name` on each tick of
    /// `cron` (a 6-field, second-precision spec). The reconciler started by
    /// [`launch`](Self::launch) installs it on its next pass.
    ///
    /// # Errors
    ///
    /// Fails if the cron spec or `ScheduleOptions` timezone is invalid, the
    /// workflow is not registered here, or a schedule with this name already
    /// exists.
    #[doc(alias = "cron")]
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
        if !self.workflows.contains_key(workflow_name) {
            return Err(Error::UnknownWorkflow(workflow_name.to_string()));
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

    /// Create or replace each schedule by name, in one call. Useful for declaring
    /// a fixed set of schedules at startup: an existing schedule of the same name
    /// is replaced (a fresh `schedule_id`, so the reconciler re-seats its firing
    /// loop). Every entry is validated (cron, timezone, workflow registered)
    /// before any write, so a bad entry rejects the whole batch.
    pub async fn apply_schedules(&self, schedules: Vec<ApplySchedule>) -> Result<()> {
        for req in &schedules {
            if req.schedule_name.is_empty() {
                return Err(Error::app("schedule_name is required"));
            }
            parse_cron(&req.schedule)?;
            if let Some(tz) = &req.options.cron_timezone {
                parse_timezone(tz)?;
            }
            if !self.workflows.contains_key(&req.workflow_name) {
                return Err(Error::UnknownWorkflow(req.workflow_name.clone()));
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

    /// Fire a schedule's ticks for every cron instant in `(start, end)` (start
    /// exclusive, end exclusive), under the same deterministic per-tick ids the
    /// live loop uses — so a tick that already ran is skipped, not duplicated.
    /// Returns the id of every tick in the range (including skipped ones).
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
        backfill_ticks(&self.runtime(), &schedule, start, end).await
    }

    /// Fire a schedule's workflow immediately, once, returning a handle to the
    /// run. The tick uses a distinct `sched-{name}-trigger-{time}` id (it does
    /// not collide with or replace a regular cron tick) and the schedule's queue
    /// if it has one.
    pub async fn trigger_schedule<O>(&self, schedule_name: &str) -> Result<WorkflowHandle<O>>
    where
        O: DeserializeOwned,
    {
        let schedule = self
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| Error::app(format!("schedule not found: {schedule_name}")))?;
        let now = Utc::now();
        let stamp = now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let opts = WorkflowOptions {
            workflow_id: Some(format!("sched-{schedule_name}-trigger-{stamp}")),
            queue: schedule.queue_name.clone(),
            ..Default::default()
        };
        self.start(&schedule.workflow_name, schedule.tick_input(now), opts)
            .await
    }

    /// Every registered application version, newest first. Versions are recorded
    /// in `application_versions` when an engine of that version launches.
    pub async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.provider.list_application_versions().await
    }

    /// The latest registered application version (most recent `version_timestamp`),
    /// or `None` if none are registered.
    pub async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        self.provider.get_latest_application_version().await
    }

    /// Mark a registered version as latest (bumps its `version_timestamp` to now).
    /// Returns whether a matching version existed.
    pub async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        if version_name.is_empty() {
            return Err(Error::app("version_name is required"));
        }
        self.provider
            .set_latest_application_version(version_name)
            .await
    }

    /// Start background processing: one dispatcher task per registered queue and
    /// one scheduler task per `#[workflow(schedule = …)]` workflow. Workflow
    /// timeouts are enforced inline per run, so they need no separate sweep. Call
    /// once per launch; safe to call again after `shutdown`.
    ///
    /// When [`recover_on_launch`](EngineConfig::recover_on_launch) is enabled
    /// (off by default), this also recovers this executor's workflows left
    /// pending by a previous run, re-dispatching them on a background task — so a
    /// crash and restart resumes unfinished work without a separate call. It is
    /// opt-in because it is only sound when each live process has a *unique*
    /// executor id; otherwise drive recovery yourself with
    /// [`recover`](Self::recover).
    pub async fn launch(&self) -> Result<()> {
        // A deactivated process must not start claiming work again.
        if self.is_deactivated() {
            return Ok(());
        }
        // A prior `shutdown` cancelled the token and closed the tracker. Install a
        // fresh token (a cancelled one can't be reset) and reopen the tracker, so
        // this launch's loops and runs are live and the next `shutdown` stops and
        // drains them.
        let cancel = {
            let mut token = self.shutdown_token.lock().expect("shutdown token poisoned");
            if token.is_cancelled() {
                *token = CancellationToken::new();
            }
            token.clone()
        };
        self.tasks.reopen();
        let rt = self.runtime();

        // Register this process's application version.
        if let Err(e) = self
            .provider
            .create_application_version(&self.app_version)
            .await
        {
            tracing::warn!(version = %self.app_version, error = %e, "failed to register application version");
        }

        // Resolve the queue-registry conflict policy from the application version
        // (matching Go's default): this process may overwrite an existing queue
        // row only if it runs the latest registered version, so an older executor
        // mid-rolling-deploy can't clobber a newer queue's configuration. Treat a
        // lookup failure as "not latest" — don't overwrite on uncertainty. This
        // also drives the not-the-latest warning.
        let update_existing = match self.provider.get_latest_application_version().await {
            Ok(Some(latest)) => {
                if latest.version_name != self.app_version {
                    tracing::warn!(
                        current = %self.app_version, latest = %latest.version_name,
                        "current application version is not the latest"
                    );
                }
                latest.version_name == self.app_version
            }
            Ok(None) => true, // no versions registered yet: this process is first
            Err(_) => false,  // unknown: don't overwrite
        };

        // Persist the registered queues into the `queues` table so the conductor
        // (and a foreign SDK's control plane) can see this executor's queues
        // fleet-wide. The internal queue is an implementation detail and stays
        // unsurfaced, matching `list_registered_queues`.
        for queue in self.queues.values() {
            if queue.name == INTERNAL_QUEUE {
                continue;
            }
            if let Err(e) = self.provider.upsert_queue(queue, update_existing).await {
                tracing::warn!(queue = %queue.name, error = %e, "failed to persist queue to the registry");
            }
        }

        // When recovery-on-launch is opted into, snapshot this executor's pending
        // workflows *before* starting dispatchers and returning, so recovery picks
        // up only a previous run's leftovers — not a workflow this process creates
        // afterward. Opt-in (off by default) because it is only sound when each
        // live process has a unique executor id; the dispatch runs on a background
        // task below.
        let to_recover = if self.recover_on_launch {
            list_pending_workflows(&rt, std::slice::from_ref(&self.executor_id)).await?
        } else {
            Vec::new()
        };

        let mut tasks = self.dispatchers.lock().expect("dispatcher lock poisoned");
        for queue in self.queues.values() {
            // Skip queues this process is configured not to listen to. The
            // internal queue is always dispatched (re-execution depends on it).
            if let Some(listen) = &self.listen_filter {
                if queue.name != INTERNAL_QUEUE && !listen.contains(&queue.name) {
                    continue;
                }
            }
            tasks.push(tokio::spawn(queue_dispatch_loop(
                queue.clone(),
                rt.clone(),
                cancel.clone(),
            )));
        }
        tasks.push(tokio::spawn(schedule_reconciler(
            rt.clone(),
            cancel.clone(),
            self.macro_schedules(),
        )));

        // Dispatch the recovery snapshot taken above on a background task, so
        // launch stays prompt — a recovered workflow runs to completion, which
        // would otherwise block startup for its full duration. Spawned through
        // the tracker (non-queued recovered runs execute inline within it, on no
        // tracked task of their own), so `shutdown` waits the whole recovery out;
        // once shutdown begins, the dispatch loop stops starting workflows it has
        // not reached yet. (`spawn` is not an await, so holding the dispatcher
        // lock above across it is fine.)
        if !to_recover.is_empty() {
            let rt = rt.clone();
            let max = self.max_recovery_attempts;
            let cancel = cancel.clone();
            self.tasks.spawn(async move {
                match dispatch_pending_workflows(&rt, max, to_recover, &cancel).await {
                    Ok(ids) => {
                        tracing::info!(count = ids.len(), "recovered pending workflows on launch")
                    }
                    Err(e) => tracing::warn!(error = %e, "recover-on-launch failed"),
                }
            });
        }
        Ok(())
    }

    /// In-memory schedules for the `#[workflow(schedule = …)]` workflows, named
    /// after the workflow. These are *not* persisted: a decorator-style schedule
    /// is pure code, so it is re-seeded from the registry each launch and stops
    /// firing the moment the attribute is removed. Persisted, manageable
    /// schedules come from [`create_schedule`](Self::create_schedule) instead.
    /// The synthetic `macro:` id lets the reconciler tell them apart and re-seat
    /// a loop only when the cron spec changes.
    fn macro_schedules(&self) -> Vec<WorkflowSchedule> {
        self.scheduled
            .iter()
            .map(|(name, spec)| WorkflowSchedule {
                schedule_id: format!("macro:{name}:{spec}"),
                schedule_name: name.clone(),
                workflow_name: name.clone(),
                schedule: spec.clone(),
                status: ScheduleStatus::Active,
                context: None,
                last_fired_at: None,
                automatic_backfill: false,
                cron_timezone: None,
                queue_name: None,
            })
            .collect()
    }

    /// Stop claiming new work without shutting the process down: abort the queue
    /// dispatchers and the schedule reconciler, but leave in-flight workflow
    /// tasks running and keep any admin server serving. Idempotent — a second
    /// call is a no-op. Used by the admin server's `GET /deactivate`.
    pub fn deactivate(&self) {
        if self.deactivated.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::info!(executor = %self.executor_id, "deactivating executor: stopping dispatch");
        for d in self
            .dispatchers
            .lock()
            .expect("dispatcher lock poisoned")
            .drain(..)
        {
            d.abort();
        }
    }

    /// Whether [`deactivate`](Self::deactivate) has been called on this engine.
    pub fn is_deactivated(&self) -> bool {
        self.deactivated.load(Ordering::SeqCst)
    }

    /// A point-in-time readiness report: is the state backend reachable with a
    /// current system schema, and is this process dispatching work?
    ///
    /// Never fails — a probe endpoint must always produce an answer; failures
    /// are the report's *content*. One cheap backend round trip, suitable for
    /// a load-balancer or orchestrator probe interval. The admin server
    /// (feature `admin`) serves it as `GET /readyz`; wire it into your own
    /// HTTP handler otherwise:
    ///
    /// ```
    /// # use durare::{DurableEngine, InMemoryProvider, Result};
    /// # use std::sync::Arc;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<()> {
    /// let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    /// engine.launch().await?;
    /// let report = engine.health().await;
    /// assert!(report.is_ready());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health(&self) -> HealthReport {
        let database = self.provider.ping().await.err().map(|e| e.to_string());
        HealthReport {
            database,
            dispatch: self.dispatch_state(),
        }
    }

    /// The dispatch axis of [`health`](Self::health): `None` when this process
    /// is claiming work, else why not. Ordered by intent — a deliberate state
    /// (deactivated, shut down) is reported as itself, not as its side effect
    /// (aborted dispatcher tasks).
    fn dispatch_state(&self) -> Option<String> {
        if self.is_deactivated() {
            return Some("deactivated: this process stopped claiming new work".into());
        }
        if self
            .shutdown_token
            .lock()
            .expect("shutdown token poisoned")
            .is_cancelled()
        {
            return Some("shut down".into());
        }
        let dispatchers = self.dispatchers.lock().expect("dispatcher lock poisoned");
        if dispatchers.is_empty() {
            return Some("not launched".into());
        }
        // A dispatcher task can only be finished here if it died — launch
        // spawned it to run until shutdown, and both deliberate stops were
        // handled above.
        let dead = dispatchers.iter().filter(|d| d.is_finished()).count();
        if dead > 0 {
            return Some(format!(
                "{dead} of {} dispatcher tasks have exited unexpectedly",
                dispatchers.len()
            ));
        }
        None
    }

    /// Cancel every workflow still in a non-terminal queueable state
    /// (`PENDING`/`ENQUEUED`/`DELAYED`) that was created at or before
    /// `cutoff_epoch_ms`. Returns how many were cancelled. Backs the admin
    /// server's `POST /dbos-global-timeout`.
    pub async fn cancel_all_before(&self, cutoff_epoch_ms: i64) -> Result<usize> {
        let filter = ListFilter {
            status: vec![
                STATUS_PENDING.to_string(),
                STATUS_ENQUEUED.to_string(),
                STATUS_DELAYED.to_string(),
            ],
            end_time_ms: Some(cutoff_epoch_ms),
            load_input: false,
            load_output: false,
            ..Default::default()
        };
        let ids: Vec<String> = self
            .provider
            .list_workflows(&filter)
            .await?
            .into_iter()
            .map(|w| w.id)
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }
        self.cancel_workflows(&ids).await?;
        Ok(ids.len())
    }

    /// Stop the queue dispatchers and wait for in-flight workflow tasks started
    /// here to drain (up to `timeout`). Runs re-dispatched by recovery —
    /// [`recover`](Self::recover) or
    /// [`recover_on_launch`](EngineConfig::recover_on_launch) — drain too: a
    /// recovery still working through its snapshot finishes the run it is on,
    /// starts no more, and leaves the untouched remainder `PENDING` for a later
    /// recovery.
    pub async fn shutdown(&self, timeout: Duration) -> Result<()> {
        self.shutdown_token
            .lock()
            .expect("shutdown token poisoned")
            .cancel();
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
        // Drain in-flight runs, bounded by `timeout`: `close` lets `wait` return
        // once the tracked set empties. A run still going when the deadline passes
        // is left mid-flight — durable, so a later recovery resumes it.
        self.tasks.close();
        let _ = tokio::time::timeout(timeout, self.tasks.wait()).await;
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
    ///
    /// ```
    /// # use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
    /// # use std::sync::Arc;
    /// # #[durare::workflow]
    /// # async fn greet(ctx: DurableContext, name: String) -> Result<String> {
    /// #     Ok(format!("hello, {name}"))
    /// # }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<()> {
    /// # let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    /// // Start by registered name; the id is the idempotency key.
    /// let handle = engine
    ///     .start::<_, String>("greet", "world".to_string(), WorkflowOptions::with_id("greet-1"))
    ///     .await?;
    /// assert_eq!(handle.await?, "hello, world");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// For compile-time input/output checking without the turbofish, use
    /// [`start_with`](Self::start_with).
    ///
    /// # Errors
    ///
    /// [`Error::UnknownWorkflow`] if `name` is not registered;
    /// [`Error::QueueDeduplicated`] if `opts.dedup_id` collides with an active
    /// workflow on the queue (under the default `Reject` policy); an
    /// application error if a deduplication policy is set without a
    /// deduplication id.
    pub async fn start<I, O>(
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
            // An explicit empty id means "assign one for me": fall through to a
            // fresh id so an empty id is never persisted.
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let input_json = serde_json::to_value(input)?;
        if opts.dedup_policy != DeduplicationPolicy::Reject && opts.dedup_id.is_none() {
            return Err(Error::app(
                "a deduplication policy requires a deduplication id",
            ));
        }
        let handler = rt
            .workflows
            .get(&registry_key(name, opts.config_name.as_deref()))
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(name.to_string()))?;

        // A top-level run takes its identity from the options.
        let auth = AuthContext {
            authenticated_user: opts.authenticated_user.clone(),
            assumed_role: opts.assumed_role.clone(),
            authenticated_roles: opts.authenticated_roles.clone(),
        };
        // On a dedup collision under `ReturnExisting`, hand back the workflow
        // already holding the slot; retry if it was freed between insert and
        // lookup (the slot's owner just completed).
        let (canonical, queued, _created) = loop {
            match rt
                .insert_run(&id, name, input_json.clone(), &opts, None, &auth)
                .await
            {
                Ok(v) => break v,
                Err(e)
                    if opts.dedup_policy == DeduplicationPolicy::ReturnExisting
                        && e.code() == ErrorCode::QueueDeduplicated =>
                {
                    if let (Some(q), Some(d)) = (opts.queue.as_deref(), opts.dedup_id.as_deref()) {
                        if let Some(existing) =
                            self.provider.get_deduplicated_workflow(q, d).await?
                        {
                            return Ok(WorkflowHandle::polling(existing, self.provider.clone()));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        };

        // Terminal already, or owned by a queue: observe via polling.
        if queued || is_terminal(&canonical.status) {
            return Ok(WorkflowHandle::polling(id, self.provider.clone()));
        }

        let join = rt.spawn_owned(
            id.clone(),
            name,
            handler,
            canonical.input,
            canonical.deadline_ms,
            auth,
        );
        Ok(WorkflowHandle::local(id, self.provider.clone(), join))
    }

    /// Start a workflow from its typed [`WorkflowDef`] reference — the marker
    /// `#[durare::workflow]` emits — returning a [`WorkflowHandle`] immediately.
    ///
    /// The reference fixes the input and output types from the workflow's own
    /// signature, so neither needs a turbofish and a wrong input type is a
    /// compile error:
    ///
    /// ```
    /// # use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
    /// # use std::sync::Arc;
    /// # #[durare::workflow]
    /// # async fn process_order(ctx: DurableContext, order: String) -> Result<String> {
    /// #     Ok(format!("receipt for {order}"))
    /// # }
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<()> {
    /// # let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    /// # let order = String::from("1001");
    /// let handle = engine.start_with(ProcessOrder, order, WorkflowOptions::default()).await?;
    /// let receipt: String = handle.await?;
    /// # assert_eq!(receipt, "receipt for 1001");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This is sugar for [`start`](Self::start) with the name
    /// taken from `W::NAME`; set `opts.queue` to enqueue onto a queue instead of
    /// running in-process.
    pub async fn start_with<W>(
        &self,
        _wf: W,
        input: W::Input,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<W::Output>>
    where
        W: WorkflowDef,
        W::Input: Serialize,
    {
        self.start::<W::Input, W::Output>(W::NAME, input, opts)
            .await
    }

    /// Send a message to a workflow from **outside** any workflow (e.g. an API
    /// handler nudging a waiting workflow). Not durable — there is no calling
    /// workflow to checkpoint into; from workflow code use
    /// [`DurableContext::send`] instead.
    ///
    /// # Errors
    ///
    /// [`Error::NonExistentWorkflow`] if the destination workflow does not
    /// exist.
    pub async fn send<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
    ) -> Result<()> {
        self.provider
            .insert_notification(destination_id, topic, serde_json::to_value(message)?, None)
            .await
    }

    /// Like [`send`](Self::send) but idempotent: the message is delivered **at
    /// most once** for a given `idempotency_key` and destination, so a caller that
    /// retries after a transient failure never double-delivers. Two sends with the
    /// same key to the same workflow collapse to one; distinct keys each deliver.
    pub async fn send_with_idempotency_key<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
        idempotency_key: &str,
    ) -> Result<()> {
        self.provider
            .insert_notification(
                destination_id,
                topic,
                serde_json::to_value(message)?,
                Some(idempotency_key),
            )
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

    /// Count workflows grouped by one or more `workflow_status` columns and/or a
    /// `created_at` time bucket, after applying `query`'s filters. Returns one
    /// [`WorkflowAggregate`](crate::WorkflowAggregate) per non-empty group.
    ///
    /// Errors if `query` groups by nothing (no `by_*` flag and no
    /// `time_bucket_ms`).
    pub async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        if query.is_empty() {
            return Err(Error::app(
                "get_workflow_aggregates requires at least one grouping dimension",
            ));
        }
        if query.no_select() {
            return Err(Error::app(
                "get_workflow_aggregates requires at least one selected aggregate",
            ));
        }
        self.provider.get_workflow_aggregates(query).await
    }

    /// Aggregate step records grouped by function name / derived status and/or a
    /// `completed_at` time bucket, selecting count and/or max duration, after
    /// applying `query`'s filters. Returns one
    /// [`StepAggregate`](crate::StepAggregate) per non-empty group.
    ///
    /// Errors if `query` groups by nothing, or selects no aggregate.
    pub async fn get_step_aggregates(
        &self,
        query: &StepAggregateQuery,
    ) -> Result<Vec<StepAggregate>> {
        if query.no_grouping() {
            return Err(Error::app(
                "get_step_aggregates requires at least one grouping dimension",
            ));
        }
        if query.no_select() {
            return Err(Error::app(
                "get_step_aggregates requires at least one selected aggregate",
            ));
        }
        self.provider.get_step_aggregates(query).await
    }

    /// List a workflow's recorded operations. Returns each durable step / sleep /
    /// send / child invocation as a [`StepInfo`], ordered by step id. Empty for
    /// an unknown workflow or one that has run no steps.
    pub async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.provider.get_workflow_steps(workflow_id).await
    }

    /// All `(key, value)` events a workflow has set (`set_event`), ordered by key.
    pub async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        self.provider.list_workflow_events(workflow_id).await
    }

    /// All notifications in a workflow's `send`/`recv` mailbox, oldest first
    /// (including already-consumed ones).
    pub async fn list_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<crate::provider::NotificationInfo>> {
        self.provider.list_workflow_notifications(workflow_id).await
    }

    /// All of a workflow's streams, grouped by key and ordered by write offset.
    pub async fn list_workflow_streams(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<(String, Vec<Value>)>> {
        self.provider.list_workflow_streams(workflow_id).await
    }

    /// Export a workflow (and, when `export_children`, its transitive children)
    /// into the portable [`ExportedWorkflow`](crate::provider::ExportedWorkflow)
    /// form for transfer to another environment.
    pub async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<crate::provider::ExportedWorkflow>> {
        self.provider
            .export_workflow(workflow_id, export_children)
            .await
    }

    /// Import previously exported workflows, re-creating each one's durable state.
    pub async fn import_workflow(
        &self,
        workflows: &[crate::provider::ExportedWorkflow],
    ) -> Result<()> {
        self.provider.import_workflow(workflows).await
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
        crate::provider::drain_stream(self.provider.as_ref(), workflow_id, key).await
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
        crate::provider::snapshot_stream(self.provider.as_ref(), workflow_id, key, from_offset)
            .await
    }

    /// Read the durable stream `key` on `workflow_id` as an asynchronous
    /// [`Stream`](futures_util::Stream), yielding each value in order as it is
    /// committed — the incremental counterpart to [`read_stream`](Self::read_stream),
    /// which blocks and returns the whole stream at once. The stream ends when the
    /// producer closes it or goes inactive; a decode or backend failure (or a
    /// missing workflow) is the final `Err` item. A live read, not checkpointed.
    /// Consume it with [`StreamExt::next`](futures_util::StreamExt::next).
    pub fn read_stream_values<T: DeserializeOwned + 'static>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> impl futures_util::Stream<Item = Result<T>> + '_ {
        crate::provider::stream_values(self.provider.as_ref(), workflow_id, key)
    }

    /// Cancel a workflow. A non-terminal workflow is set `CANCELLED` and removed
    /// from its queue; a running workflow stops at its next step (cooperative
    /// cancellation).
    pub async fn cancel_workflow(&self, id: &str) -> Result<()> {
        self.provider.cancel_workflow(id).await
    }

    /// Resume a cancelled (or otherwise non-terminal) workflow. It is re-queued
    /// onto the internal queue — which every engine dispatches, so it always
    /// makes progress — and re-run from its checkpoints; the returned handle
    /// tracks it by polling. Resuming an already-completed workflow is a no-op:
    /// the handle simply reads its recorded outcome. A missing id is a typed
    /// [`Error::NonExistentWorkflow`]. Requires a launched engine to make
    /// progress.
    pub async fn resume_workflow<O>(&self, id: &str) -> Result<WorkflowHandle<O>> {
        self.resume_workflow_on(id, INTERNAL_QUEUE).await
    }

    /// Like [`resume_workflow`](Self::resume_workflow) but re-queues onto
    /// `queue` instead of the internal queue, so the resumed run competes under
    /// that queue's concurrency and rate limits.
    pub async fn resume_workflow_on<O>(&self, id: &str, queue: &str) -> Result<WorkflowHandle<O>> {
        if self.provider.resume_workflow(id).await? {
            self.provider.enqueue_existing(id, queue).await?;
        } else if self.provider.get_workflow_status(id).await?.is_none() {
            return Err(Error::nonexistent_workflow(id));
        }
        Ok(WorkflowHandle::polling(
            id.to_string(),
            self.provider.clone(),
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
    /// `SUCCESS`/`ERROR` is re-queued onto the internal queue for a dispatcher
    /// to re-run. A polling handle is returned for **every id that exists**, in
    /// input order — an already-terminal workflow is a no-op whose handle reads
    /// its recorded outcome; missing ids yield no handle (and no error).
    pub async fn resume_workflows<O>(&self, ids: &[String]) -> Result<Vec<WorkflowHandle<O>>> {
        self.resume_workflows_on(ids, INTERNAL_QUEUE).await
    }

    /// Like [`resume_workflows`](Self::resume_workflows) but re-queues onto
    /// `queue` instead of the internal queue.
    pub async fn resume_workflows_on<O>(
        &self,
        ids: &[String],
        queue: &str,
    ) -> Result<Vec<WorkflowHandle<O>>> {
        let resumed = self.provider.resume_workflows(ids).await?;
        for id in &resumed {
            self.provider.enqueue_existing(id, queue).await?;
        }
        let existing: std::collections::HashSet<String> = self
            .provider
            .list_workflows(&ListFilter {
                workflow_ids: ids.to_vec(),
                ..Default::default()
            })
            .await?
            .into_iter()
            .map(|w| w.id)
            .collect();
        Ok(ids
            .iter()
            .filter(|id| existing.contains(*id))
            .map(|id| WorkflowHandle::polling(id.clone(), self.provider.clone()))
            .collect())
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
    /// there. The new id comes from `opts.workflow_id` or is generated; the fork
    /// is enqueued onto `opts.queue` (the internal queue when unset, so it is
    /// always dispatched) with `opts.partition_key`, and the returned handle
    /// tracks it by polling. `opts.app_version` overrides the version stamped on
    /// the fork (e.g. forking onto a new deploy); unset, the fork inherits the
    /// original's, staying runnable by the executors that could run the original.
    pub async fn fork_workflow<O>(
        &self,
        original_id: &str,
        start_step: i32,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>> {
        if opts.partition_key.is_some() && opts.queue.is_none() {
            return Err(crate::error::Error::app(
                "a queue partition key requires a queue name",
            ));
        }
        let new_id = opts
            .workflow_id
            // An explicit empty id means "assign one for me": fall through to a
            // fresh id so an empty id is never persisted.
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        self.provider
            .fork_workflow(&ForkParams {
                original_id: original_id.to_string(),
                new_id: new_id.clone(),
                start_step,
                // An explicit empty version also means "inherit".
                app_version: opts.app_version.filter(|v| !v.is_empty()),
                queue_name: opts.queue.unwrap_or_else(|| INTERNAL_QUEUE.to_string()),
                partition_key: opts.partition_key,
            })
            .await?;
        Ok(WorkflowHandle::polling(new_id, self.provider.clone()))
    }

    /// Re-run every incomplete workflow of this engine's application version,
    /// resuming each from its checkpoints. Workflows of a different version are
    /// left alone (version-gated recovery), and a workflow recovered more than
    /// `max_recovery_attempts` times is parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. Queued workflows are returned to their
    /// queue for re-dispatch; the rest are re-run inline.
    ///
    /// This is the primary recovery entry point: call it on startup (or on
    /// demand) to resume unfinished work. [`launch`](Self::launch) can do this
    /// for you when you enable
    /// [`recover_on_launch`](EngineConfig::recover_on_launch).
    ///
    /// Returns the number of workflows that were recovered.
    pub async fn recover(&self) -> Result<usize> {
        Ok(self.recover_pending_for(&[]).await?.len())
    }

    /// Like [`recover`](Self::recover) but limited to workflows owned by the
    /// given executor ids (empty = any executor), returning the id of every
    /// workflow that was recovered. Backs the admin server's
    /// `POST /dbos-workflow-recovery`, which recovers a named set of executors.
    pub async fn recover_pending_for(&self, executor_ids: &[String]) -> Result<Vec<String>> {
        let cancel = self
            .shutdown_token
            .lock()
            .expect("shutdown token poisoned")
            .clone();
        recover_pending_workflows(
            &self.runtime(),
            self.max_recovery_attempts,
            executor_ids,
            &cancel,
        )
        .await
    }
}

/// Recover this app version's `PENDING` workflows owned by the given executors
/// (empty = any), returning the id of each one recovered. Backs
/// [`DurableEngine::recover_pending_for`] and the background recovery that
/// [`DurableEngine::launch`] spawns. Runs each recovered workflow to completion;
/// a workflow that was claimed off a queue goes back on its queue instead.
///
/// Split out as a free function so `launch` can spawn it onto a background task
/// (the engine itself is not `Clone`, but the [`Runtime`] behind it is `Arc`).
pub(crate) async fn recover_pending_workflows(
    rt: &Arc<Runtime>,
    max_recovery_attempts: i32,
    executor_ids: &[String],
    cancel: &CancellationToken,
) -> Result<Vec<String>> {
    let pending = list_pending_workflows(rt, executor_ids).await?;
    dispatch_pending_workflows(rt, max_recovery_attempts, pending, cancel).await
}

/// The `PENDING` workflows of this app version owned by the given executors
/// (empty = any). Split from dispatch so [`DurableEngine::launch`] can snapshot
/// the set *synchronously* at launch time — recovering only what a previous run
/// left behind, never a workflow this process creates after launch returns.
pub(crate) async fn list_pending_workflows(
    rt: &Arc<Runtime>,
    executor_ids: &[String],
) -> Result<Vec<WorkflowStatus>> {
    let filter = ListFilter {
        status: vec![STATUS_PENDING.to_string()],
        app_version: vec![rt.app_version.clone()],
        executor_ids: executor_ids.to_vec(),
        ..Default::default()
    };
    rt.provider.list_workflows(&filter).await
}

/// Re-dispatch a snapshot of pending workflows: each is bumped past the recovery
/// cap (parking it if exceeded), re-queued if it was claimed off a queue, or
/// otherwise re-run to completion.
///
/// Once cancellation is observed, no further workflows are started; the
/// remainder stays `PENDING` for a later recovery. Only shutdown stops the loop
/// — recovery on a [`deactivate`](DurableEngine::deactivate)d engine is
/// deliberate, since the admin recovery endpoint exists to re-dispatch work.
pub(crate) async fn dispatch_pending_workflows(
    rt: &Arc<Runtime>,
    max_recovery_attempts: i32,
    pending: Vec<WorkflowStatus>,
    cancel: &CancellationToken,
) -> Result<Vec<String>> {
    let total = pending.len();
    let mut recovered = Vec::new();
    for (dispatched, record) in pending.into_iter().enumerate() {
        // Checked before each record — and before its recovery-attempt bump, so
        // a workflow this loop never reaches doesn't burn an attempt. The run
        // already in flight is not cut; shutdown waits it out via the caller's
        // drain guard.
        if cancel.is_cancelled() {
            tracing::info!(
                dispatched,
                remaining = total - dispatched,
                "shutdown began during recovery; leaving the remaining pending workflows for a later recovery"
            );
            break;
        }
        let attempts = rt
            .provider
            .bump_recovery_attempts(&record.id, max_recovery_attempts)
            .await?;
        if attempts > max_recovery_attempts {
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
            rt.provider
                .set_workflow_status(&record.id, STATUS_ENQUEUED, None, None)
                .await?;
            recovered.push(record.id);
            continue;
        }

        if let Some(handler) = rt
            .workflows
            .get(&registry_key(&record.name, record.config_name.as_deref()))
            .cloned()
        {
            // Best-effort: a workflow that fails again is marked ERROR by
            // `run_to_completion`; we keep going with the rest.
            let auth = AuthContext::from_status(&record);
            let span = rt.workflow_span(&record.id, &record.name, None, &auth);
            let _ = run_to_completion(
                rt.clone(),
                handler,
                record.id.clone(),
                record.input.clone(),
                record.deadline_ms,
                auth,
                span,
            )
            .await;
            recovered.push(record.id);
        } else {
            tracing::warn!(
                workflow = %record.name,
                id = %record.id,
                "skipping recovery: no handler registered for this workflow name"
            );
        }
    }
    Ok(recovered)
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
    tasks: TaskTracker,
}

impl Runtime {
    pub(crate) fn provider(&self) -> &Arc<dyn StateProvider> {
        &self.provider
    }

    /// The span covering one workflow execution, carrying the DBOS trace
    /// attributes (see the [`observability`](crate::observability) guide).
    /// Created at the call site so a run started from inside another traced
    /// context — a child workflow, an instrumented HTTP handler — parents
    /// under it contextually.
    fn workflow_span(
        &self,
        id: &str,
        name: &str,
        queue: Option<&str>,
        auth: &AuthContext,
    ) -> tracing::Span {
        // Roles are recorded as a JSON array string, matching what the other
        // DBOS SDKs emit for this attribute.
        let roles = if auth.authenticated_roles.is_empty() {
            None
        } else {
            serde_json::to_string(&auth.authenticated_roles).ok()
        };
        tracing::info_span!(
            "workflow",
            otel.name = %name,
            dbos.operation.type = "workflow",
            dbos.operation.workflow_id = %id,
            dbos.application.version = %self.app_version,
            dbos.executor.id = %self.executor_id,
            dbos.queue.name = queue,
            dbos.user.name = auth.authenticated_user.as_deref(),
            dbos.user.assumed_role = auth.assumed_role.as_deref(),
            dbos.user.roles = roles.as_deref(),
            dbos.workflow.status = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        )
    }

    /// Build a new run's status row from `opts` and persist it idempotently,
    /// returning the canonical row, whether it was routed to a queue, and whether
    /// **this call** created the row (the arbiter when executors race a
    /// deterministic id). Shared by top-level runs and child workflows;
    /// `parent_id`/`auth` are stamped on the row.
    async fn insert_run(
        &self,
        id: &str,
        name: &str,
        input_json: Value,
        opts: &WorkflowOptions,
        parent_id: Option<&str>,
        auth: &AuthContext,
    ) -> Result<(WorkflowStatus, bool, bool)> {
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

        let app_version = opts.app_version.as_deref().unwrap_or(&self.app_version);
        let mut row = WorkflowStatus::new(id, name, input_json, status, executor, app_version);
        row.queue_name = opts.queue.clone();
        row.priority = opts.priority;
        row.queue_partition_key = opts.partition_key.clone();
        row.dedup_id = opts.dedup_id.clone();
        row.parent_workflow_id = parent_id.map(|s| s.to_string());
        row.authenticated_user = auth.authenticated_user.clone();
        row.assumed_role = auth.assumed_role.clone();
        row.authenticated_roles = auth.authenticated_roles.clone();
        row.class_name = opts.class_name.clone();
        row.config_name = opts.config_name.clone();
        row.timeout_ms = opts.timeout.map(|d| d.as_millis() as i64);
        row.delay_until_ms = opts.delay.map(|d| now_ms + d.as_millis() as i64);
        if !queued {
            // Direct runs start the instant they are created, so the deadline is
            // fixed here (queued runs get theirs when a dispatcher claims them).
            // Derive both from the row's own `created_at` rather than the separate,
            // earlier `now_ms` read: otherwise `started_at` can land a millisecond
            // before `created_at`, making queue-wait (`started_at - created_at`)
            // spuriously negative — a row that "started before it was created".
            let created_ms = row.created_at.timestamp_millis();
            row.started_at_ms = Some(created_ms);
            row.deadline_ms = row.timeout_ms.map(|t| created_ms + t);
        }

        let (canonical, created) = self.provider.insert_workflow_status(row).await?;
        Ok((canonical, queued, created))
    }

    /// Spawn a run on a task this caller owns (for a local [`WorkflowHandle`]).
    /// Spawned through the [`TaskTracker`] so [`DurableEngine::shutdown`] waits it
    /// out — the task is counted from the moment it is created.
    fn spawn_owned(
        self: &Arc<Self>,
        id: String,
        name: &str,
        handler: WorkflowFn,
        input: Value,
        deadline_ms: Option<i64>,
        auth: AuthContext,
    ) -> JoinHandle<Result<Value>> {
        // Built here, in the caller's context, so a child workflow's span
        // parents under the workflow (or handler) span that started it.
        let span = self.workflow_span(&id, name, None, &auth);
        let rt = self.clone();
        self.tasks.spawn(async move {
            run_to_completion(rt, handler, id, input, deadline_ms, auth, span).await
        })
    }

    /// Spawn a run on a self-owned, detached task; the result is observed by
    /// polling the status row. Used for queue claims, recovery, schedules, and
    /// child workflows.
    fn spawn_detached(
        self: &Arc<Self>,
        id: String,
        name: &str,
        handler: WorkflowFn,
        input: Value,
        deadline_ms: Option<i64>,
        auth: AuthContext,
    ) {
        let join = self.spawn_owned(id, name, handler, input, deadline_ms, auth);
        // Detach: the run is tracked by the `TaskTracker`, so shutdown still
        // drains it.
        drop(join);
    }

    /// Persist one scheduled tick at `instant` under the deterministic
    /// `sched-{name}-{time}` id (idempotent, so the tick is created at most once
    /// across executors). Returns the canonical row, whether it is queued,
    /// whether this call created it, and the id. Pairs with
    /// [`launch_scheduled_tick`](Self::launch_scheduled_tick).
    async fn persist_scheduled_tick(
        &self,
        schedule: &WorkflowSchedule,
        instant: DateTime<Utc>,
    ) -> Result<(WorkflowStatus, bool, bool, String)> {
        let wf_id = format!("sched-{}-{}", schedule.schedule_name, instant.to_rfc3339());
        let opts = WorkflowOptions {
            workflow_id: Some(wf_id.clone()),
            queue: schedule.queue_name.clone(),
            ..Default::default()
        };
        let auth = AuthContext::default();
        let input = serde_json::to_value(schedule.tick_input(instant))?;
        let (canonical, queued, created) = self
            .insert_run(&wf_id, &schedule.workflow_name, input, &opts, None, &auth)
            .await?;
        Ok((canonical, queued, created, wf_id))
    }

    /// Run a freshly-persisted tick now if this executor's insert created it
    /// (won the cross-executor race) and it is a direct (unqueued) run that has
    /// not finished. A queued tick is left for a dispatcher to claim. The
    /// arbiter is the idempotent insert itself, NOT executor-id equality —
    /// executor ids are not unique across processes (every local process
    /// defaults to `"local"`), so id equality would double-fire the tick.
    fn launch_scheduled_tick(
        self: &Arc<Self>,
        schedule: &WorkflowSchedule,
        canonical: WorkflowStatus,
        queued: bool,
        created: bool,
        id: &str,
    ) {
        if queued || !created || is_terminal(&canonical.status) {
            return;
        }
        if let Some(handler) = self.workflows.get(&schedule.workflow_name).cloned() {
            self.spawn_detached(
                id.to_string(),
                &schedule.workflow_name,
                handler,
                canonical.input,
                canonical.deadline_ms,
                AuthContext::default(),
            );
        }
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
            .get(&registry_key(name, opts.config_name.as_deref()))
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(name.to_string()))?;
        let (canonical, queued, _created) = self
            .insert_run(child_id, name, input_json, &opts, Some(parent_id), &auth)
            .await?;
        if !queued && !is_terminal(&canonical.status) {
            self.spawn_detached(
                child_id.to_string(),
                name,
                handler,
                canonical.input,
                canonical.deadline_ms,
                auth,
            );
        }
        Ok(())
    }
}

/// Releases a per-partition worker-concurrency slot when a queued run finishes,
/// even if it panics. (Shutdown-drain is handled separately by the engine's
/// [`TaskTracker`]; this only bounds how many runs a queue starts at once.)
struct RunningGuard(Arc<AtomicUsize>);
impl Drop for RunningGuard {
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
    cancel: CancellationToken,
) {
    let provider = rt.provider.clone();
    let executor_id = rt.executor_id.clone();
    let app_version = rt.app_version.clone();
    // Local running count per partition key (`""` for a non-partitioned queue).
    let local_running: std::sync::Mutex<HashMap<String, Arc<AtomicUsize>>> = Default::default();
    let mut interval = queue.base_polling_interval;

    loop {
        if cancel.is_cancelled() {
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
                        let Some(handler) = rt
                            .workflows
                            .get(&registry_key(&wf.name, wf.config_name.as_deref()))
                            .cloned()
                        else {
                            // Release the claim instead of stranding the row
                            // PENDING under an executor that can never run it,
                            // so an executor that does have the handler can
                            // claim it on its next poll. (Go logs and abandons
                            // the row here — a deliberate improvement.)
                            tracing::error!(
                                workflow = %wf.name,
                                id = %wf.id,
                                "dequeued workflow has no registered handler; releasing the claim"
                            );
                            let _ = provider
                                .set_workflow_status(&wf.id, STATUS_ENQUEUED, None, None)
                                .await;
                            continue;
                        };
                        counter.fetch_add(1, Ordering::Relaxed);
                        let run_rt = rt.clone();
                        let local_guard = RunningGuard(counter.clone());
                        let auth = AuthContext::from_status(&wf);
                        let span =
                            rt.workflow_span(&wf.id, &wf.name, wf.queue_name.as_deref(), &auth);
                        // Spawn through the tracker so `shutdown` drains this run.
                        rt.tasks.spawn(async move {
                            let _local = local_guard;
                            // Terminal state is recorded by run_to_completion;
                            // a handle observing this workflow polls it.
                            let _ = run_to_completion(
                                run_rt,
                                handler,
                                wf.id,
                                wf.input,
                                wf.deadline_ms,
                                auth,
                                span,
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
        // Wake immediately on shutdown rather than sleeping out the poll interval.
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval.mul_f64(jitter)) => {}
        }
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
    span: tracing::Span,
) -> Result<Value> {
    // The span covers the whole execution, terminal status write included;
    // `recorder` sets the outcome fields declared `Empty` at creation.
    let recorder = span.clone();
    async move {
    let provider = rt.provider().clone();
    let ctx = DurableContext::new(id.clone(), rt, auth);
    // Catch a panic in the workflow body so it can't unwind past the status
    // write below — which would strand the row PENDING with observers waiting
    // forever (finding F1). Steps catch their own panics (subject to retry);
    // this handles a panic in the workflow body itself.
    let run = AssertUnwindSafe(handler(ctx, input)).catch_unwind();

    // Enforce a workflow deadline if one was set: when it elapses, the run
    // future is dropped (cancelled at its next await) and the workflow is
    // marked CANCELLED. `caught` is `Ok(returned)` if the body finished (returned
    // Ok or Err) or `Err(panic)` if it panicked.
    let caught = match deadline_ms {
        Some(dl) => {
            let remaining = (dl - chrono::Utc::now().timestamp_millis()).max(0) as u64;
            match tokio::time::timeout(Duration::from_millis(remaining), run).await {
                Ok(caught) => caught,
                Err(_elapsed) => {
                    provider
                        .set_workflow_status(&id, STATUS_CANCELLED, None, Some("deadline exceeded"))
                        .await?;
                    recorder.record("dbos.workflow.status", STATUS_CANCELLED);
                    recorder.record("otel.status_code", "ERROR");
                    return Err(Error::Timeout);
                }
            }
        }
        None => run.await,
    };

    // A panic in the workflow body is treated as a *recoverable* failure, like a
    // crash — not a terminal error (finding F1, option B; the durable-execution
    // norm, where only a returned error terminates a workflow). Leave the row in
    // its current non-terminal state so a later `recover()` re-runs it from its
    // checkpoints, bounded by the recovery-attempt cap (a deterministic panic
    // eventually dead-letters). Surface the panic to the owning caller, but write
    // no terminal status.
    let result = match caught {
        Ok(returned) => returned,
        Err(payload) => {
            let msg = panic_message(&*payload);
            tracing::error!(id = %id, panic = %msg, "workflow panicked; left recoverable for recovery to re-run");
            // No `dbos.workflow.status`: the row keeps its non-terminal state.
            recorder.record("otel.status_code", "ERROR");
            return Err(Error::app(format!("workflow panicked: {msg}")));
        }
    };

    match result {
        Ok(output) => {
            provider
                .set_workflow_status(&id, STATUS_SUCCESS, Some(&output), None)
                .await?;
            recorder.record("dbos.workflow.status", STATUS_SUCCESS);
            recorder.record("otel.status_code", "OK");
            Ok(output)
        }
        Err(Error::Cancelled(_)) => {
            // The workflow stopped because it was cancelled; reflect that
            // terminal state rather than ERROR.
            provider
                .set_workflow_status(&id, STATUS_CANCELLED, None, Some("cancelled"))
                .await?;
            recorder.record("dbos.workflow.status", STATUS_CANCELLED);
            recorder.record("otel.status_code", "ERROR");
            Err(Error::Cancelled(id))
        }
        Err(e) => {
            // Encode the error in the provider's format: a portable provider
            // stores the cross-language envelope (carrying a structured
            // `Error::Portable`'s name/code/data), others store the bare message.
            let stored = crate::serialize::encode_error(&provider.serializer(), &e);
            provider
                .set_workflow_status(&id, STATUS_ERROR, None, Some(&stored))
                .await?;
            recorder.record("dbos.workflow.status", STATUS_ERROR);
            recorder.record("otel.status_code", "ERROR");
            Err(e)
        }
    }
    }
    .instrument(span)
    .await
}

/// A firing loop the reconciler has installed for one schedule.
struct InstalledSchedule {
    /// The schedule row's id when installed; a change means it was recreated, so
    /// the loop is retired and replaced.
    schedule_id: String,
    /// Set to stop the firing loop (schedule paused, deleted, or recreated).
    stop: Arc<AtomicBool>,
}

/// Parse a 6-field (second-precision) cron spec, mapping the parse error to an
/// application error.
pub(crate) fn parse_cron(spec: &str) -> Result<cron::Schedule> {
    cron::Schedule::from_str(spec)
        .map_err(|e| Error::app(format!("invalid cron schedule `{spec}`: {e}")))
}

/// Validate an IANA timezone name (e.g. `America/New_York`).
pub(crate) fn parse_timezone(tz: &str) -> Result<chrono_tz::Tz> {
    tz.parse::<chrono_tz::Tz>()
        .map_err(|_| Error::app(format!("invalid cron timezone `{tz}`")))
}

/// The next cron instant strictly after `after`, interpreting the spec in `tz`
/// (an IANA name; `None` is UTC) but always returned in UTC. `None` if the
/// schedule has no further ticks or the timezone is invalid.
fn next_cron_instant(
    cron: &cron::Schedule,
    tz: Option<&str>,
    after: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    match tz {
        Some(name) => {
            let zone = name.parse::<chrono_tz::Tz>().ok()?;
            cron.after(&after.with_timezone(&zone))
                .next()
                .map(|t| t.with_timezone(&Utc))
        }
        None => cron.after(&after).next(),
    }
}

/// Every cron instant in `(start, end)` (start exclusive, end exclusive), in UTC.
pub(crate) fn cron_ticks_between(
    cron: &cron::Schedule,
    tz: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    let mut out = Vec::new();
    let mut cursor = start;
    while let Some(next) = next_cron_instant(cron, tz, cursor) {
        if next >= end {
            break;
        }
        out.push(next);
        cursor = next;
    }
    out
}

/// Fire every cron tick of `schedule` in `(start, end)` under the deterministic
/// per-tick id; an already-persisted tick is skipped (the idempotent insert
/// dedups). Returns the id of every tick in the range, in order.
async fn backfill_ticks(
    rt: &Arc<Runtime>,
    schedule: &WorkflowSchedule,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<String>> {
    let cron = parse_cron(&schedule.schedule)?;
    let mut ids = Vec::new();
    for instant in cron_ticks_between(&cron, schedule.cron_timezone.as_deref(), start, end) {
        let (canonical, queued, created, id) = rt.persist_scheduled_tick(schedule, instant).await?;
        rt.launch_scheduled_tick(schedule, canonical, queued, created, &id);
        ids.push(id);
    }
    Ok(ids)
}

/// Reconciles the desired set of schedules with running firing loops. Each pass
/// the desired set is the persisted `ACTIVE` schedules plus the in-memory
/// `macro_schedules` (the `#[workflow(schedule)]` ones, re-seeded every launch),
/// with a persisted schedule shadowing a macro one of the same name. It installs
/// a [`schedule_fire_loop`] for any newly desired schedule and retires loops
/// whose schedule was paused, deleted, recreated, or (for a macro) removed from
/// the code.
async fn schedule_reconciler(
    rt: Arc<Runtime>,
    cancel: CancellationToken,
    macro_schedules: Vec<WorkflowSchedule>,
) {
    let mut installed: HashMap<String, InstalledSchedule> = HashMap::new();

    loop {
        if cancel.is_cancelled() {
            for entry in installed.values() {
                entry.stop.store(true, Ordering::Relaxed);
            }
            return;
        }

        let mut desired = match rt
            .provider
            .list_schedules(&ScheduleFilter {
                statuses: vec![ScheduleStatus::Active],
                ..Default::default()
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "schedule reconciler: failed to list schedules");
                sleep_until_or_shutdown(SCHEDULE_RECONCILE_INTERVAL, &cancel).await;
                continue;
            }
        };

        // Add macro schedules not shadowed by a persisted one of the same name.
        let persisted: std::collections::HashSet<&str> =
            desired.iter().map(|s| s.schedule_name.as_str()).collect();
        let extra: Vec<WorkflowSchedule> = macro_schedules
            .iter()
            .filter(|m| !persisted.contains(m.schedule_name.as_str()))
            .cloned()
            .collect();
        desired.extend(extra);

        // Retire loops whose schedule is gone, no longer desired, or recreated.
        installed.retain(|name, entry| {
            let keep = desired
                .iter()
                .any(|s| s.schedule_name == *name && s.schedule_id == entry.schedule_id);
            if !keep {
                entry.stop.store(true, Ordering::Relaxed);
            }
            keep
        });

        // Install loops for newly desired schedules.
        for schedule in desired {
            if installed.contains_key(&schedule.schedule_name) {
                continue;
            }
            // Catch up missed ticks before this loop starts firing live: from
            // just after the last fired tick to now. Opt-in per schedule.
            if schedule.automatic_backfill {
                if let Some(last) = schedule.last_fired_at {
                    let start = last + chrono::Duration::seconds(1);
                    let end = Utc::now();
                    if start < end {
                        if let Err(e) = backfill_ticks(&rt, &schedule, start, end).await {
                            tracing::error!(
                                schedule = %schedule.schedule_name, error = %e,
                                "automatic backfill failed"
                            );
                        }
                    }
                }
            }
            let stop = Arc::new(AtomicBool::new(false));
            installed.insert(
                schedule.schedule_name.clone(),
                InstalledSchedule {
                    schedule_id: schedule.schedule_id.clone(),
                    stop: stop.clone(),
                },
            );
            tokio::spawn(schedule_fire_loop(
                schedule,
                rt.clone(),
                stop,
                cancel.clone(),
            ));
        }

        sleep_until_or_shutdown(SCHEDULE_RECONCILE_INTERVAL, &cancel).await;
    }
}

/// Per-schedule cron loop: at each tick, start the workflow under a
/// deterministic id derived from the tick time, so the run happens exactly once
/// even across multiple executors (the idempotent status insert is the
/// arbiter). The per-tick id has the form `sched-{name}-{time}`. Exits when the
/// reconciler sets `stop` or the engine is shutting down.
async fn schedule_fire_loop(
    schedule: WorkflowSchedule,
    rt: Arc<Runtime>,
    stop: Arc<AtomicBool>,
    cancel: CancellationToken,
) {
    let cron = match parse_cron(&schedule.schedule) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                schedule = %schedule.schedule_name, spec = %schedule.schedule, error = %e,
                "invalid cron schedule; not firing"
            );
            return;
        }
    };
    let tz = schedule.cron_timezone.as_deref();

    loop {
        if stop.load(Ordering::Relaxed) || cancel.is_cancelled() {
            return;
        }
        let Some(next) = next_cron_instant(&cron, tz, Utc::now()) else {
            return;
        };
        let wait = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        if !sleep_until_or_stop(wait, &stop, &cancel).await {
            return;
        }

        match rt.persist_scheduled_tick(&schedule, next).await {
            Ok((canonical, queued, created, id)) => {
                // Test hook: simulate an abrupt failure of the scheduling
                // process right after the tick is persisted but before it runs
                // (and before `last_fired_at` is stamped). Recovery must then
                // complete the orphaned PENDING tick exactly once. A no-op unless
                // armed via the `fail` registry. See `tests/schedule_failpoint.rs`.
                fail::fail_point!("schedule_tick_after_persist", |_| {});

                rt.launch_scheduled_tick(&schedule, canonical, queued, created, &id);
            }
            Err(e) => {
                tracing::warn!(
                    schedule = %schedule.schedule_name, error = %e,
                    "failed to persist scheduled tick"
                );
            }
        }

        // Test hook: simulate the scheduling process dying after the tick was
        // dispatched but before `last_fired_at` is recorded. The dispatched run
        // still completes; only the bookkeeping write is skipped. A no-op unless
        // armed via the `fail` registry. See `tests/schedule_failpoint.rs`.
        fail::fail_point!("schedule_tick_before_reschedule", |_| {});

        let _ = rt
            .provider
            .set_schedule_last_fired(&schedule.schedule_name, next.timestamp_millis())
            .await;
    }
}

/// Sleep up to `dur`, waking immediately if the engine starts shutting down.
/// Used by the reconciler between passes.
async fn sleep_until_or_shutdown(dur: Duration, cancel: &CancellationToken) {
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep(dur) => {}
    }
}

/// Sleep up to `dur`, returning `false` early if `stop` is set (polled in short
/// slices, since it is not awaitable) or the engine starts shutting down
/// (immediate). Returns `true` if the full duration elapsed.
async fn sleep_until_or_stop(dur: Duration, stop: &AtomicBool, cancel: &CancellationToken) -> bool {
    let deadline = std::time::Instant::now() + dur;
    loop {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return true;
        }
        let slice = (deadline - now).min(Duration::from_millis(100));
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = tokio::time::sleep(slice) => {}
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    /// Env-mutating tests share the process environment; serialize them.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII guard restoring an env var on drop, so a panicking assert can't
    /// leak state into the other tests.
    struct EnvVar(&'static str, Option<String>);
    impl EnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let prior = std::env::var(key).ok();
            std::env::set_var(key, value);
            EnvVar(key, prior)
        }
    }
    impl Drop for EnvVar {
        fn drop(&mut self) {
            match &self.1 {
                Some(v) => std::env::set_var(self.0, v),
                None => std::env::remove_var(self.0),
            }
        }
    }

    #[test]
    fn defaults_are_binary_hash_and_local() {
        let _g = ENV_LOCK.lock().unwrap();
        let cfg = EngineConfig::default();
        let ver = cfg.resolve_app_version();
        assert_eq!(ver.len(), 64, "sha-256 hex of the executable: {ver}");
        assert!(ver.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(cfg.resolve_executor_id(), "local");
    }

    #[test]
    fn explicit_config_wins_over_defaults() {
        let _g = ENV_LOCK.lock().unwrap();
        let cfg = EngineConfig::default()
            .app_version("9.9.9")
            .executor_id("exec-7");
        assert_eq!(cfg.resolve_app_version(), "9.9.9");
        assert_eq!(cfg.resolve_executor_id(), "exec-7");
    }

    #[test]
    fn env_overrides_explicit_config() {
        let _g = ENV_LOCK.lock().unwrap();
        let _v = EnvVar::set("DBOS__APPVERSION", "env-ver");
        let _e = EnvVar::set("DBOS__VMID", "env-vm");
        let cfg = EngineConfig::default()
            .app_version("9.9.9")
            .executor_id("exec-7");
        assert_eq!(cfg.resolve_app_version(), "env-ver");
        assert_eq!(cfg.resolve_executor_id(), "env-vm");
    }

    #[test]
    fn empty_env_is_ignored() {
        let _g = ENV_LOCK.lock().unwrap();
        let _v = EnvVar::set("DBOS__APPVERSION", "");
        let _e = EnvVar::set("DBOS__VMID", "");
        let cfg = EngineConfig::default().app_version("9.9.9");
        assert_eq!(cfg.resolve_app_version(), "9.9.9");
        assert_eq!(cfg.resolve_executor_id(), "local");
    }
}
