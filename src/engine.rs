use crate::context::{AuthContext, DurableContext};
use crate::error::{Error, ErrorCode, Result};
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
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

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

/// A compile-time, typed reference to a registered workflow.
///
/// `#[durust::workflow]` emits one of these for every annotated function: a
/// zero-sized marker named by the function in `UpperCamelCase` (so
/// `process_order` yields a `ProcessOrder` marker). Passing the marker to
/// [`DurableEngine::start_with`] fixes the input and output types from the
/// function's own signature, so the call is checked without a turbofish and a
/// wrong input type is a compile error:
///
/// ```ignore
/// #[durust::workflow]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { /* … */ }
///
/// let handle = engine.start_with(ProcessOrder, order, opts).await?; // input: Order, checked
/// let receipt: Receipt = handle.await?;                             // output: Receipt, inferred
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
/// `#[durust::workflow]` macro name a workflow's output as
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
    /// Set by [`deactivate`](Self::deactivate): this process stops claiming new
    /// work (dispatchers/scheduler aborted) but keeps serving in-flight runs and
    /// the admin server. Idempotent.
    deactivated: Arc<AtomicBool>,
    /// Count of workflow tasks this process is currently running, so
    /// [`shutdown`](Self::shutdown) can drain before returning.
    inflight: Arc<AtomicUsize>,
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

    /// Set the recovery-attempt cap before a workflow is parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED` (default 100).
    pub fn max_recovery_attempts(&mut self, max: i32) -> &mut Self {
        self.max_recovery_attempts = max;
        self
    }

    /// Initialize the backend and build the engine, collecting every
    /// `#[durust::workflow]` in the binary plus the explicit registrations into
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
        // Auto-registered `#[durust::workflow]`s.
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
            shutting_down: Arc::new(AtomicBool::new(false)),
            deactivated: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
            dispatchers: std::sync::Mutex::new(Vec::new()),
            runtime: std::sync::OnceLock::new(),
        })
    }
}

impl DurableEngine {
    /// Create an engine with a generated executor id and a default app version.
    ///
    /// Every workflow annotated with `#[durust::workflow]` anywhere in the binary
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
            shutting_down: Arc::new(AtomicBool::new(false)),
            deactivated: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
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
    /// `#[durust::workflow]` in the binary is collected automatically, as with
    /// [`new`](Self::new).
    ///
    /// ```no_run
    /// # use durust::{DurableEngine, PostgresProvider};
    /// # async fn f() -> durust::Result<()> {
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
    /// # use durust::DurableEngine;
    /// # async fn f() -> durust::Result<()> {
    /// let engine = DurableEngine::connect("postgres://localhost/db").await?.build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect(url: &str) -> Result<DurableEngineBuilder> {
        let provider: Arc<dyn StateProvider> =
            if url.starts_with("postgres://") || url.starts_with("postgresql://") {
                Arc::new(crate::PostgresProvider::connect(url).await?)
            } else if url.starts_with("sqlite:") {
                Arc::new(crate::SqliteProvider::connect(url).await?)
            } else if url == "memory:" || url == "memory://" {
                Arc::new(crate::InMemoryProvider::new())
            } else {
                return Err(crate::error::Error::app(format!(
                    "unrecognized state-backend URL scheme in `{url}` \
                 (expected postgres://, sqlite://, or memory:)"
                )));
            };
        Ok(Self::builder(provider))
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

    /// All workflows registered on this engine — both `#[durust::workflow]`
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
    /// [`launch`](Self::launch) installs it on its next pass. Errors if the cron
    /// spec is invalid, the workflow is not registered here, or a schedule with
    /// this name already exists.
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
        self.run_workflow(&schedule.workflow_name, schedule.tick_input(now), opts)
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
    pub async fn launch(&self) -> Result<()> {
        // A deactivated process must not start claiming work again.
        if self.is_deactivated() {
            return Ok(());
        }
        self.shutting_down.store(false, Ordering::Relaxed);
        let rt = self.runtime();

        // Register this process's application version and warn if it is not the
        // latest (a newer deploy has registered a higher version).
        if let Err(e) = self
            .provider
            .create_application_version(&self.app_version)
            .await
        {
            tracing::warn!(version = %self.app_version, error = %e, "failed to register application version");
        } else if let Ok(Some(latest)) = self.provider.get_latest_application_version().await {
            if latest.version_name != self.app_version {
                tracing::warn!(
                    current = %self.app_version, latest = %latest.version_name,
                    "current application version is not the latest"
                );
            }
        }

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
                self.shutting_down.clone(),
            )));
        }
        tasks.push(tokio::spawn(schedule_reconciler(
            rt.clone(),
            self.shutting_down.clone(),
            self.macro_schedules(),
        )));
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
            handler,
            canonical.input,
            canonical.deadline_ms,
            auth,
        );
        Ok(WorkflowHandle::local(id, self.provider.clone(), join))
    }

    /// Start a workflow from its typed [`WorkflowDef`] reference — the marker
    /// `#[durust::workflow]` emits — returning a [`WorkflowHandle`] immediately.
    ///
    /// The reference fixes the input and output types from the workflow's own
    /// signature, so neither needs a turbofish and a wrong input type is a
    /// compile error:
    ///
    /// ```ignore
    /// let handle = engine.start_with(ProcessOrder, order, WorkflowOptions::default()).await?;
    /// let receipt: Receipt = handle.await?;
    /// ```
    ///
    /// This is sugar for [`run_workflow`](Self::run_workflow) with the name
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
        self.run_workflow::<W::Input, W::Output>(W::NAME, input, opts)
            .await
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
    /// queue for re-dispatch; the rest are re-run inline. Call once on startup.
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
        let filter = ListFilter {
            status: vec![STATUS_PENDING.to_string()],
            app_version: vec![self.app_version.clone()],
            executor_ids: executor_ids.to_vec(),
            ..Default::default()
        };
        let pending = self.provider.list_workflows(&filter).await?;
        let rt = self.runtime();
        let mut recovered = Vec::new();
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
                let _ = run_to_completion(
                    rt.clone(),
                    handler,
                    record.id.clone(),
                    record.input.clone(),
                    record.deadline_ms,
                    AuthContext::from_status(&record),
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
            // Encode the error in the provider's format: a portable provider
            // stores the cross-language envelope (carrying a structured
            // `Error::Portable`'s name/code/data), others store the bare message.
            let stored = crate::serialize::encode_error(&provider.serializer(), &e);
            provider
                .set_workflow_status(&id, STATUS_ERROR, None, Some(&stored))
                .await?;
            Err(e)
        }
    }
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
    shutting_down: Arc<AtomicBool>,
    macro_schedules: Vec<WorkflowSchedule>,
) {
    let mut installed: HashMap<String, InstalledSchedule> = HashMap::new();

    loop {
        if shutting_down.load(Ordering::Relaxed) {
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
                sleep_until_or_shutdown(SCHEDULE_RECONCILE_INTERVAL, &shutting_down).await;
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
                shutting_down.clone(),
            ));
        }

        sleep_until_or_shutdown(SCHEDULE_RECONCILE_INTERVAL, &shutting_down).await;
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
    shutting_down: Arc<AtomicBool>,
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
        if stop.load(Ordering::Relaxed) || shutting_down.load(Ordering::Relaxed) {
            return;
        }
        let Some(next) = next_cron_instant(&cron, tz, Utc::now()) else {
            return;
        };
        let wait = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        if !sleep_until_or_stop(wait, &stop, &shutting_down).await {
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

/// Sleep up to `dur`, returning early if the engine starts shutting down. Used
/// by the reconciler between passes.
async fn sleep_until_or_shutdown(dur: Duration, shutting_down: &Arc<AtomicBool>) {
    sleep_until_or_stop(dur, &Arc::new(AtomicBool::new(false)), shutting_down).await;
}

/// Sleep up to `dur` in short slices so `stop`/`shutting_down` are observed
/// promptly. Returns `true` if the full duration elapsed, `false` if it was cut
/// short by a stop/shutdown signal.
async fn sleep_until_or_stop(
    dur: Duration,
    stop: &Arc<AtomicBool>,
    shutting_down: &Arc<AtomicBool>,
) -> bool {
    let deadline = std::time::Instant::now() + dur;
    loop {
        if stop.load(Ordering::Relaxed) || shutting_down.load(Ordering::Relaxed) {
            return false;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            return true;
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(100))).await;
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
