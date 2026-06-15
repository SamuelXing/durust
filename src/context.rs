use crate::engine::{Runtime, WorkflowOptions};
use crate::error::{Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{StateProvider, WorkflowStatus, STATUS_CANCELLED};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Retry policy for a durable step — the Rust analog of Go's `WithStepMaxRetries`
/// / `WithBackoffFactor` / `WithBaseInterval` / `WithMaxInterval`.
///
/// Defaults match Go: no retries, factor 2.0, 100ms base, 5s cap.
#[derive(Clone)]
pub struct StepOptions {
    /// Step name recorded with the checkpoint.
    pub name: String,
    /// Additional attempts after the first failure (0 = run once, no retry).
    pub max_retries: u32,
    /// Exponential backoff multiplier between attempts.
    pub backoff_factor: f64,
    /// Delay before the first retry.
    pub base_interval: Duration,
    /// Upper bound on any single backoff delay.
    pub max_interval: Duration,
}

impl StepOptions {
    /// Default policy (no retries) for a step named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            max_retries: 0,
            backoff_factor: 2.0,
            base_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
        }
    }

    /// Set the number of retries (attempts after the first).
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the backoff multiplier.
    pub fn backoff_factor(mut self, f: f64) -> Self {
        self.backoff_factor = f;
        self
    }

    /// Set the initial retry delay.
    pub fn base_interval(mut self, d: Duration) -> Self {
        self.base_interval = d;
        self
    }

    /// Set the maximum retry delay.
    pub fn max_interval(mut self, d: Duration) -> Self {
        self.max_interval = d;
        self
    }
}

/// The identity a workflow runs under: the user it was started on behalf of,
/// the role assumed for this run, and the full set of roles available to that
/// user. It is persisted with the workflow and flows into any work the workflow
/// starts, so an audit trail and authorization decisions stay consistent across
/// a workflow tree and across recovery.
///
/// All fields are optional — a workflow started without an identity carries an
/// empty `AuthContext`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthContext {
    /// User on whose behalf the workflow was started.
    pub authenticated_user: Option<String>,
    /// Role assumed for this run.
    pub assumed_role: Option<String>,
    /// Roles available to the authenticated user.
    pub authenticated_roles: Vec<String>,
}

impl AuthContext {
    /// Lift the identity recorded on a persisted workflow row.
    pub(crate) fn from_status(s: &WorkflowStatus) -> Self {
        Self {
            authenticated_user: s.authenticated_user.clone(),
            assumed_role: s.assumed_role.clone(),
            authenticated_roles: s.authenticated_roles.clone(),
        }
    }

    /// `true` when no identity was attached.
    pub fn is_empty(&self) -> bool {
        self.authenticated_user.is_none()
            && self.assumed_role.is_none()
            && self.authenticated_roles.is_empty()
    }
}

/// Handle passed into every workflow function. It carries the workflow id, the
/// state backend, the identity the workflow runs under, and a deterministic
/// per-execution step counter.
///
/// All durable operations a workflow performs go through this context:
/// [`DurableContext::step`] / [`DurableContext::step_with`] for checkpointed work
/// and [`DurableContext::sleep`] for durable timers.
#[derive(Clone)]
pub struct DurableContext {
    workflow_id: String,
    provider: Arc<dyn StateProvider>,
    /// Shared execution core, so a workflow can start child workflows.
    runtime: Arc<Runtime>,
    auth: AuthContext,
    // Monotonic step index. Because the workflow's control flow is
    // deterministic, the same code path yields the same seq on every replay,
    // which is how we match a step call to its stored checkpoint.
    seq: Arc<AtomicI32>,
}

impl DurableContext {
    pub(crate) fn new(workflow_id: String, runtime: Arc<Runtime>, auth: AuthContext) -> Self {
        Self {
            workflow_id,
            provider: runtime.provider().clone(),
            runtime,
            auth,
            seq: Arc::new(AtomicI32::new(0)),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// The identity this workflow runs under (see [`AuthContext`]).
    pub fn auth(&self) -> &AuthContext {
        &self.auth
    }

    /// The user this workflow was started on behalf of, if any.
    pub fn authenticated_user(&self) -> Option<&str> {
        self.auth.authenticated_user.as_deref()
    }

    /// The role assumed for this run, if any.
    pub fn assumed_role(&self) -> Option<&str> {
        self.auth.assumed_role.as_deref()
    }

    /// The roles available to the authenticated user.
    pub fn authenticated_roles(&self) -> &[String] {
        &self.auth.authenticated_roles
    }

    fn next_seq(&self) -> i32 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// The current step index — the `seq` the next durable operation will use,
    /// i.e. how many durable operations (steps, sleeps, sends, child workflows)
    /// this execution has performed so far. The Rust analog of Go's `GetStepID`.
    pub fn current_step_id(&self) -> i32 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Start a **child workflow** from within this workflow and return a handle
    /// to it. Await its result with [`WorkflowHandle::get_result`].
    ///
    /// The child runs durably and independently of the parent. It is keyed to
    /// this call's step position: unless `opts.workflow_id` is set, it gets the
    /// deterministic id `{parent_id}-{seq}`, and the parent→child link is
    /// checkpointed. On replay the same child is re-attached instead of being
    /// started again, so the child runs at most once per logical call.
    ///
    /// The child inherits this workflow's identity ([`AuthContext`]) and records
    /// its `parent_workflow_id`. Pass `opts.queue` to route the child through a
    /// queue instead of running it inline.
    pub async fn start_workflow<I, O>(
        &self,
        name: &str,
        input: I,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        let seq = self.next_seq();

        // Replay: re-attach to the child already started at this step.
        if let Some(child_id) = self
            .provider
            .check_child_workflow(&self.workflow_id, seq)
            .await?
        {
            return Ok(WorkflowHandle::polling(child_id, self.provider.clone()));
        }

        let child_id = opts
            .workflow_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", self.workflow_id, seq));
        let mut opts = opts;
        opts.workflow_id = Some(child_id.clone());
        let input_json = serde_json::to_value(input)?;

        self.runtime
            .spawn_child(
                &child_id,
                name,
                input_json,
                opts,
                &self.workflow_id,
                self.auth.clone(),
            )
            .await?;
        self.provider
            .record_child_workflow(&self.workflow_id, seq, name, &child_id)
            .await?;

        Ok(WorkflowHandle::polling(child_id, self.provider.clone()))
    }

    /// Run a durable step with the default policy (no retries).
    ///
    /// On the first execution, `f` runs and its result is checkpointed to the
    /// state backend. On any later replay (e.g. after a crash) the stored result
    /// is returned and `f` is **not** run again — so side effects inside `f`
    /// execute at most once per logical step under normal operation.
    ///
    /// `f` is `FnOnce`: it is invoked at most once per call. For automatic
    /// retries, use [`step_with`](Self::step_with).
    pub async fn step<T, F, Fut>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let seq = self.next_seq();
        if let Some(stored) = self.replay_or_guard::<T>(seq).await? {
            return Ok(stored);
        }
        let result = f().await?;
        self.checkpoint(seq, name, result).await
    }

    /// Run a durable step with an explicit retry [`StepOptions`] policy.
    ///
    /// If the closure errors, it is retried with exponential backoff up to
    /// `max_retries` times. Only the **final** outcome is checkpointed, so a
    /// replay never re-runs a step that previously succeeded. Before running a
    /// fresh (non-replayed) attempt, the workflow's status is checked: a
    /// `CANCELLED` workflow refuses to run the step and returns
    /// [`Error::Cancelled`].
    pub async fn step_with<T, F, Fut>(&self, opts: StepOptions, mut f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let seq = self.next_seq();
        if let Some(stored) = self.replay_or_guard::<T>(seq).await? {
            return Ok(stored);
        }
        // Run with retries; only the final result/error is observed.
        let result = self.run_with_retries(&opts, &mut f).await?;
        self.checkpoint(seq, &opts.name, result).await
    }

    /// Shared step preamble: serve a replayed checkpoint if present, otherwise
    /// refuse to start fresh work on a `CANCELLED` workflow. `Ok(Some(v))` means
    /// "return `v`"; `Ok(None)` means "proceed to run the closure".
    async fn replay_or_guard<T: DeserializeOwned>(&self, seq: i32) -> Result<Option<T>> {
        if let Some(stored) = self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            return Ok(Some(serde_json::from_value(stored)?));
        }
        if let Some(status) = self.provider.get_workflow_status(&self.workflow_id).await? {
            if status.status == STATUS_CANCELLED {
                return Err(Error::Cancelled(self.workflow_id.clone()));
            }
        }
        Ok(None)
    }

    /// Durably record `result` under `(workflow_id, seq)` and return the
    /// canonical stored value (a racing writer's value wins if there is one).
    async fn checkpoint<T: Serialize + DeserializeOwned>(
        &self,
        seq: i32,
        name: &str,
        result: T,
    ) -> Result<T> {
        let json = serde_json::to_value(&result)?;
        let canonical = self
            .provider
            .record_step_result(&self.workflow_id, seq, name, json)
            .await?;
        Ok(serde_json::from_value(canonical)?)
    }

    /// Drive `f` to success, retrying on error per `opts` with exponential
    /// backoff. Returns the last error if all attempts are exhausted.
    async fn run_with_retries<T, F, Fut>(&self, opts: &StepOptions, f: &mut F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if attempt >= opts.max_retries {
                        return Err(e);
                    }
                    let backoff =
                        opts.base_interval.as_secs_f64() * opts.backoff_factor.powi(attempt as i32);
                    let delay = Duration::from_secs_f64(backoff).min(opts.max_interval);
                    tracing::warn!(
                        step = %opts.name,
                        attempt = attempt + 1,
                        error = %e,
                        "step failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Durably sleep for `dur`.
    ///
    /// The absolute wake time is fixed and persisted on the first call as an
    /// ordinary `DBOS.sleep` step (the same way the Go SDK records it in
    /// `operation_outputs`), so the timer does not drift if the workflow crashes
    /// and is replayed: a replay reads the same wake instant and only waits the
    /// *remaining* time.
    pub async fn sleep(&self, dur: Duration) -> Result<()> {
        let seq = self.next_seq();
        let wake_at = self.durable_wake_at(seq, dur).await?;
        let now = chrono::Utc::now();
        if wake_at > now {
            let remaining = (wake_at - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining).await;
        }
        Ok(())
    }

    /// Resolve the absolute wake instant for a durable timer at `seq`: the
    /// first call records `now + dur` as a `DBOS.sleep` step; replays read the
    /// stored instant back, so timers (and recv/get_event timeouts built on
    /// them) never extend across crashes.
    async fn durable_wake_at(
        &self,
        seq: i32,
        dur: Duration,
    ) -> Result<chrono::DateTime<chrono::Utc>> {
        match self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            Some(stored) => Ok(serde_json::from_value(stored)?),
            None => {
                let proposed = chrono::Utc::now()
                    + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero());
                let canonical = self
                    .provider
                    .record_step_result(
                        &self.workflow_id,
                        seq,
                        "DBOS.sleep",
                        serde_json::to_value(proposed)?,
                    )
                    .await?;
                Ok(serde_json::from_value(canonical)?)
            }
        }
    }

    /// Durably send a message to another workflow on `topic` — the Rust analog
    /// of Go's `Send`. Recorded as a `DBOS.send` step, so a replay does not
    /// re-send. Errors if the destination workflow does not exist.
    ///
    /// Like any step side effect, the send commits before its checkpoint: a
    /// crash in that window re-sends on replay (at-least-once).
    pub async fn send<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
    ) -> Result<()> {
        let seq = self.next_seq();
        if let Some(_done) = self.replay_or_guard::<Value>(seq).await? {
            return Ok(());
        }
        self.provider
            .insert_notification(destination_id, topic, serde_json::to_value(message)?)
            .await?;
        self.provider
            .record_step_result(&self.workflow_id, seq, "DBOS.send", Value::Null)
            .await?;
        Ok(())
    }

    /// Receive the oldest unconsumed message sent to this workflow on `topic`,
    /// waiting up to `timeout` — the Rust analog of Go's `Recv`. Messages are
    /// consumed FIFO, exactly once: the claim and the step checkpoint commit
    /// atomically, and a replay returns the recorded message without consuming
    /// another. Returns `None` on timeout (also recorded, so a replay does not
    /// wait again). The timeout deadline itself is durable: a crash mid-wait
    /// resumes with the *remaining* time, not a fresh timeout.
    pub async fn recv<T: DeserializeOwned>(
        &self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let seq = self.next_seq();
        let deadline_seq = self.next_seq();

        if let Some(stored) = self.replay_or_guard::<Option<T>>(seq).await? {
            return Ok(stored);
        }

        let mut deadline: Option<chrono::DateTime<chrono::Utc>> = None;
        loop {
            if let Some(msg) = self
                .provider
                .consume_notification(&self.workflow_id, topic, seq, "DBOS.recv")
                .await?
            {
                return Ok(Some(serde_json::from_value(msg)?));
            }

            // Mailbox empty: fix the durable deadline (first miss only), then
            // poll until a message arrives or the deadline passes.
            let deadline = match deadline {
                Some(d) => d,
                None => *deadline.insert(self.durable_wake_at(deadline_seq, timeout).await?),
            };
            let now = chrono::Utc::now();
            if now >= deadline {
                self.provider
                    .record_step_result(&self.workflow_id, seq, "DBOS.recv", Value::Null)
                    .await?;
                return Ok(None);
            }
            let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining.min(NOTIFICATION_POLL_INTERVAL)).await;
        }
    }

    /// Publish (or overwrite) the value of event `key` on this workflow — the
    /// Rust analog of Go's `SetEvent`. Recorded as a `DBOS.setEvent` step;
    /// other workflows and external code read it with `get_event`.
    pub async fn set_event<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        let seq = self.next_seq();
        if let Some(_done) = self.replay_or_guard::<Value>(seq).await? {
            return Ok(());
        }
        self.provider
            .upsert_event(&self.workflow_id, key, serde_json::to_value(value)?)
            .await?;
        self.provider
            .record_step_result(&self.workflow_id, seq, "DBOS.setEvent", Value::Null)
            .await?;
        Ok(())
    }

    /// Read event `key` of another workflow, waiting up to `timeout` for it to
    /// be set — the Rust analog of Go's `GetEvent` inside a workflow. The value
    /// observed is recorded as a `DBOS.getEvent` step, so replays see the same
    /// value even if the event is overwritten later. Returns `None` on timeout.
    pub async fn get_event<T: DeserializeOwned>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let seq = self.next_seq();
        let deadline_seq = self.next_seq();

        if let Some(stored) = self.replay_or_guard::<Option<T>>(seq).await? {
            return Ok(stored);
        }

        let mut deadline: Option<chrono::DateTime<chrono::Utc>> = None;
        loop {
            if let Some(value) = self
                .provider
                .get_event_value(target_workflow_id, key)
                .await?
            {
                let canonical = self
                    .provider
                    .record_step_result(&self.workflow_id, seq, "DBOS.getEvent", value)
                    .await?;
                return Ok(Some(serde_json::from_value(canonical)?));
            }

            let deadline = match deadline {
                Some(d) => d,
                None => *deadline.insert(self.durable_wake_at(deadline_seq, timeout).await?),
            };
            let now = chrono::Utc::now();
            if now >= deadline {
                self.provider
                    .record_step_result(&self.workflow_id, seq, "DBOS.getEvent", Value::Null)
                    .await?;
                return Ok(None);
            }
            let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining.min(NOTIFICATION_POLL_INTERVAL)).await;
        }
    }

    /// Escape hatch for building application errors inside steps.
    pub fn err(&self, msg: impl Into<String>) -> Error {
        Error::app(msg)
    }
}

/// How often blocked `recv`/`get_event` calls re-check the database. (The Go
/// SDK avoids polling with Postgres LISTEN/NOTIFY; polling keeps this portable
/// across backends and is a future optimization point.)
const NOTIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(25);
