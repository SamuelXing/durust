use crate::error::{Error, Result};
use crate::provider::{StateProvider, STATUS_CANCELLED};
use serde::{de::DeserializeOwned, Serialize};
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

/// Handle passed into every workflow function. It carries the workflow id, the
/// state backend, and a deterministic per-execution step counter.
///
/// All durable operations a workflow performs go through this context:
/// [`DurableContext::step`] / [`DurableContext::step_with`] for checkpointed work
/// and [`DurableContext::sleep`] for durable timers.
#[derive(Clone)]
pub struct DurableContext {
    workflow_id: String,
    provider: Arc<dyn StateProvider>,
    // Monotonic step index. Because the workflow's control flow is
    // deterministic, the same code path yields the same seq on every replay,
    // which is how we match a step call to its stored checkpoint.
    seq: Arc<AtomicI32>,
}

impl DurableContext {
    pub(crate) fn new(workflow_id: String, provider: Arc<dyn StateProvider>) -> Self {
        Self {
            workflow_id,
            provider,
            seq: Arc::new(AtomicI32::new(0)),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    fn next_seq(&self) -> i32 {
        self.seq.fetch_add(1, Ordering::SeqCst)
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
        if let Some(stored) = self.provider.get_step_result(&self.workflow_id, seq).await? {
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
                    let backoff = opts.base_interval.as_secs_f64()
                        * opts.backoff_factor.powi(attempt as i32);
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

        // First call fixes the wake instant; replays read the stored one.
        let wake_at: chrono::DateTime<chrono::Utc> =
            match self.provider.get_step_result(&self.workflow_id, seq).await? {
                Some(stored) => serde_json::from_value(stored)?,
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
                    serde_json::from_value(canonical)?
                }
            };

        let now = chrono::Utc::now();
        if wake_at > now {
            let remaining = (wake_at - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining).await;
        }
        Ok(())
    }

    /// Escape hatch for building application errors inside steps.
    pub fn err(&self, msg: impl Into<String>) -> Error {
        Error::app(msg)
    }
}
