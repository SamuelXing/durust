use crate::error::{Error, Result};
use crate::provider::{is_terminal, StateProvider, WorkflowStatus, STATUS_CANCELLED, STATUS_ERROR};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

/// A reference to a workflow execution.
///
/// Returned by [`start`](crate::DurableEngine::start),
/// [`start_with`](crate::DurableEngine::start_with), and `retrieve_workflow`. It
/// lets the caller observe the workflow's result without blocking the call that
/// started it.
///
/// Await it directly for the typed output, or share it:
///
/// ```no_run
/// # use durust::{DurableEngine, WorkflowHandle, Result};
/// # async fn f(engine: &DurableEngine) -> Result<()> {
/// let handle: WorkflowHandle<i64> = engine.start("add", 1_i64, Default::default()).await?;
/// let observer = handle.clone();          // hand a copy to another task
/// let total: i64 = handle.await?;         // or `handle.result().await?`
/// # let _ = observer;
/// # Ok(())
/// # }
/// ```
///
/// Two flavors, transparent to the caller:
/// - **Local**: started on this process, so the handle holds the task's
///   [`JoinHandle`] and awaits it directly (lowest latency, and a panicking
///   task surfaces as an error rather than hanging).
/// - **Polling**: obtained for a workflow running elsewhere (or already
///   persisted); the result is observed by polling the status row until it
///   reaches a terminal state. This is how DBOS handles cross-process /
///   post-restart waits.
///
/// [`clone`](Clone::clone) is cheap and yields another handle to the *same*
/// workflow. The in-process task can be awaited only once, so whichever clone
/// resolves first consumes it and the rest fall back to polling the persisted
/// status. Every normal outcome — success, error, cancellation, timeout — is
/// written to that status before the task returns, so all clones observe the
/// same result. A *panicking* workflow is the exception: the clone that owns
/// the task surfaces the panic as an error, while the others poll a status the
/// panic never wrote and keep waiting until the run is recovered. (Idiomatic
/// failures return `Err`, which is terminal, so this affects only real panics.)
pub struct WorkflowHandle<O> {
    id: String,
    provider: Arc<dyn StateProvider>,
    // Shared so a handle stays `Clone`. The local task can be awaited only once;
    // the first resolver `take`s it and the rest poll the persisted status.
    join: Arc<Mutex<Option<JoinHandle<Result<Value>>>>>,
    poll_interval: Duration,
    // `fn() -> O` keeps the handle `Send + Sync` regardless of `O` (it never
    // holds an `O` — the output is deserialized on demand).
    _marker: PhantomData<fn() -> O>,
}

impl<O> Clone for WorkflowHandle<O> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            provider: self.provider.clone(),
            join: self.join.clone(),
            poll_interval: self.poll_interval,
            _marker: PhantomData,
        }
    }
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
            join: Arc::new(Mutex::new(Some(join))),
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
            join: Arc::new(Mutex::new(None)),
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
    /// For a local handle this awaits the in-process task the first time it is
    /// called; for a polling handle (or a clone whose sibling already consumed
    /// the task) it reads the status row every `poll_interval` until the
    /// workflow reaches a terminal state, then deserializes the stored output.
    /// A `CANCELLED` workflow yields [`Error::Cancelled`]; an `ERROR` workflow
    /// yields the recorded application error.
    pub async fn result(&self) -> Result<O> {
        // Claim the in-process task exactly once. The guard is a temporary of
        // this statement, so it is dropped here — never held across the await.
        let owned = self
            .join
            .lock()
            .expect("workflow handle mutex poisoned")
            .take();
        if let Some(join) = owned {
            // Own the task: await it directly, surfacing panics as errors.
            return match join.await {
                Ok(res) => {
                    let value = res?;
                    Ok(serde_json::from_value(value)?)
                }
                Err(e) => Err(Error::app(format!("workflow task failed: {e}"))),
            };
        }

        // No task to await: poll the status row until terminal. A polling handle
        // may name a workflow that does not exist yet (e.g. a debounced run the
        // collector has not started); treat that as "not ready" and keep waiting.
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
            // A workflow that failed under portable mode carries a structured
            // error; reconstruct it so an observer reads the same name/code/data
            // any SDK wrote. Otherwise the bare message.
            STATUS_ERROR => Err(match status.error_info {
                Some(info) => Error::Portable(info),
                None => Error::app(
                    status
                        .error
                        .unwrap_or_else(|| "workflow failed".to_string()),
                ),
            }),
            _ => {
                let output = status.output.unwrap_or(Value::Null);
                Ok(serde_json::from_value(output)?)
            }
        }
    }
}

/// `handle.await` resolves to the workflow's typed output — sugar for
/// [`result`](WorkflowHandle::result) that consumes the handle.
impl<O: DeserializeOwned + 'static> IntoFuture for WorkflowHandle<O> {
    type Output = Result<O>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<O>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.result().await })
    }
}
