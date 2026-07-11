//! Workflow **debouncing**: coalesce rapid repeated triggers of a workflow into
//! a single delayed run.
//!
//! Each [`Debouncer::debounce`] call (re)schedules the target workflow `delay`
//! into the future and replaces its input with the latest one. As long as new
//! calls keep arriving within `delay` of each other, the run keeps getting
//! pushed back — up to an optional `timeout` from the first call — and only the
//! final input runs. Calls are grouped by `key`, so independent groups debounce
//! independently.
//!
//! It is built entirely on existing durable primitives: one internal workflow
//! per key (kept unique by a queue **deduplication id** equal to the key) that
//! collects pushed-back inputs over [`recv`](crate::DurableContext::recv) and
//! finally starts the target once. Producers nudge a running collector with
//! [`send`](crate::DurableEngine::send) + an event ACK.

use crate::client::Client;
use crate::context::DurableContext;
use crate::engine::{DurableEngine, WorkflowOptions, INTERNAL_QUEUE};
use crate::error::{Error, ErrorCode, Result};
use crate::handle::WorkflowHandle;
use crate::provider::StateProvider;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// Registered name of the internal debouncer workflow (auto-registered by every
/// engine, like the internal queue).
pub(crate) const DEBOUNCER_WF: &str = "_dbos_debouncer";
/// Topic the collector receives pushed-back inputs on.
const DEBOUNCER_TOPIC: &str = "_dbos_debouncer_topic";
/// How long a producer waits for the collector to ACK a pushed input before it
/// assumes the collector already finished and starts a fresh one.
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// The subset of [`WorkflowOptions`] carried to the collector so the debounced
/// target runs with the caller's queue / priority / auth / version — not just
/// its id. Serializable (unlike `WorkflowOptions`, which holds `Duration`s and a
/// dedup policy that are meaningless for the target); the debouncer owns the
/// target id, dedup, and delay, so those are intentionally not threaded.
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct TargetOptions {
    queue: Option<String>,
    priority: i32,
    partition_key: Option<String>,
    app_version: Option<String>,
    timeout_ms: Option<i64>,
    authenticated_user: Option<String>,
    assumed_role: Option<String>,
    authenticated_roles: Vec<String>,
}

impl TargetOptions {
    fn from_options(o: &WorkflowOptions) -> Self {
        Self {
            queue: o.queue.clone(),
            priority: o.priority,
            partition_key: o.partition_key.clone(),
            app_version: o.app_version.clone(),
            timeout_ms: o.timeout.map(|d| d.as_millis() as i64),
            authenticated_user: o.authenticated_user.clone(),
            assumed_role: o.assumed_role.clone(),
            authenticated_roles: o.authenticated_roles.clone(),
        }
    }

    /// Rebuild a [`WorkflowOptions`] for the target, pinned to `target_id`.
    fn into_options(self, target_id: &str) -> WorkflowOptions {
        let mut opts = WorkflowOptions::with_id(target_id);
        opts.queue = self.queue;
        opts.priority = self.priority;
        opts.partition_key = self.partition_key;
        opts.app_version = self.app_version;
        opts.timeout = self.timeout_ms.map(|ms| Duration::from_millis(ms as u64));
        opts.authenticated_user = self.authenticated_user;
        opts.assumed_role = self.assumed_role;
        opts.authenticated_roles = self.authenticated_roles;
        opts
    }
}

/// Input to the internal debouncer workflow. Carries the target it will start
/// and the first input; later inputs arrive as [`DebounceMessage`]s.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DebouncerInput {
    target_name: String,
    target_id: String,
    delay_ms: i64,
    timeout_ms: i64,
    initial_input: Value,
    #[serde(default)]
    target_opts: TargetOptions,
}

/// A pushed-back input sent to a running collector.
#[derive(Serialize, Deserialize)]
pub(crate) struct DebounceMessage {
    input: Value,
    delay_ms: i64,
    /// ACK id: the collector sets an event under this key once it takes the
    /// message, so the producer knows it landed.
    id: String,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// The internal debouncer workflow: collect pushed-back inputs until the delay
/// elapses with no new input, then start the target once with the latest input.
pub(crate) async fn internal_debouncer(ctx: DurableContext, input: DebouncerInput) -> Result<()> {
    let start = ctx
        .step("DBOS.debounce.startTime", || async {
            Ok::<_, Error>(now_ms())
        })
        .await?;
    let mut current_input = input.initial_input.clone();
    let max_ms = start + input.timeout_ms;
    let cap = |t: i64| {
        if input.timeout_ms > 0 && t > max_ms {
            max_ms
        } else {
            t
        }
    };
    let mut target_ms = cap(start + input.delay_ms);

    loop {
        let now = ctx
            .step("DBOS.debounce.loopTime", || async {
                Ok::<_, Error>(now_ms())
            })
            .await?;
        let remaining = target_ms - now;
        if remaining <= 0 {
            break;
        }
        // Wait for a new input up to the remaining time. A timeout (`None`)
        // means no one pushed it back further, so run with what we have.
        let Some(msg): Option<DebounceMessage> = ctx
            .recv(DEBOUNCER_TOPIC, Duration::from_millis(remaining as u64))
            .await?
        else {
            break;
        };
        current_input = msg.input;
        target_ms = cap(now + msg.delay_ms);
        if !msg.id.is_empty() {
            ctx.set_event(&msg.id, true).await?;
        }
    }

    // Start the target once, under the id every producer was handed and with
    // the caller's queue / priority / auth / version threaded through.
    let opts = input.target_opts.into_options(&input.target_id);
    ctx.start_workflow::<Value, Value>(&input.target_name, current_input, opts)
        .await?;
    Ok(())
}

/// The producer-side operations the debounce loop needs. Implemented by both
/// [`DurableEngine`] (in-process) and [`Client`] (out-of-process): debouncing is
/// built entirely from enqueue / send / get_event / provider queries — it never
/// runs the collector itself (a launched engine does that), so an out-of-process
/// producer can debounce just as well as an in-process one.
#[allow(async_fn_in_trait)] // crate-internal, only ever used with a concrete backend
pub(crate) trait DebounceBackend {
    fn provider(&self) -> &Arc<dyn StateProvider>;
    /// Enqueue the collector on the internal queue, deduplicated by `key`.
    async fn enqueue_debouncer(&self, input: DebouncerInput, key: &str) -> Result<()>;
    /// Push a new input to a running collector.
    async fn send_debounce(&self, destination: &str, msg: DebounceMessage) -> Result<()>;
    /// Wait for the collector's ACK that it took the pushed input.
    async fn get_ack(&self, workflow_id: &str, ack_id: &str) -> Result<Option<bool>>;
}

impl DebounceBackend for DurableEngine {
    fn provider(&self) -> &Arc<dyn StateProvider> {
        DurableEngine::provider(self)
    }
    async fn enqueue_debouncer(&self, input: DebouncerInput, key: &str) -> Result<()> {
        self.start::<_, ()>(
            DEBOUNCER_WF,
            input,
            WorkflowOptions::default()
                .dedup_id(key)
                .queue(INTERNAL_QUEUE),
        )
        .await
        .map(|_| ())
    }
    async fn send_debounce(&self, destination: &str, msg: DebounceMessage) -> Result<()> {
        self.send(destination, msg, DEBOUNCER_TOPIC).await
    }
    async fn get_ack(&self, workflow_id: &str, ack_id: &str) -> Result<Option<bool>> {
        self.get_event::<bool>(workflow_id, ack_id, ACK_TIMEOUT)
            .await
    }
}

impl DebounceBackend for Client {
    fn provider(&self) -> &Arc<dyn StateProvider> {
        Client::provider(self)
    }
    async fn enqueue_debouncer(&self, input: DebouncerInput, key: &str) -> Result<()> {
        self.enqueue::<_, ()>(
            INTERNAL_QUEUE,
            DEBOUNCER_WF,
            input,
            WorkflowOptions::default().dedup_id(key),
        )
        .await
        .map(|_| ())
    }
    async fn send_debounce(&self, destination: &str, msg: DebounceMessage) -> Result<()> {
        self.send(destination, msg, DEBOUNCER_TOPIC).await
    }
    async fn get_ack(&self, workflow_id: &str, ack_id: &str) -> Result<Option<bool>> {
        self.get_event::<bool>(workflow_id, ack_id, ACK_TIMEOUT)
            .await
    }
}

/// Schedule (or push back) the target's run for `key`, over any
/// [`DebounceBackend`]. Returns the target workflow id every producer for this
/// active `key` is handed (they all point at the same eventual run).
pub(crate) async fn run_debounce<B: DebounceBackend>(
    backend: &B,
    target: &str,
    timeout: Duration,
    target_opts: &TargetOptions,
    key: &str,
    delay: Duration,
    input_val: Value,
) -> Result<String> {
    let delay_ms = delay.as_millis() as i64;
    let timeout_ms = timeout.as_millis() as i64;
    let provider = backend.provider();

    loop {
        let target_id = uuid::Uuid::new_v4().to_string();
        let dinput = DebouncerInput {
            target_name: target.to_string(),
            target_id: target_id.clone(),
            delay_ms,
            timeout_ms,
            initial_input: input_val.clone(),
            target_opts: target_opts.clone(),
        };
        // First call for this key wins the dedup slot and starts the collector.
        match backend.enqueue_debouncer(dinput, key).await {
            Ok(()) => return Ok(target_id),
            Err(e) if e.code() == ErrorCode::QueueDeduplicated => {
                // A collector for this key is already running: push it the new
                // input instead of starting another.
                let Some(existing) = provider
                    .get_deduplicated_workflow(INTERNAL_QUEUE, key)
                    .await?
                else {
                    continue; // it finished between our enqueue and lookup; retry
                };
                let Some(status) = provider.get_workflow_status(&existing).await? else {
                    continue;
                };
                let existing_input: DebouncerInput = serde_json::from_value(status.input)?;
                let message_id = uuid::Uuid::new_v4().to_string();
                backend
                    .send_debounce(
                        &existing,
                        DebounceMessage {
                            input: input_val.clone(),
                            delay_ms,
                            id: message_id.clone(),
                        },
                    )
                    .await?;
                // Wait for the collector to ACK; if it already finished, start a
                // fresh one on the next loop.
                if backend.get_ack(&existing, &message_id).await?.is_none() {
                    continue;
                }
                return Ok(existing_input.target_id);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Debounces a target workflow. Create one with
/// [`DurableEngine::debouncer`](crate::DurableEngine::debouncer).
pub struct Debouncer<'e> {
    engine: &'e DurableEngine,
    target: String,
    timeout: Duration,
    target_opts: TargetOptions,
}

impl<'e> Debouncer<'e> {
    pub(crate) fn new(engine: &'e DurableEngine, target: impl Into<String>) -> Self {
        Debouncer {
            engine,
            target: target.into(),
            timeout: Duration::ZERO,
            target_opts: TargetOptions::default(),
        }
    }

    /// Cap how far the start time can be pushed back from the first call.
    /// `Duration::ZERO` (the default) means no cap.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Run the debounced target with these [`WorkflowOptions`] — its queue,
    /// priority, partition key, application version, timeout, and authenticated
    /// identity. The target's id is owned by the debouncer (set per `key`), so
    /// `workflow_id`/`dedup_id`/`delay` on `opts` are ignored.
    pub fn options(mut self, opts: WorkflowOptions) -> Self {
        self.target_opts = TargetOptions::from_options(&opts);
        self
    }

    /// Schedule (or push back) the target's run for `key`, with the latest
    /// `input`. Returns a handle to the single eventual run; every call for the
    /// same active `key` returns a handle to the *same* run.
    pub async fn debounce<I, O>(
        &self,
        key: &str,
        delay: Duration,
        input: I,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input_val = serde_json::to_value(input)?;
        let target_id = run_debounce(
            self.engine,
            &self.target,
            self.timeout,
            &self.target_opts,
            key,
            delay,
            input_val,
        )
        .await?;
        Ok(WorkflowHandle::polling(
            target_id,
            self.engine.provider().clone(),
        ))
    }
}

/// Debounces a target workflow from an out-of-process [`Client`]. Create one with
/// [`Client::debouncer`]. Identical to [`Debouncer`] except the producer runs
/// outside any engine — a launched engine (which auto-registers the internal
/// debouncer and the target) still executes the coalesced run.
pub struct DebouncerClient<'c> {
    client: &'c Client,
    target: String,
    timeout: Duration,
    target_opts: TargetOptions,
}

impl<'c> DebouncerClient<'c> {
    pub(crate) fn new(client: &'c Client, target: impl Into<String>) -> Self {
        DebouncerClient {
            client,
            target: target.into(),
            timeout: Duration::ZERO,
            target_opts: TargetOptions::default(),
        }
    }

    /// Cap how far the start time can be pushed back from the first call.
    /// `Duration::ZERO` (the default) means no cap.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Run the debounced target with these [`WorkflowOptions`] — its queue,
    /// priority, partition key, application version, timeout, and authenticated
    /// identity. The target's id is owned by the debouncer (set per `key`), so
    /// `workflow_id`/`dedup_id`/`delay` on `opts` are ignored.
    pub fn options(mut self, opts: WorkflowOptions) -> Self {
        self.target_opts = TargetOptions::from_options(&opts);
        self
    }

    /// Schedule (or push back) the target's run for `key`, with the latest
    /// `input`. Returns a handle to the single eventual run; every call for the
    /// same active `key` returns a handle to the *same* run.
    ///
    /// Requires a live engine running the collector: with none (or an app-version
    /// mismatch), a call that coalesces into an existing debouncer blocks —
    /// retrying until an engine appears — rather than erroring. Matches Go's
    /// client debouncer.
    pub async fn debounce<I, O>(
        &self,
        key: &str,
        delay: Duration,
        input: I,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input_val = serde_json::to_value(input)?;
        let target_id = run_debounce(
            self.client,
            &self.target,
            self.timeout,
            &self.target_opts,
            key,
            delay,
            input_val,
        )
        .await?;
        Ok(WorkflowHandle::polling(
            target_id,
            self.client.provider().clone(),
        ))
    }
}
