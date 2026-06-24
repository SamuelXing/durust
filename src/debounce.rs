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

use crate::context::DurableContext;
use crate::engine::{DurableEngine, WorkflowOptions, INTERNAL_QUEUE};
use crate::error::{Error, ErrorCode, Result};
use crate::handle::WorkflowHandle;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// Registered name of the internal debouncer workflow (auto-registered by every
/// engine, like the internal queue).
pub(crate) const DEBOUNCER_WF: &str = "_dbos_debouncer";
/// Topic the collector receives pushed-back inputs on.
const DEBOUNCER_TOPIC: &str = "_dbos_debouncer_topic";
/// How long a producer waits for the collector to ACK a pushed input before it
/// assumes the collector already finished and starts a fresh one.
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Input to the internal debouncer workflow. Carries the target it will start
/// and the first input; later inputs arrive as [`DebounceMessage`]s.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DebouncerInput {
    target_name: String,
    target_id: String,
    delay_ms: i64,
    timeout_ms: i64,
    initial_input: Value,
}

/// A pushed-back input sent to a running collector.
#[derive(Serialize, Deserialize)]
struct DebounceMessage {
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

    // Start the target once, under the id every producer was handed.
    ctx.start_workflow::<Value, Value>(
        &input.target_name,
        current_input,
        WorkflowOptions::with_id(&input.target_id),
    )
    .await?;
    Ok(())
}

/// Debounces a target workflow. Create one with
/// [`DurableEngine::debouncer`](crate::DurableEngine::debouncer).
pub struct Debouncer<'e> {
    engine: &'e DurableEngine,
    target: String,
    timeout: Duration,
}

impl<'e> Debouncer<'e> {
    pub(crate) fn new(engine: &'e DurableEngine, target: impl Into<String>) -> Self {
        Debouncer {
            engine,
            target: target.into(),
            timeout: Duration::ZERO,
        }
    }

    /// Cap how far the start time can be pushed back from the first call.
    /// `Duration::ZERO` (the default) means no cap.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
        let delay_ms = delay.as_millis() as i64;
        let timeout_ms = self.timeout.as_millis() as i64;
        let provider = self.engine.provider();

        loop {
            let target_id = uuid::Uuid::new_v4().to_string();
            let dinput = DebouncerInput {
                target_name: self.target.clone(),
                target_id: target_id.clone(),
                delay_ms,
                timeout_ms,
                initial_input: input_val.clone(),
            };
            // First call for this key wins the dedup slot and starts the collector.
            match self
                .engine
                .enqueue::<_, ()>(
                    INTERNAL_QUEUE,
                    DEBOUNCER_WF,
                    dinput,
                    WorkflowOptions::default().dedup_id(key),
                )
                .await
            {
                Ok(_) => return Ok(WorkflowHandle::polling(target_id, provider.clone())),
                Err(e) if e.code() == ErrorCode::QueueDeduplicated => {
                    // A collector for this key is already running: push it the
                    // new input instead of starting another.
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
                    self.engine
                        .send(
                            &existing,
                            DebounceMessage {
                                input: input_val.clone(),
                                delay_ms,
                                id: message_id.clone(),
                            },
                            DEBOUNCER_TOPIC,
                        )
                        .await?;
                    // Wait for the collector to ACK; if it already finished,
                    // start a fresh one on the next loop.
                    let ack: Option<bool> = self
                        .engine
                        .get_event(&existing, &message_id, ACK_TIMEOUT)
                        .await?;
                    if ack.is_none() {
                        continue;
                    }
                    return Ok(WorkflowHandle::polling(
                        existing_input.target_id,
                        provider.clone(),
                    ));
                }
                Err(e) => return Err(e),
            }
        }
    }
}
