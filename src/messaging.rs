//! Messaging and events: how workflows talk to each other and to the outside
//! world.
//!
//! Two primitives, for two shapes of communication:
//!
//! | | Mailbox ([`send`] / [`recv`]) | Events ([`set_event`] / [`get_event`]) |
//! |---|---|---|
//! | Shape | FIFO queue of messages, per workflow, per topic | key → latest value, per workflow |
//! | Consumption | each message consumed **exactly once** | reads don't consume; last write wins |
//! | Direction | *into* a workflow | *out of* a workflow |
//! | Use it for | approvals, triggers, hand-offs between workflows | progress, status pages, results for pollers |
//!
//! Both are durable state in the workflow database — no broker to run — and
//! both wake blocked waiters promptly on Postgres (`LISTEN`/`NOTIFY`) while
//! falling back to polling elsewhere.
//!
//! The classic human-in-the-loop flow uses both — a workflow publishes its
//! status, blocks on a decision, and the outside world reads and nudges it:
//!
//! ```
//! use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[durare::workflow]
//! async fn approval(ctx: DurableContext, what: String) -> Result<String> {
//!     ctx.set_event("status", "waiting".to_string()).await?;
//!     let decision = ctx
//!         .recv::<String>("decision", Duration::from_secs(10))
//!         .await?
//!         .unwrap_or_else(|| "timed out".to_string());
//!     Ok(format!("{what}: {decision}"))
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! let handle = engine
//!     .start_with(Approval, "expense-42".to_string(), WorkflowOptions::with_id("appr-1"))
//!     .await?;
//!
//! // From outside — an API handler, a CLI: watch progress, then decide.
//! let status: Option<String> = engine.get_event("appr-1", "status", Duration::from_secs(5)).await?;
//! assert_eq!(status.as_deref(), Some("waiting"));
//! engine.send("appr-1", "approved".to_string(), "decision").await?;
//!
//! assert_eq!(handle.await?, "expense-42: approved");
//! # Ok(())
//! # }
//! ```
//!
//! # Delivery semantics
//!
//! Each verb is a durable operation, and each end has a precise guarantee:
//!
//! - **[`recv`] consumes exactly once.** Claiming the message and writing the
//!   step checkpoint commit atomically, and a replay returns the recorded
//!   message instead of consuming another.
//! - **[`send`] from a workflow delivers at least once.** The insert commits
//!   before the send's checkpoint, so a crash in that window re-sends on
//!   replay; the receiver's exactly-once `recv` is unaffected (it consumes
//!   whichever copy it claims, once). A replayed `send` step never re-sends.
//! - **Senders outside a workflow** ([`DurableEngine::send`],
//!   [`Client::send`](crate::Client::send)) have no checkpoint to lean on. A
//!   retrying producer should use
//!   [`send_with_idempotency_key`](DurableEngine::send_with_idempotency_key) —
//!   at most one delivery per `(key, destination)`, however often it retries.
//! - **[`set_event`] is last-write-wins**, and [`get_event`] records the value
//!   it observed, so a replay sees the same value even if the event was
//!   overwritten later.
//!
//! # Timeouts are durable
//!
//! [`recv`] and [`get_event`] take a timeout, and the **deadline itself is
//! checkpointed**: a workflow that crashes mid-wait resumes waiting for the
//! *remaining* time, not a fresh window. A timeout is a normal outcome —
//! `Ok(None)` — not an error, and it is recorded too, so a replay does not
//! wait again.
//!
//! # Topics and keys
//!
//! Messages are addressed `(destination workflow, topic)` — a workflow can
//! serve several independent mailboxes, FIFO within each topic. Events are
//! addressed `(workflow, key)` — one latest value per key. Neither namespace
//! collides across workflows.
//!
//! For high-volume or ordered fan-*out* — progress feeds, logs, incremental
//! results — use [streams](DurableContext::write_stream) instead: append-only,
//! offset-ordered, and tailable from outside with
//! [`DurableEngine::read_stream_values`].
//!
//! [`send`]: DurableContext::send
//! [`recv`]: DurableContext::recv
//! [`set_event`]: DurableContext::set_event
//! [`get_event`]: DurableContext::get_event

#[allow(unused_imports)]
use crate::{DurableContext, DurableEngine};
