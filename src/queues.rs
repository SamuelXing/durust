//! Durable queues: decouple *submitting* work from *running* it, with
//! fleet-wide control over parallelism.
//!
//! Starting a workflow with [`opts.queue`](crate::WorkflowOptions::queue) does
//! not run it — it persists the workflow `ENQUEUED`. A **dispatcher** (one per
//! queue, started by [`DurableEngine::launch`]) claims enqueued workflows and
//! runs them, subject to the queue's limits. Because the queue lives in the
//! database, the producer and the executors can be different processes: an API
//! server [enqueues through a `Client`](crate::Client::enqueue) with no
//! workflow code at all, and any executor fleet picks the work up.
//!
//! ```
//! use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions, WorkflowQueue};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[durare::workflow]
//! async fn convert(ctx: DurableContext, file: String) -> Result<String> {
//!     ctx.step("transcode", || async move { Ok::<_, durare::Error>(format!("{file}.mp4")) })
//!         .await
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! engine.register_queue(
//!     WorkflowQueue::new("media")
//!         .worker_concurrency(2) // at most 2 run in this process at once
//!         .base_polling_interval(Duration::from_millis(20)),
//! );
//! engine.launch().await?; // start the dispatcher
//!
//! let mut handles = Vec::new();
//! for f in ["intro", "demo", "outro"] {
//!     let opts = WorkflowOptions::with_id(format!("convert-{f}")).queue("media");
//!     handles.push(engine.start::<_, String>("convert", f.to_string(), opts).await?);
//! }
//! for h in handles {
//!     assert!(h.result().await?.ends_with(".mp4"));
//! }
//! engine.shutdown(Duration::from_secs(1)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # The claim model
//!
//! Each dispatcher polls its queue (interval starting at
//! [`base_polling_interval`](WorkflowQueue::base_polling_interval), backing
//! off toward [`max_polling_interval`](WorkflowQueue::max_polling_interval) on
//! errors) and claims a batch of due workflows. On Postgres the claim uses
//! `FOR UPDATE SKIP LOCKED`, so any number of executors can poll the same
//! queue without contending or double-claiming — work is load-balanced across
//! the fleet by construction. A claimed workflow transitions to `PENDING` and
//! runs; enqueueing with [`opts.delay`](crate::WorkflowOptions::delay) parks
//! it `DELAYED` until due.
//!
//! # Concurrency budgets
//!
//! Three independent throttles, checked in order at claim time:
//!
//! | Knob | Scope | Enforced by |
//! |---|---|---|
//! | [`worker_concurrency`](WorkflowQueue::worker_concurrency) | this process | local running count |
//! | [`global_concurrency`](WorkflowQueue::global_concurrency) | all executors | `PENDING` count in the database |
//! | [`rate_limiter`](WorkflowQueue::rate_limiter) | all executors | starts within a trailing window |
//!
//! A [`RateLimiter`] caps *starts per period* rather than concurrent runs —
//! `limit: 100, period: 60s` admits at most 100 workflow starts in any
//! trailing 60-second window, however long each runs.
//!
//! # Priority
//!
//! On a [`priority_enabled`](WorkflowQueue::priority_enabled) queue, claims go
//! lowest-[`priority`](crate::WorkflowOptions::priority)-value first (and FIFO
//! within a value). Without the flag, queues are FIFO by creation time.
//!
//! # Deduplication
//!
//! Give an enqueue a [`dedup_id`](crate::WorkflowOptions::dedup_id) and the
//! queue admits **at most one active workflow per id**: a second enqueue under
//! the same id is rejected with [`Error::QueueDeduplicated`](crate::Error) —
//! or, under
//! [`DeduplicationPolicy::ReturnExisting`](crate::DeduplicationPolicy),
//! returns a handle to the workflow already holding the slot:
//!
//! ```no_run
//! # use durare::{DeduplicationPolicy, DurableEngine, Result, WorkflowOptions};
//! # async fn demo(engine: &DurableEngine) -> Result<()> {
//! let opts = WorkflowOptions::default()
//!     .queue("emails")
//!     .dedup_id("welcome-user-17")
//!     .dedup_policy(DeduplicationPolicy::ReturnExisting);
//! // Both calls yield a handle to the same run.
//! let a = engine.start::<_, ()>("send_welcome", 17_u64, opts.clone()).await?;
//! let b = engine.start::<_, ()>("send_welcome", 17_u64, opts).await?;
//! assert_eq!(a.id(), b.id());
//! # Ok(())
//! # }
//! ```
//!
//! The slot frees as soon as the holder reaches a terminal state, so the same
//! id can be enqueued again afterwards.
//!
//! # Partitioned queues
//!
//! A [`partitioned`](WorkflowQueue::partitioned) queue scopes every budget
//! above to a
//! [`partition_key`](crate::WorkflowOptions::partition_key) — `worker_concurrency(1)`
//! then means *one at a time per key* (say, per customer), not one per queue.
//! Keyless enqueues to a partitioned queue are never dispatched.
//!
//! # Crash behavior
//!
//! Queues need no special crash handling: an `ENQUEUED` row simply survives
//! and is claimed later. A workflow that was already `PENDING` on the crashed
//! executor is re-dispatched by [`DurableEngine::recover`] and replays from
//! its checkpoints — see the [durability guide](crate::durability).

#[doc(no_inline)]
pub use crate::{RateLimiter, WorkflowQueue};

#[allow(unused_imports)]
use crate::DurableEngine;
