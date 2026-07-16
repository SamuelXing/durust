//! # durare
//!
//! A [DBOS](https://docs.dbos.dev)-style **durable execution** library for
//! Rust.
//!
//! Write ordinary async code; wrap each side-effecting unit in a step. Every
//! step's result is checkpointed to a [`StateProvider`] (Postgres, SQLite, or
//! in-memory). If the process crashes or restarts, call
//! [`DurableEngine::recover`] and every unfinished workflow resumes exactly
//! where it stopped — completed steps are served from their checkpoints instead
//! of re-running.
//!
//! There is **no separate server**: the engine is a library that runs inside
//! your worker and talks directly to the database, using the same `dbos` system
//! schema as the DBOS Transact SDKs for Python, Go, and TypeScript — the SDKs
//! interoperate on one database.
//!
//! The problem this solves: any multi-step operation has crash windows — after
//! the card is charged but before the receipt is sent, the process dies, and a
//! naive retry charges twice. In `durare` the workflow id is an idempotency key
//! and every step is checkpointed, so a duplicate trigger (a retried webhook, a
//! double-click, a crashed-and-rerun caller) attaches to the same run instead
//! of repeating its effects. This example is a test — the assertions at the
//! bottom hold on every commit:
//!
//! ```
//! use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicU32, Ordering};
//!
//! /// Stand-in for a payment API we must never call twice for the same order.
//! static CHARGES: AtomicU32 = AtomicU32::new(0);
//!
//! #[durare::step]
//! async fn charge_card(ctx: &DurableContext, order_id: String) -> Result<String> {
//!     CHARGES.fetch_add(1, Ordering::SeqCst);
//!     Ok(format!("ch_{order_id}"))
//! }
//!
//! #[durare::step]
//! async fn send_receipt(ctx: &DurableContext, charge_id: String) -> Result<()> {
//!     // A crash between the two steps does NOT re-charge: on restart,
//!     // `charge_card` is served from its checkpoint and the workflow
//!     // resumes right here.
//!     println!("emailing receipt for {charge_id}");
//!     Ok(())
//! }
//!
//! #[durare::workflow]
//! async fn process_order(ctx: DurableContext, order_id: String) -> Result<String> {
//!     // Reads like ordinary async code; each step checkpoints once.
//!     let charge_id = charge_card(&ctx, order_id).await?;
//!     send_receipt(&ctx, charge_id.clone()).await?;
//!     Ok(charge_id)
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! engine.recover().await?; // after a crash: resume every unfinished workflow
//!
//! let handle = engine
//!     .start_with(ProcessOrder, "1001".into(), WorkflowOptions::with_id("order-1001"))
//!     .await?;
//! assert_eq!(handle.await?, "ch_1001");
//!
//! // The same trigger arrives again — same workflow id, no second charge.
//! let duplicate = engine
//!     .start_with(ProcessOrder, "1001".into(), WorkflowOptions::with_id("order-1001"))
//!     .await?;
//! assert_eq!(duplicate.await?, "ch_1001");          // served from the checkpoint
//! assert_eq!(CHARGES.load(Ordering::SeqCst), 1);    // charged exactly once
//! # Ok(())
//! # }
//! ```
//!
//! Swap [`InMemoryProvider`] for [`PostgresProvider`] and the same guarantees
//! hold across processes, restarts, and a fleet of workers. For the full
//! crash-and-recover demo — a process killed mid-workflow, restarted, and
//! finishing without repeating work — run
//! [`examples/order.rs`](https://github.com/SamuelXing/durare/blob/main/examples/order.rs).
//!
//! # What's in the crate
//!
//! - **Workflows and steps** — [`DurableEngine`], [`DurableContext::step`] with
//!   retry policies ([`StepOptions`]), typed starts via
//!   [`DurableEngine::start_with`], durable [`sleep`](DurableContext::sleep).
//! - **Queues** — [`WorkflowQueue`]: worker and global concurrency, rate
//!   limits, priorities, deduplication, partitions.
//! - **Messaging and events** — durable [`send`](DurableContext::send) /
//!   [`recv`](DurableContext::recv) between workflows,
//!   [`set_event`](DurableContext::set_event) /
//!   [`get_event`](DurableContext::get_event) for observable state.
//! - **Streams** — append-only durable streams:
//!   [`write_stream`](DurableContext::write_stream), read whole
//!   ([`DurableEngine::read_stream`]) or incrementally
//!   ([`read_stream_values`](DurableEngine::read_stream_values)).
//! - **Scheduling** — cron workflows via `#[durare::workflow(schedule = "…")]`,
//!   plus managed schedules ([`DurableEngine::create_schedule`]: pause, resume,
//!   trigger, backfill).
//! - **Transactions** — [`DurableContext::transaction`] commits your SQL and
//!   the step checkpoint atomically, making the step exactly-once.
//! - **Composition** — child workflows
//!   ([`start_workflow`](DurableContext::start_workflow)), durable
//!   [`select`](DurableContext::select), code evolution with
//!   [`patch`](DurableContext::patch).
//! - **Management and operations** — list / cancel / resume / fork, timeouts,
//!   [`Debouncer`], the registry-less [`Client`] for other processes,
//!   [`AdminServer`] (feature `admin`), [`Conductor`] (feature `conductor`).
//! - **Backends** — [`PostgresProvider`] (feature `postgres`),
//!   [`SqliteProvider`] (feature `sqlite`), and [`InMemoryProvider`], all behind
//!   the [`StateProvider`] seam.
//!
//! # Guides
//!
//! Six module-level guides explain the concepts in depth, `std`-style, each
//! with tested examples: start with [`durability`] (checkpoints, replay, and
//! the determinism contract — read this first), then its companion
//! [`determinism`] (the rules for writing a correct workflow body — deterministic
//! control flow, durable-safe data, and dependencies), and [`queues`],
//! [`messaging`], [`transactions`], and [`observability`] (the `tracing`
//! spans the engine emits and how to export them). Eleven runnable,
//! end-to-end examples live in
//! [`examples/`](https://github.com/SamuelXing/durare/tree/main/examples).
//!
//! # Cargo features
//!
//! Backends are compiled behind features, all on by default; enable just one to
//! drop the other's driver. **At least one backend is required** — a
//! zero-backend build is a compile error. [`InMemoryProvider`] is always
//! available (no feature).
//!
//! - **`postgres`** *(default)* — the [`PostgresProvider`] backend.
//! - **`sqlite`** *(default)* — the [`SqliteProvider`] backend (a bundled C
//!   library; drop it with `default-features = false, features = ["postgres"]`
//!   for a pure-Postgres build that needs no C toolchain).
//! - **`conductor`** *(off by default)* — the DBOS Conductor client
//!   ([`Conductor`], [`ConductorConfig`], [`AlertHandler`]): a websocket client
//!   for the DBOS control plane, behind a feature because it pulls in a TLS
//!   websocket stack and gzip framing.
//! - **`admin`** *(off by default)* — the [`AdminServer`] HTTP control surface
//!   (health, recovery, and workflow management for the DBOS console/conductor
//!   and health probes), behind a feature because it pulls in the axum/hyper/tower
//!   HTTP stack.
// Render `#[doc(cfg(...))]` "available on feature X" banners on docs.rs (which
// builds with `--cfg docsrs`, see Cargo.toml). Inert on stable and CI builds.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

// A SQL backend is required: the transaction layer and the error type are built
// on sqlx driver types that exist only when a backend is compiled. The in-memory
// backend is always available but isn't enough on its own.
#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!(
    "durare needs at least one SQL backend feature: enable `postgres` and/or \
     `sqlite` (both are on by default)."
);

// Concept guides — std-style module pages (think `std::pin`) that each explain
// one subsystem, with tested examples. Implementation lives in the private
// modules below; the guides re-export the relevant types with
// `#[doc(no_inline)]` so every item's canonical documentation stays at the
// crate root.
pub mod determinism;
pub mod durability;
pub mod messaging;
pub mod observability;
pub mod queues;
pub mod transactions;

#[cfg(feature = "admin")]
mod admin;
mod client;
#[cfg(feature = "conductor")]
mod conductor;
mod context;
mod debounce;
mod engine;
mod error;
mod handle;
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
mod provider;
mod queue;
mod schedule;
mod serialize;
#[cfg(feature = "sqlite")]
mod sqlite;
mod tx;

#[cfg(feature = "admin")]
#[cfg_attr(docsrs, doc(cfg(feature = "admin")))]
pub use admin::AdminServer;
pub use client::Client;
#[cfg(feature = "conductor")]
#[cfg_attr(docsrs, doc(cfg(feature = "conductor")))]
pub use conductor::{AlertHandler, Conductor, ConductorConfig};
pub use context::{AuthContext, DurableContext, RetryPredicate, StepOptions};
pub use debounce::{Debouncer, DebouncerClient};
/// Macro plumbing referenced by `#[durare::workflow]`; not public API.
#[doc(hidden)]
pub use engine::WorkflowResult;
pub use engine::{
    erase, DeduplicationPolicy, DurableEngine, DurableEngineBuilder, EngineConfig, HealthReport,
    RegisteredWorkflow, WorkflowDef, WorkflowFn, WorkflowOptions, WorkflowRegistration,
};
pub use error::{Error, ErrorCode, Result};
/// Re-exported so callers can consume the asynchronous stream returned by
/// `read_stream_values` (`StreamExt::next`) without depending on `futures` directly.
pub use futures_util::{Stream, StreamExt};
pub use handle::WorkflowHandle;
pub use memory::InMemoryProvider;
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub use postgres::PostgresProvider;
pub use provider::{
    is_terminal, ChangeWait, DequeueRequest, ExportedWorkflow, ForkParams, ListFilter,
    StateProvider, StepAggregate, StepAggregateQuery, StepInfo, VersionInfo, WorkflowAggregate,
    WorkflowAggregateQuery, WorkflowStatus, STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED,
    STATUS_ERROR, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS,
};
pub use queue::{RateLimiter, WorkflowQueue};
pub use schedule::{
    ApplySchedule, ScheduleFilter, ScheduleOptions, ScheduleStatus, ScheduledInput,
    WorkflowSchedule,
};
pub use serialize::{
    PortableWorkflowArgs, PortableWorkflowError, Serializer, SerializerCodec, PORTABLE_ERROR_NAME,
};
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub use sqlite::SqliteProvider;
pub use tx::{IsolationLevel, Param, Row, RowValue, TransactionOptions, Tx, TxBody};

/// The `#[workflow]` attribute macro. Annotate an
/// `async fn(DurableContext, Input) -> Result<Output>` to have it
/// auto-registered with every [`DurableEngine`] in the binary.
pub use durare_macros::workflow;

/// The `#[step]` attribute macro. Annotate an
/// `async fn(&DurableContext, args..) -> Result<T>` to have its body run as a
/// durable [`DurableContext::step`] — checkpointed once, replayed thereafter —
/// so it reads like an ordinary async call.
pub use durare_macros::step;

/// The `#[transaction]` attribute macro. Annotate an
/// `async fn(&DurableContext, &mut Tx, args..) -> Result<T>` to have its body
/// run as a durable [`DurableContext::transaction`] — the SQL writes and the
/// checkpoint commit atomically — without the `|tx| Box::pin(..)` wrapper.
pub use durare_macros::transaction;

/// Re-exported so the `#[workflow]` macro can reference `durare::inventory::*`
/// from user crates without them depending on `inventory` directly.
#[doc(hidden)]
pub use inventory;
