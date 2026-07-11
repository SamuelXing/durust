//! # durare
//!
//! A DBOS-style **durable execution** library for Rust.
//!
//! Write normal async code; wrap each side-effecting unit in [`DurableContext::step`].
//! Every step's result is checkpointed to a [`StateProvider`] (Postgres in v0.1).
//! If the process crashes, call [`DurableEngine::recover`] on restart and each
//! workflow resumes exactly where it left off — completed steps are served from
//! their checkpoints instead of re-running.
//!
//! There is **no separate server**: the engine is a library that runs inside
//! your worker and talks directly to the database. Storage is hidden behind the
//! [`StateProvider`] trait so a DynamoDB / Aurora DSQL backend can be added
//! later without changing the engine.
//!
//! ```no_run
//! use durare::{DurableEngine, DurableContext, InMemoryProvider, Error, Result, WorkflowOptions};
//! use std::sync::Arc;
//!
//! async fn hello(ctx: DurableContext, name: String) -> Result<String> {
//!     let greeting = ctx.step("build_greeting", || async {
//!         Ok::<_, Error>(format!("hello, {name}"))
//!     }).await?;
//!     Ok(greeting)
//! }
//!
//! # async fn run() -> Result<()> {
//! let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! engine.register("hello", hello);
//! engine.recover().await?; // resume anything left incomplete by a prior crash
//! // `start` returns a handle immediately; await it (or `.result()`) for the output.
//! let handle = engine.start("hello", "world".to_string(), WorkflowOptions::with_id("wf-1")).await?;
//! let out: String = handle.result().await?;
//! assert_eq!(out, "hello, world");
//! # Ok(())
//! # }
//! ```

mod admin;
mod client;
mod conductor;
mod context;
mod debounce;
mod engine;
mod error;
mod handle;
mod memory;
mod postgres;
mod provider;
mod queue;
mod schedule;
mod serialize;
mod sqlite;
mod tx;

pub use admin::AdminServer;
pub use client::Client;
pub use conductor::{AlertHandler, Conductor, ConductorConfig};
pub use context::{AuthContext, DurableContext, RetryPredicate, StepOptions};
pub use debounce::{Debouncer, DebouncerClient};
/// Macro plumbing referenced by `#[durare::workflow]`; not public API.
#[doc(hidden)]
pub use engine::WorkflowResult;
pub use engine::{
    erase, DeduplicationPolicy, DurableEngine, DurableEngineBuilder, EngineConfig,
    RegisteredWorkflow, WorkflowDef, WorkflowFn, WorkflowOptions, WorkflowRegistration,
};
pub use error::{Error, ErrorCode, Result};
/// Re-exported so callers can consume the asynchronous stream returned by
/// `read_stream_values` (`StreamExt::next`) without depending on `futures` directly.
pub use futures_util::{Stream, StreamExt};
pub use handle::WorkflowHandle;
pub use memory::InMemoryProvider;
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
pub use sqlite::SqliteProvider;
pub use tx::{IsolationLevel, Param, Row, TransactionOptions, Tx, TxBody};

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
