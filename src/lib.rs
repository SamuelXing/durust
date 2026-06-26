//! # durust
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
//! use durust::{DurableEngine, DurableContext, InMemoryProvider, Error, Result};
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
//! let out: String = engine.start_typed("hello", "wf-1", "world".to_string()).await?;
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
pub use context::{AuthContext, DurableContext, StepOptions};
pub use debounce::Debouncer;
pub use engine::{
    erase, DeduplicationPolicy, DurableEngine, RegisteredWorkflow, WorkflowFn, WorkflowOptions,
    WorkflowRegistration,
};
pub use error::{Error, ErrorCode, Result};
pub use handle::WorkflowHandle;
pub use memory::InMemoryProvider;
pub use postgres::PostgresProvider;
pub use provider::{
    is_terminal, DequeueRequest, ExportedWorkflow, ListFilter, StateProvider, StepAggregate,
    StepAggregateQuery, StepInfo, VersionInfo, WorkflowAggregate, WorkflowAggregateQuery,
    WorkflowStatus, STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS,
};
pub use queue::{RateLimiter, WorkflowQueue};
pub use schedule::{
    ApplySchedule, ScheduleFilter, ScheduleOptions, ScheduleStatus, WorkflowSchedule,
};
pub use serialize::{PortableWorkflowArgs, Serializer};
pub use sqlite::SqliteProvider;
pub use tx::{IsolationLevel, Param, Row, TransactionOptions, Tx, TxBody};

/// The `#[workflow]` attribute macro. Annotate an
/// `async fn(DurableContext, Input) -> Result<Output>` to have it
/// auto-registered with every [`DurableEngine`] in the binary.
pub use durust_macros::workflow;

/// Re-exported so the `#[workflow]` macro can reference `durust::inventory::*`
/// from user crates without them depending on `inventory` directly.
#[doc(hidden)]
pub use inventory;
