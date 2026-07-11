//! How durable execution works: checkpoints, replay, and the determinism
//! contract.
//!
//! Read this guide first — everything else in the crate builds on the model
//! described here. It has no API of its own; it explains the machinery behind
//! [`DurableContext`] and [`DurableEngine`].
//!
//! # The execution model
//!
//! A workflow is an ordinary async function. What makes it durable is that it
//! is allowed to be **executed more than once** — and must reach the same
//! result each time. The first execution does the real work. If the process
//! crashes, restarts, or the workflow is resumed or
//! [forked](DurableEngine::fork_workflow), a later execution **replays** the
//! function from the beginning.
//!
//! Replay does not repeat side effects, because of one rule: every side effect
//! lives inside a **step**, and every step's outcome is **checkpointed** to
//! the database before the workflow moves past it. Each workflow accumulates
//! an ordered log of its durable operations — steps, [sleeps], [sends],
//! [child starts] — one row per operation, keyed by sequence position. When
//! the function is replayed, the engine walks it again; at each durable
//! operation it finds the recorded outcome and returns it *without running
//! anything*, until it reaches the first operation with no checkpoint. That is
//! exactly where the previous execution stopped, and execution is live from
//! there on.
//!
//! Three consequences worth internalizing:
//!
//! - **A step that succeeded never re-runs.** Its recorded output is served on
//!   every subsequent execution.
//! - **A step that failed stays failed.** The error is checkpointed too, and a
//!   replay returns the same error rather than giving a flaky step a second
//!   chance to succeed and send the workflow down a different path.
//! - **Code *between* steps re-runs on every replay.** It must be a pure
//!   function of the workflow input and prior step results.
//!
//! # The determinism contract
//!
//! Replay is only sound if the workflow function — given the same input and
//! the same recorded step results — performs the **same durable operations in
//! the same order**. The engine enforces this as it goes: when a replay
//! reaches step position *n* and finds a *different* operation recorded there,
//! it fails with [`Error::UnexpectedStep`] instead of silently returning the
//! wrong checkpoint.
//!
//! In practice the contract reduces to one habit: capture every source of
//! non-determinism **inside a step**, and only branch on the recorded value.
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! // WRONG: fresh randomness outside a step. A replay draws a different
//! // value, the branch flips, and the step sequence diverges.
//! let lucky = uuid::Uuid::new_v4().as_u128() % 2 == 0;
//!
//! // RIGHT: record it once; every replay sees the same value.
//! let lucky = ctx.step("draw_lottery", || async {
//!     Ok::<_, Error>(uuid::Uuid::new_v4().as_u128() % 2 == 0)
//! }).await?;
//!
//! if lucky {
//!     ctx.step("apply_discount", || async { Ok::<_, Error>(()) }).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The same reasoning applies to clocks, environment variables, config reads,
//! and anything fetched over the network. Durable primitives are already safe:
//! [`sleep`][sleeps] records its wake instant, `recv`/`get_event` record both
//! the value observed and the timeout deadline, and a [durable
//! select](DurableContext::select) records which branch won.
//!
//! # Crash recovery
//!
//! On startup, call [`DurableEngine::recover`]: it finds every workflow this
//! application version left unfinished and re-dispatches it — each one replays
//! to its frontier and continues. Queued workflows need nothing special: an
//! `ENQUEUED` row survives the crash and is simply claimed again by the next
//! dispatcher. A workflow that keeps crashing is not retried forever — after a
//! bounded number of recovery attempts it is parked as
//! `MAX_RECOVERY_ATTEMPTS_EXCEEDED` for an operator to inspect and
//! resume.
//!
//! For a live demonstration — a process killed mid-workflow, restarted, and
//! finishing without repeating completed work — run
//! [`examples/order.rs`](https://github.com/SamuelXing/durare/blob/main/examples/order.rs).
//!
//! # The at-least-once window
//!
//! A step performs its effect, *then* its checkpoint commits — two writes to
//! two systems, and a crash can land between them. On replay the step re-runs.
//! So a step's side effect is **at-least-once**: exactly-once except when a
//! crash splits that window. Where it matters, make the effect idempotent —
//! pass a key derived from [`ctx.workflow_id()`](DurableContext::workflow_id)
//! and the step name to the downstream API, so the retry is recognized.
//!
//! Two cases are already closed for you:
//!
//! - **Writes to the workflow database**: use a
//!   [transaction](crate::transactions) — the SQL and the checkpoint commit
//!   atomically, making the step genuinely exactly-once.
//! - **Messages between workflows**: [`send`][sends] may re-deliver across a
//!   crash, but [`recv`](DurableContext::recv) consumes exactly once, and
//!   producers outside a workflow can use
//!   [`send_with_idempotency_key`](DurableEngine::send_with_idempotency_key).
//!
//! # Evolving workflow code
//!
//! Replay pins each workflow to the code shape it started under, so changing a
//! workflow function while runs are in flight needs care. Three tools, from
//! coarse to fine:
//!
//! - **Version gating.** Every run is stamped with an application version, and
//!   [`recover`](DurableEngine::recover) only re-dispatches rows of its own
//!   version — old executors drain old runs while new code takes new ones.
//! - **Patching.** [`DurableContext::patch`] forks behavior *inside* one
//!   workflow function: runs that already passed the patch point keep the old
//!   path, everything else takes the new one, and checkpoints stay aligned.
//! - **Forking.** [`DurableEngine::fork_workflow`] clones a workflow's
//!   checkpoints up to a chosen step and re-executes from there — useful to
//!   re-run a fixed version of a failed workflow without repeating its
//!   completed work.
//!
//! [sleeps]: DurableContext::sleep
//! [sends]: DurableContext::send
//! [child starts]: DurableContext::start_workflow

#[allow(unused_imports)]
use crate::{DurableContext, DurableEngine, Error};
