# durust

A **DBOS-style durable execution** library for Rust, aligned with the
[DBOS Transact Go SDK](https://github.com/dbos-inc/dbos-transact-golang). Write
normal async code; each step is checkpointed to a database; after a crash the
workflow resumes exactly where it left off — completed steps are **not** re-run.

There is no separate server. The engine is a library that runs inside your worker
and talks directly to the state backend. Storage sits behind one trait
(`StateProvider`) with three implementations — **Postgres**, **SQLite**, and an
in-memory one for tests — so a new backend can be added without touching the
engine.

## The model

```rust
use durust::{DurableContext, Error, Result};

#[durust::workflow]
async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> {
    let charge_id = ctx.step("charge_card", || async {
        Ok::<_, Error>(charge_card(&order).await?)   // side effect, recorded once
    }).await?;

    let shipment_id = ctx.step("create_shipment", || async {
        Ok::<_, Error>(create_shipment(&order).await?)
    }).await?;

    Ok(Receipt { charge_id, shipment_id })
}
```

```rust
use durust::{DurableEngine, SqliteProvider, WorkflowOptions};
use std::sync::Arc;

# async fn run() -> durust::Result<()> {
let engine = DurableEngine::new(Arc::new(SqliteProvider::connect("sqlite://durust.db").await?)).await?;
engine.recover().await?;           // resume anything a prior crash left incomplete
engine.launch().await?;            // start queue dispatchers + cron schedulers

// Non-blocking: returns a handle immediately.
let mut handle = engine
    .run_workflow::<_, Receipt>("process_order", order, WorkflowOptions::default())
    .await?;
let receipt: Receipt = handle.get_result().await?;
# Ok(()) }
```

Steps are matched to their checkpoints by a **deterministic per-execution
counter**, so — exactly like Temporal/DBOS — your workflow's control flow must be
deterministic. Non-determinism (wall-clock, RNG, map iteration order) belongs
*inside* a step, where its result is recorded.

## Features

Annotate workflows with `#[durust::workflow]` (auto-registered via `inventory`)
or register them manually with `engine.register(name, f)`.

### Durable steps & retries
- **`ctx.step(name, closure)`** — runs the closure once, persists its result; on
  replay returns the stored result without re-running.
- **`ctx.step_with(StepOptions, closure)`** — adds exponential-backoff retries
  (`max_retries`, `backoff_factor`, `base_interval`, `max_interval`).
- **`ctx.sleep(duration)`** — durable timer; the wake instant is recorded as a
  step so it doesn't drift across crashes.

### Workflow handles
`run_workflow` returns a `WorkflowHandle<O>` **without blocking**:
`handle.get_result().await`, `handle.get_status().await`, `handle.id()`.
`start` / `start_typed` are blocking convenience wrappers.

### Durable queues
```rust
use durust::{WorkflowQueue, RateLimiter};
use std::time::Duration;

engine.register_queue(
    WorkflowQueue::new("emails")
        .worker_concurrency(4)                                    // per-process limit
        .global_concurrency(20)                                   // across all executors
        .priority_enabled()                                       // lower priority runs first
        .rate_limiter(RateLimiter { limit: 50, period: Duration::from_secs(60) }),
);
let handle = engine.enqueue::<_, ()>("emails", "send_email", msg, opts).await?;
```
A per-queue dispatcher (started by `launch()`) claims work respecting worker /
global concurrency, rate limits, priority, per-tick **delay**, and queue-scoped
**deduplication** (`WorkflowOptions { dedup_id, delay, priority, .. }`). On
Postgres claims use `FOR UPDATE SKIP LOCKED`; SQLite uses a transactional claim.

### Messaging & events
- **`ctx.send(dest_id, msg, topic)` / `ctx.recv::<T>(topic, timeout)`** — durable,
  FIFO, exactly-once messaging (the claim and its checkpoint commit atomically);
  `recv` returns `None` on timeout.
- **`ctx.set_event(key, value)` / `ctx.get_event::<T>(target_id, key, timeout)`** —
  key-value events published from a workflow.
- `engine.send` / `engine.get_event` are available to non-workflow callers.

### Scheduled (cron) workflows
```rust
// 6-field cron (sec min hour dom mon dow). The tick input carries the fire
// time and any context attached to the schedule.
#[durust::workflow(schedule = "0 0 * * * *")] // top of every hour
async fn hourly(ctx: DurableContext, tick: ScheduledInput) -> Result<()> {
    println!("fired for {}", tick.scheduled_time);
    Ok(())
}
```
Each tick starts the workflow under a deterministic `sched-<name>-<time>` id, so
it runs **once per tick even across multiple executors**.

### Timeouts
`WorkflowOptions { timeout: Some(dur), .. }` fixes a deadline when the workflow
starts (at claim time for queued workflows); a run that overruns it is cancelled.

### Management & recovery
- `retrieve_workflow`, `list_workflows(ListFilter { .. })`, `cancel_workflow`,
  `resume_workflow` (re-runs from checkpoints), `fork_workflow(id, start_step)`
  (reuses checkpoints before `start_step`, re-executes the rest).
- `recover()` re-runs incomplete workflows of the engine's **application
  version**; runs past a recovery-attempt cap are parked in
  `MAX_RECOVERY_ATTEMPTS_EXCEEDED`; queued workflows are returned to their queue.

## Quick start

```bash
cargo test                     # 35 tests; in-memory + SQLite (no server needed)
cargo run --example order      # in-memory backend, or set DATABASE_URL for Postgres
```

### Crash recovery (Postgres)

```bash
createdb durust
export DATABASE_URL=postgres://localhost:5432/durust

# Run 1: a fail-rs failpoint crashes the process right after charging.
FAILPOINTS=after_charge=return cargo run --example order
# Run 2: recover() resumes; the card is NOT re-charged (charge step is replayed).
cargo run --example order
```

## Backends & schema

| Backend | Use | Crash recovery |
| --- | --- | --- |
| `PostgresProvider` | production / multi-executor | yes |
| `SqliteProvider` | durable local dev, single node | yes (file DB) |
| `InMemoryProvider` | tests, examples | no (in-process only) |

The schema follows the DBOS canonical tables — `workflow_status`,
`operation_outputs` (step checkpoints), `notifications`, `workflow_events` —
applied via embedded, per-dialect migrations (`migrations/{postgres,sqlite}/`)
using `sqlx::migrate!`. `init()` runs pending migrations on startup; inspect
state with plain SQL:

```sql
SELECT workflow_uuid, name, status FROM workflow_status;
SELECT workflow_uuid, function_id, function_name, output FROM operation_outputs;
```

## Exactly-once, honestly

- **Workflow state transitions are exactly-once** (checkpoint per step, idempotent
  insert).
- **A step's external side effect is at-least-once** in the crash window
  "side-effect committed, checkpoint not yet written." Make external calls
  idempotent (idempotency keys) — same caveat as Temporal and DBOS.

## Out of scope (for now)

Child workflows, streaming, authenticated-user/roles, queue partitioning,
cross-language serialization, the application-version management API, a conductor
/ admin server, and Postgres `LISTEN`/`NOTIFY` (blocked `recv`/`get_event` poll
instead). The `StateProvider` trait is the seam where more backends can be added.

## License

MIT OR Apache-2.0
