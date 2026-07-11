//! Scheduled (cron) workflow: run a workflow automatically on a recurring tick.
//!
//! Annotating a workflow with `#[durare::workflow(schedule = "...")]` makes the
//! engine fire it on a cron schedule once [`DurableEngine::launch`] is running.
//! The schedule is a 6-field cron expression — `sec min hour dom mon dow` — so
//! sub-minute cadences are expressible; this one runs every second.
//!
//! Each tick is a durable workflow keyed by its tick time, so it runs **exactly
//! once** even if several executors share one database — whichever inserts the
//! `sched-…` row first wins, the rest collide and skip.
//!
//! ```text
//! cargo run --example scheduled
//! ```

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, ScheduledInput};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static TICKS: AtomicUsize = AtomicUsize::new(0);

// Runs every second. The `ScheduledInput` carries the cron instant this run
// fires for (and any context value attached to the schedule).
#[durare::workflow(schedule = "* * * * * *")]
async fn collect_metrics(_ctx: DurableContext, tick: ScheduledInput) -> Result<()> {
    let n = TICKS.fetch_add(1, Ordering::SeqCst) + 1;
    println!(
        "  ⏰ tick #{n} at {} — collecting metrics",
        tick.scheduled_time.format("%H:%M:%S")
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // No manual registration: `#[durare::workflow(schedule = ...)]` auto-registers
    // collect_metrics and its schedule. launch() starts the cron loop.
    println!("[start] scheduler running — watching for ~4 seconds");
    engine.launch().await?;

    // Let a few ticks fire.
    tokio::time::sleep(Duration::from_millis(4000)).await;

    // shutdown() drains any in-flight tick before we read the counter.
    engine.shutdown(Duration::from_secs(1)).await?;
    println!("[done] {} ticks fired", TICKS.load(Ordering::SeqCst));
    Ok(())
}
