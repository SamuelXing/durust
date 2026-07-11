//! Durable sleep: a timer that survives restarts without drifting or re-firing.
//!
//! `ctx.sleep(dur)` is not `tokio::sleep`. On the first call it persists the
//! absolute wake instant as a checkpoint, so if the workflow crashes and is
//! recovered mid-nap it waits only the *remaining* time — and the steps before
//! the nap are served from their checkpoints, never re-run. That's what lets a
//! workflow legitimately "sleep for 30 days" between actions: a subscription that
//! bills monthly, a reminder, a trial-expiry sweep.
//!
//! This demo bills three months with a durable nap between each, then re-runs the
//! same workflow id to show the naps and charges are checkpointed: the second run
//! returns instantly and charges nothing. (For the crash-and-recover mechanics on
//! a persistent backend, see the `order` example.)
//!
//! ```text
//! cargo run --example timer
//! ```

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Counts real charges so we can prove the second run repeats none.
static CHARGES: AtomicUsize = AtomicUsize::new(0);

#[durare::step]
async fn charge(ctx: &DurableContext, month: u32, cents: i64) -> Result<i64> {
    CHARGES.fetch_add(1, Ordering::SeqCst);
    println!("  >> charging {cents} cents for month {month}");
    Ok(cents)
}

#[durare::workflow]
async fn subscription(ctx: DurableContext, months: u32) -> Result<i64> {
    let mut total = 0;
    for m in 1..=months {
        total += charge(&ctx, m, 999).await?;
        if m < months {
            // The durable nap. Persisted wake instant → a crash here resumes the
            // remaining wait on recovery, and never re-charges the months above.
            ctx.sleep(Duration::from_millis(700)).await?;
        }
    }
    Ok(total)
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // Run 1: three charges, two ~0.7s durable naps between them.
    let t0 = Instant::now();
    let total: i64 = engine
        .start_with(Subscription, 3u32, WorkflowOptions::with_id("sub-ada"))
        .await?
        .await?;
    println!(
        "[run 1] billed {total} cents over 3 months in {:?} — {} charges\n",
        t0.elapsed(),
        CHARGES.load(Ordering::SeqCst)
    );

    // Run 2: same id. The workflow is terminal, so every step — charges AND naps
    // — is served from its checkpoint: this returns immediately and bills nothing.
    let t1 = Instant::now();
    let total_again: i64 = engine
        .start_with(Subscription, 3u32, WorkflowOptions::with_id("sub-ada"))
        .await?
        .await?;
    println!(
        "[run 2] same id -> {total_again} cents in {:?} — still {} charges (no re-nap, no re-bill)",
        t1.elapsed(),
        CHARGES.load(Ordering::SeqCst)
    );

    assert_eq!(total, total_again);
    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        3,
        "run 2 must not re-charge"
    );
    println!("\n[ok] durable naps + charges checkpointed: replay never re-waits or re-bills");
    Ok(())
}
