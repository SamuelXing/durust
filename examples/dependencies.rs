//! Dependency injection: where a database pool, HTTP client, or config lives.
//!
//! A dependency is not durable — a live connection cannot be checkpointed or
//! replayed. So it never belongs in durable state: not as a workflow parameter
//! (those are serialized), not in a step's return value. The pattern is to build
//! it once at startup into a process global, and read it *inside steps*, where
//! side effects belong.
//!
//! This demo wires a small `PricingService` (stand-in for an HTTP client plus the
//! config it needs) through a global `OnceLock`, reads it inside a step, and
//! proves the model by re-running the same workflow id: the dependency is invoked
//! exactly once and its result is checkpointed, so the replay never calls it
//! again — and the workflow body never mentions the dependency at all.
//!
//! See the [`determinism`](durare::determinism) guide's "Dependencies" section
//! for the full rationale.
//!
//! ```text
//! cargo run --example dependencies
//! ```

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

// A dependency: in a real app this owns an HTTP client and the config it needs.
// Built once at startup, never serialized, never stored in durable state.
struct PricingService {
    api_base: String,
    fee_bps: u64, // fee in basis points (1 bps = 0.01%)
}

impl PricingService {
    // Stands in for an outbound call to the pricing API. Counts its invocations
    // so `main` can prove a replay does not call it again.
    fn quote(&self, amount_cents: u64) -> u64 {
        CALLS.fetch_add(1, Ordering::SeqCst);
        println!(
            "  >> POST {}/quote  ({amount_cents}c @ {}bps)",
            self.api_base, self.fee_bps
        );
        amount_cents * self.fee_bps / 10_000
    }
}

// The process global. Set once in `main`, before the engine launches; read inside
// steps via `DEPS.get()`. Rebuilt fresh on every process start — never persisted.
static DEPS: OnceLock<PricingService> = OnceLock::new();

// Proves the dependency runs on first execution only, not on replay.
static CALLS: AtomicUsize = AtomicUsize::new(0);

// A step reads the global dependency. Using it is a side effect, so it belongs
// here — not in the workflow body, which re-runs on replay.
#[durare::step]
async fn compute_fee(ctx: &DurableContext, amount_cents: u64) -> Result<u64> {
    let deps = DEPS.get().expect("deps set at startup");
    Ok(deps.quote(amount_cents))
}

// The workflow body is deterministic and never names the dependency: it only
// calls the step and adds up recorded results.
#[durare::workflow]
async fn checkout(ctx: DurableContext, amount_cents: u64) -> Result<u64> {
    let fee = compute_fee(&ctx, amount_cents).await?;
    Ok(amount_cents + fee)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Build dependencies ONCE, at startup, before launching the engine. Swapping
    // this line per environment (a staging vs prod URL, a different fee) needs no
    // change to the workflow or its durable state.
    DEPS.set(PricingService {
        api_base: "https://pricing.internal".to_string(),
        fee_bps: 250,
    })
    .unwrap_or_else(|_| unreachable!("DEPS is set exactly once at startup"));

    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.launch().await?;

    // Run 1: the step calls the dependency and checkpoints the result.
    let total: u64 = engine
        .start_with(Checkout, 10_000u64, WorkflowOptions::with_id("checkout-1"))
        .await?
        .await?;
    println!(
        "[run 1] total = {total}c  (dependency calls: {})\n",
        CALLS.load(Ordering::SeqCst)
    );

    // Run 2: same id. The step result is served from its checkpoint — the
    // dependency is NOT called again, and the value is identical across replay.
    let total_again: u64 = engine
        .start_with(Checkout, 10_000u64, WorkflowOptions::with_id("checkout-1"))
        .await?
        .await?;
    println!(
        "[run 2] total = {total_again}c  (dependency calls: still {})",
        CALLS.load(Ordering::SeqCst)
    );

    assert_eq!(total, total_again);
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        1,
        "replay must not re-invoke the dependency"
    );
    println!(
        "\n[ok] dependency lives in a global, is read inside a step, and runs \
         exactly once — the replay uses the checkpoint"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
