//! Human-in-the-loop: a workflow durably waits for an external decision.
//!
//! Two primitives make this work:
//!   * `ctx.recv(topic, timeout)` — the workflow blocks until someone sends a
//!     message on `topic` (or the timeout elapses). The wait is a checkpoint, so
//!     it survives a restart: on a persistent backend you can kill the process
//!     while it waits and it resumes the wait on recovery (see the `order`
//!     example for the crash mechanics).
//!   * `ctx.set_event(key, value)` — the workflow publishes progress that the
//!     outside world reads with `engine.get_event` / `client.get_event`, without
//!     touching the workflow itself.
//!
//! Here an expense workflow announces `awaiting_approval`, parks in `recv`, and a
//! "manager" on the outside observes that status and sends the decision.
//!
//! ```text
//! cargo run --example approval
//! ```

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Expense {
    who: String,
    cents: i64,
}

#[durare::workflow]
async fn expense_approval(ctx: DurableContext, exp: Expense) -> Result<String> {
    println!("  >> {} filed an expense for {} cents", exp.who, exp.cents);

    // Announce that we are waiting; an outside observer can read this.
    ctx.set_event("status", "awaiting_approval").await?;

    // Durably block until a decision arrives on the "decision" topic.
    let decision: Option<String> = ctx.recv("decision", Duration::from_secs(10)).await?;

    let outcome = match decision.as_deref() {
        Some("approve") => "approved",
        Some(_) => "rejected",
        None => "timed_out",
    };
    ctx.set_event("status", outcome).await?;
    println!("  << expense {outcome}");
    Ok(outcome.to_string())
}

/// Start one expense, watch it reach `awaiting_approval` from the outside, then
/// send it `decision`. Returns the final outcome.
async fn decide(engine: &DurableEngine, id: &str, exp: Expense, decision: &str) -> Result<String> {
    let handle = engine
        .start_with(ExpenseApproval, exp, WorkflowOptions::with_id(id))
        .await?;

    // Observe progress from outside the workflow.
    let status: Option<String> = engine
        .get_event(id, "status", Duration::from_secs(2))
        .await?;
    println!(
        "  [observer] {id} status = {}",
        status.as_deref().unwrap_or("?")
    );

    // A manager makes the call: nudge the waiting workflow.
    println!("  [manager] sending '{decision}' to {id}");
    engine.send(id, decision.to_string(), "decision").await?;

    handle.result().await
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    println!("== expense 1: approved ==");
    let a = decide(
        &engine,
        "exp-approve",
        Expense {
            who: "ada".to_string(),
            cents: 4200,
        },
        "approve",
    )
    .await?;
    println!("[done] outcome = {a}\n");

    println!("== expense 2: rejected ==");
    let b = decide(
        &engine,
        "exp-reject",
        Expense {
            who: "grace".to_string(),
            cents: 99_900,
        },
        "reject",
    )
    .await?;
    println!("[done] outcome = {b}");

    Ok(())
}
