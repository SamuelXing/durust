//! Order-processing demo: charge → ship → email, with durable checkpoints.
//!
//! Run it twice to see crash recovery. State must survive a process restart, so
//! use a persistent backend (Postgres here; SQLite works too). The crash is
//! injected with a [fail-rs](https://github.com/tikv/fail-rs) failpoint named
//! `after_charge`:
//!
//! ```text
//! export DATABASE_URL=postgres://localhost:5432/durare
//!
//! # Run 1: the `after_charge` failpoint exits the process right after charging.
//! FAILPOINTS=after_charge=return cargo run --example order
//!
//! # Run 2: recover() resumes the same workflow. Notice the card is NOT
//! # charged again — the charge step is served from its checkpoint.
//! cargo run --example order
//! ```
//!
//! Without DATABASE_URL it uses the in-memory backend (no crash-across-restart,
//! but you can still see the workflow run end to end).

use durare::{
    DurableContext, DurableEngine, InMemoryProvider, PostgresProvider, Result, StateProvider,
    WorkflowOptions,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone)]
struct Order {
    id: String,
    amount_cents: i64,
    email: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Receipt {
    charge_id: String,
    shipment_id: String,
}

// Each side effect is a durable step: it runs at most once per order and is
// served from its checkpoint on replay. `#[durare::step]` is all it takes —
// no closure, no `Box::pin`, no `Ok::<_, Error>`.
#[durare::step]
async fn charge_card(ctx: &DurableContext, order_id: String, amount_cents: i64) -> Result<String> {
    println!("  >> CHARGING card for {amount_cents} cents  (real side effect!)");
    Ok(format!("ch_{order_id}"))
}

#[durare::step]
async fn create_shipment(ctx: &DurableContext, order_id: String) -> Result<String> {
    println!("  >> creating shipment");
    Ok(format!("sh_{order_id}"))
}

#[durare::step]
async fn send_email(ctx: &DurableContext, to: String) -> Result<()> {
    println!("  >> emailing receipt to {to}");
    Ok(())
}

#[durare::workflow]
async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> {
    let charge_id = charge_card(&ctx, order.id.clone(), order.amount_cents).await?;
    println!("  charge_id = {charge_id}");

    // Simulate a hard crash between steps via a failpoint. Arm it with
    // `FAILPOINTS=after_charge=return` (see the module docs); otherwise it is a
    // no-op and the workflow runs straight through.
    fail::fail_point!("after_charge", |_| {
        println!("  !! failpoint `after_charge` fired — crashing after charge");
        std::process::exit(1);
    });

    let shipment_id = create_shipment(&ctx, order.id.clone()).await?;
    send_email(&ctx, order.email.clone()).await?;

    Ok(Receipt {
        charge_id,
        shipment_id,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Read the FAILPOINTS env var so the `after_charge` failpoint can be armed.
    let _failpoints = fail::FailScenario::setup();

    // Pick a backend: Postgres if DATABASE_URL is set, else in-memory.
    let provider: Arc<dyn StateProvider> = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            println!("[backend] Postgres");
            Arc::new(PostgresProvider::connect(&url).await?)
        }
        Err(_) => {
            println!("[backend] in-memory (set DATABASE_URL for crash-across-restart)");
            Arc::new(InMemoryProvider::new())
        }
    };

    // No manual register: `#[durare::workflow]` auto-registers process_order.
    let engine = DurableEngine::new(provider).await?;

    // Resume anything a previous crashed run left incomplete (a direct workflow
    // is re-run inline from its last checkpoint).
    let resumed = engine.recover().await?;
    if resumed > 0 {
        println!("[recover] resumed {resumed} incomplete workflow(s)");
    }

    let order = Order {
        id: "1001".to_string(),
        amount_cents: 4999,
        email: "sam@example.com".to_string(),
    };

    // Start by typed reference — `ProcessOrder` is the marker `#[durare::workflow]`
    // emits, so the input type is checked and the output type is inferred (no
    // string name, no turbofish). Await the handle for the result.
    println!("[start] process_order wf-order-1001");
    let handle = engine
        .start_with(
            ProcessOrder,
            order,
            WorkflowOptions::with_id("wf-order-1001"),
        )
        .await?;
    match handle.await {
        Ok(receipt) => println!("[done] {receipt:?}"),
        Err(e) => println!("[error] {e}"),
    }

    Ok(())
}
