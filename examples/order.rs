//! Order-processing demo: charge → ship → email, with durable checkpoints.
//!
//! Run it twice to see crash recovery (requires Postgres so state survives a
//! process restart):
//!
//! ```text
//! export DATABASE_URL=postgres://localhost:5432/durust
//!
//! # Run 1: crashes right after charging the card.
//! CRASH_AFTER_CHARGE=1 cargo run --example order
//!
//! # Run 2: recover() resumes the same workflow. Notice the card is NOT
//! # charged again — the charge step is served from its checkpoint.
//! cargo run --example order
//! ```
//!
//! Without DATABASE_URL it uses the in-memory backend (no crash-across-restart,
//! but you can still see the workflow run end to end).

use durust::{
    DurableContext, DurableEngine, Error, InMemoryProvider, PostgresProvider, Result, StateProvider,
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

#[durust::workflow]
async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> {
    // STEP 1 — the side effect we must never repeat.
    let charge_id = ctx
        .step("charge_card", || async {
            println!(
                "  >> CHARGING card for {} cents  (real side effect!)",
                order.amount_cents
            );
            Ok::<_, Error>(format!("ch_{}", order.id))
        })
        .await?;
    println!("  charge_id = {charge_id}");

    // Simulate a hard crash between steps.
    if std::env::var("CRASH_AFTER_CHARGE").is_ok() {
        println!("  !! simulating crash after charge — exiting");
        std::process::exit(1);
    }

    // STEP 2
    let shipment_id = ctx
        .step("create_shipment", || async {
            println!("  >> creating shipment");
            Ok::<_, Error>(format!("sh_{}", order.id))
        })
        .await?;

    // STEP 3
    let email = order.email.clone();
    ctx.step("send_email", || async {
        println!("  >> emailing receipt to {email}");
        Ok::<_, Error>(())
    })
    .await?;

    Ok(Receipt {
        charge_id,
        shipment_id,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
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

    // No manual register: `#[durust::workflow]` auto-registers process_order.
    let engine = DurableEngine::new(provider).await?;

    // Resume anything a previous crashed run left incomplete.
    let resumed = engine.recover().await?;
    if resumed > 0 {
        println!("[recover] resumed {resumed} incomplete workflow(s)");
    }

    let order = Order {
        id: "1001".to_string(),
        amount_cents: 4999,
        email: "sam@example.com".to_string(),
    };

    println!("[start] process_order wf-order-1001");
    match engine
        .start_typed::<_, Receipt>("process_order", "wf-order-1001", order)
        .await
    {
        Ok(receipt) => println!("[done] {receipt:?}"),
        Err(e) => println!("[error] {e}"),
    }

    Ok(())
}
