//! Transactional steps: SQL writes and the durable checkpoint commit together.
//!
//! A `#[durare::transaction]` runs its body inside one database transaction and
//! records the step's completion in that *same* transaction. So either both the
//! SQL and the checkpoint land, or neither does — a crash can never leave the
//! step "done" in the log but not applied to your tables, nor applied twice.
//!
//! The `main` below proves the exactly-once property: it runs a money transfer,
//! then re-runs the *same workflow id*. The second run replays from the
//! checkpoint instead of touching the accounts again, so the balance moves once
//! — a re-execution returns the same `(70, 30)`, never `(40, 60)`.
//!
//! Uses SQLite in a temp file so it is self-contained; Postgres works the same.
//!
//! ```text
//! cargo run --example transfer
//! ```

use durare::{params, DurableContext, DurableEngine, Result, SqliteProvider, Tx, WorkflowOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
struct Transfer {
    from: String,
    to: String,
    cents: i64,
}

// Create the table and seed two accounts. `ON CONFLICT DO NOTHING` keeps a
// re-run from reseeding.
#[durare::transaction]
async fn setup(ctx: &DurableContext, tx: &mut Tx<'_>) -> Result<()> {
    tx.execute(
        "CREATE TABLE IF NOT EXISTS account (name TEXT PRIMARY KEY, cents INTEGER)",
        &params![],
    )
    .await?;
    for (name, cents) in [("alice", 100_i64), ("bob", 0_i64)] {
        tx.execute(
            "INSERT INTO account (name, cents) VALUES (?, ?) ON CONFLICT (name) DO NOTHING",
            &params![name, cents],
        )
        .await?;
    }
    Ok(())
}

// Debit and credit in one transaction — atomic with the checkpoint.
#[durare::transaction]
async fn transfer(
    ctx: &DurableContext,
    tx: &mut Tx<'_>,
    from: String,
    to: String,
    cents: i64,
) -> Result<()> {
    println!("  >> moving {cents} cents from {from} to {to}");
    tx.execute(
        "UPDATE account SET cents = cents - ? WHERE name = ?",
        &params![cents, from],
    )
    .await?;
    tx.execute(
        "UPDATE account SET cents = cents + ? WHERE name = ?",
        &params![cents, to],
    )
    .await?;
    Ok(())
}

#[durare::transaction]
async fn balance(ctx: &DurableContext, tx: &mut Tx<'_>, name: String) -> Result<i64> {
    let row = tx
        .query_one("SELECT cents FROM account WHERE name = ?", &params![name])
        .await?;
    Ok(row.get::<i64>("cents"))
}

#[durare::workflow]
async fn run_transfer(ctx: DurableContext, req: Transfer) -> Result<(i64, i64)> {
    setup(&ctx).await?;
    transfer(&ctx, req.from.clone(), req.to.clone(), req.cents).await?;
    Ok((balance(&ctx, req.from).await?, balance(&ctx, req.to).await?))
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::temp_dir().join(format!("durare-transfer-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.launch().await?;

    let req = Transfer {
        from: "alice".to_string(),
        to: "bob".to_string(),
        cents: 30,
    };

    // First run: alice 100 / bob 0  ->  transfer 30  ->  alice 70 / bob 30.
    println!("[run 1] xfer-1");
    let (alice, bob): (i64, i64) = engine
        .start_with(RunTransfer, req.clone(), WorkflowOptions::with_id("xfer-1"))
        .await?
        .await?;
    println!("  balances: alice={alice}, bob={bob}");

    // Re-run under the *same* id. The workflow is already terminal, so it
    // replays from its checkpoint — the transfer does NOT run again. If it did,
    // alice would be 40; it stays 70.
    println!("[run 2] xfer-1 again (same id — must replay, not re-transfer)");
    let (alice2, bob2): (i64, i64) = engine
        .start_with(RunTransfer, req, WorkflowOptions::with_id("xfer-1"))
        .await?
        .await?;
    println!("  balances: alice={alice2}, bob={bob2}");
    assert_eq!((alice, bob), (alice2, bob2), "re-run must be a no-op");
    println!("[ok] transfer applied exactly once");

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}
