//! `#[durare::transaction]`: an async fn whose body runs as a transactional
//! step — the SQL writes and the checkpoint commit together — with no
//! `|tx| Box::pin(async move { ... })` wrapper. Requires a SQL backend;
//! exercised here on SQLite.

use durare::{params, DurableContext, DurableEngine, Result, SqliteProvider, Tx, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

// No caller arguments: just ctx + the injected tx.
#[durare::transaction]
async fn setup(ctx: &DurableContext, tx: &mut Tx<'_>) -> Result<()> {
    tx.execute(
        "CREATE TABLE IF NOT EXISTS acct (id INTEGER PRIMARY KEY, bal INTEGER)",
        &params![],
    )
    .await?;
    tx.execute(
        "INSERT INTO acct (id, bal) VALUES (1, 100) ON CONFLICT (id) DO NOTHING",
        &params![],
    )
    .await?;
    Ok(())
}

// Caller arguments after the injected tx; the tx is not passed at the call site.
#[durare::transaction]
async fn debit(ctx: &DurableContext, tx: &mut Tx<'_>, amount: i64, id: i64) -> Result<i64> {
    tx.execute(
        "UPDATE acct SET bal = bal - ? WHERE id = ?",
        &params![amount, id],
    )
    .await?;
    let row = tx
        .query_one("SELECT bal FROM acct WHERE id = ?", &params![id])
        .await?;
    Ok(row.get::<i64>("bal"))
}

#[durare::workflow]
async fn account(ctx: DurableContext, _: ()) -> Result<i64> {
    setup(&ctx).await?;
    debit(&ctx, 10_i64, 1_i64).await
}

/// The macro'd transactional steps run against a real SQL backend and commit:
/// seed 100, debit 10 → 90.
#[tokio::test]
async fn transaction_macro_runs_a_transactional_step() -> Result<()> {
    let path = std::env::temp_dir().join(format!("durare-txn-macro-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.launch().await?;

    let bal: i64 = engine
        .start_with(Account, (), WorkflowOptions::default())
        .await?
        .await?;
    assert_eq!(bal, 90, "seeded 100, debited 10");

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}
