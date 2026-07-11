//! Transactional steps. The in-memory backend has no SQL transaction, so a
//! transactional step is rejected there — the feature requires the SQLite or
//! Postgres backend (exercised in `tests/sqlite.rs` and `tests/postgres.rs`).

use durare::{
    params, DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions,
};
use std::sync::Arc;

/// `ctx.transaction` errors on the in-memory provider rather than silently
/// running without atomicity.
#[tokio::test]
async fn transaction_step_unsupported_in_memory() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("t", |ctx: DurableContext, _: ()| async move {
        let n: i64 = ctx
            .transaction("noop", |tx| {
                Box::pin(async move {
                    tx.execute("SELECT 1", &params![]).await?;
                    Ok(1_i64)
                })
            })
            .await?;
        Ok::<_, Error>(n)
    });
    let res: Result<i64> = engine
        .start("t", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await;
    assert!(
        res.is_err(),
        "transactional steps require a SQL backend (Postgres or SQLite)"
    );
    Ok(())
}
