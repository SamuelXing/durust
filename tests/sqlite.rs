//! SQLite backend tests: durable state and crash-recovery across "restarts"
//! (separate engine + provider instances over the same database file).

use durust::{
    DurableContext, DurableEngine, Error, Result, SqliteProvider, WorkflowOptions, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A unique temp file path for an isolated SQLite database per test.
fn temp_db_url(tag: &str) -> (String, std::path::PathBuf) {
    let mut p = std::env::temp_dir();
    let unique = format!("durust-{tag}-{}.db", uuid::Uuid::new_v4());
    p.push(unique);
    (format!("sqlite://{}", p.display()), p)
}

#[tokio::test]
async fn sqlite_persists_and_runs_workflow() -> Result<()> {
    let (url, path) = temp_db_url("basic");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("add_one", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });

    let mut handle = engine
        .run_workflow::<_, i64>("add_one", 41_i64, WorkflowOptions::with_id("wf-sqlite-1"))
        .await?;
    assert_eq!(handle.get_result().await?, 42);
    assert_eq!(handle.get_status().await?.status, STATUS_SUCCESS);

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A step's side effect runs exactly once even across a simulated restart: a
/// fresh engine/provider over the same file replays the checkpointed step.
#[tokio::test]
async fn sqlite_recovers_across_restart() -> Result<()> {
    static CHARGES: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("recover");

    // "Process 1": run the workflow to completion, charging once.
    {
        let mut engine =
            DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register("charge", |ctx: DurableContext, _: ()| async move {
            let amt = ctx
                .step("charge_card", || async {
                    CHARGES.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(4999_i64)
                })
                .await?;
            Ok::<_, Error>(amt)
        });
        let out: i64 = engine.start_typed("charge", "wf-charge", ()).await?;
        assert_eq!(out, 4999);
    }

    // "Process 2": a brand-new engine/provider over the same file. Re-running
    // the same id replays the checkpoint — the card is NOT charged again.
    {
        let mut engine =
            DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register("charge", |ctx: DurableContext, _: ()| async move {
            let amt = ctx
                .step("charge_card", || async {
                    CHARGES.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(4999_i64)
                })
                .await?;
            Ok::<_, Error>(amt)
        });
        let out: i64 = engine.start_typed("charge", "wf-charge", ()).await?;
        assert_eq!(out, 4999);
    }

    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        1,
        "charge side effect must run exactly once across a restart"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}
