//! SQLite backend tests: durable state and crash-recovery across "restarts"
//! (separate engine + provider instances over the same database file).

use durust::{
    DurableContext, DurableEngine, Error, Result, SqliteProvider, WorkflowOptions, WorkflowQueue,
    STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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

/// The SQL dequeue path end to end: enqueue → dispatcher claims → result; and
/// the (queue_name, deduplication_id) unique index rejects a duplicate.
#[tokio::test]
async fn sqlite_queue_dispatch_and_dedup() -> Result<()> {
    let (url, path) = temp_db_url("queue");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("double", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * 2)
    });
    engine.register_queue(
        WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;

    let mut opts = WorkflowOptions::with_id("wf-q-1");
    opts.dedup_id = Some("only-once".to_string());
    let mut handle = engine.enqueue::<_, i64>("q", "double", 21_i64, opts).await?;
    assert_eq!(handle.get_result().await?, 42);

    // Different workflow id, same dedup id on the same queue → unique index
    // violation from the INSERT.
    let mut opts = WorkflowOptions::with_id("wf-q-2");
    opts.dedup_id = Some("only-once".to_string());
    let dup = engine.enqueue::<_, i64>("q", "double", 1_i64, opts).await;
    assert!(dup.is_err(), "dedup id reuse on the same queue must be rejected");

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}
