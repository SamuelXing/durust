//! SQLite backend tests: durable state and crash-recovery across "restarts"
//! (separate engine + provider instances over the same database file).

use durust::{
    DurableContext, DurableEngine, Error, ListFilter, Result, SqliteProvider, WorkflowOptions,
    WorkflowQueue, STATUS_CANCELLED, STATUS_SUCCESS,
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
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
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
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
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
    engine.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));
    engine.launch().await?;

    let mut opts = WorkflowOptions::with_id("wf-q-1");
    opts.dedup_id = Some("only-once".to_string());
    let mut handle = engine
        .enqueue::<_, i64>("q", "double", 21_i64, opts)
        .await?;
    assert_eq!(handle.get_result().await?, 42);

    // Different workflow id, same dedup id on the same queue → unique index
    // violation from the INSERT.
    let mut opts = WorkflowOptions::with_id("wf-q-2");
    opts.dedup_id = Some("only-once".to_string());
    let dup = engine.enqueue::<_, i64>("q", "double", 1_i64, opts).await;
    assert!(
        dup.is_err(),
        "dedup id reuse on the same queue must be rejected"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Messaging on the SQL backend: send/recv FIFO via the atomic consume,
/// set_event/get_event, and the FK rejecting sends to missing workflows.
#[tokio::test]
async fn sqlite_messaging_and_events() -> Result<()> {
    let (url, path) = temp_db_url("comm");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("exchange", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("phase", "waiting").await?;
        let a: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        let b: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(format!(
            "{},{}",
            a.unwrap_or_default(),
            b.unwrap_or_default()
        ))
    });

    let mut handle = engine
        .run_workflow::<_, String>("exchange", (), WorkflowOptions::with_id("wf-comm"))
        .await?;

    // The event is readable while the workflow is still waiting in recv.
    let phase: Option<String> = engine
        .get_event("wf-comm", "phase", Duration::from_secs(2))
        .await?;
    assert_eq!(phase.as_deref(), Some("waiting"));

    engine.send("wf-comm", "m1".to_string(), "t").await?;
    engine.send("wf-comm", "m2".to_string(), "t").await?;
    assert_eq!(handle.get_result().await?, "m1,m2");

    // FK on destination_uuid: sending to a missing workflow errors.
    assert!(engine.send("ghost", "boo".to_string(), "t").await.is_err());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Management on the SQL backend: list filters (QueryBuilder), cancel/resume,
/// and fork copying step checkpoints.
#[tokio::test]
async fn sqlite_management() -> Result<()> {
    let (url, path) = temp_db_url("manage");
    static SECOND_RUNS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("pipeline", |ctx: DurableContext, _: ()| async move {
        let a = ctx
            .step("first", || async { Ok::<_, Error>(10_i64) })
            .await?;
        let b = ctx
            .step("second", || async {
                SECOND_RUNS.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(a + 5)
            })
            .await?;
        Ok::<_, Error>(b)
    });

    engine.start_typed::<_, i64>("pipeline", "wf-1", ()).await?;

    // list filters via QueryBuilder.
    let listed = engine
        .list_workflows(&ListFilter {
            name: Some("pipeline".to_string()),
            status: vec![STATUS_SUCCESS.to_string()],
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "wf-1");

    // Fork from step 1: step 0 reused, step 1 re-runs (SQL copy of operation_outputs).
    let mut forked = engine
        .fork_workflow::<i64>("wf-1", 1, WorkflowOptions::with_id("wf-fork"))
        .await?;
    assert_eq!(forked.get_result().await?, 15);
    assert_eq!(SECOND_RUNS.load(Ordering::SeqCst), 2);
    let frow = engine.retrieve_workflow::<i64>("wf-fork").await?;
    assert_eq!(
        frow.get_status().await?.forked_from.as_deref(),
        Some("wf-1")
    );

    // Cancel a fresh pending workflow, then resume re-runs it to completion.
    engine
        .run_workflow::<_, i64>("pipeline", (), WorkflowOptions::with_id("wf-2"))
        .await?
        .get_result()
        .await?;
    engine.cancel_workflow("wf-2").await?;
    // Already terminal (SUCCESS) → cancel is a no-op; resume errors.
    assert!(engine.resume_workflow::<i64>("wf-2").await.is_err());

    // A genuinely cancellable workflow.
    use durust::{StateProvider, WorkflowStatus, STATUS_PENDING};
    let provider = SqliteProvider::connect(&url).await?;
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-3",
            "pipeline",
            serde_json::Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;
    engine.cancel_workflow("wf-3").await?;
    assert_eq!(
        engine
            .retrieve_workflow::<i64>("wf-3")
            .await?
            .get_status()
            .await?
            .status,
        STATUS_CANCELLED
    );
    let mut resumed = engine.resume_workflow::<i64>("wf-3").await?;
    assert_eq!(resumed.get_result().await?, 15);

    let _ = std::fs::remove_file(path);
    Ok(())
}
