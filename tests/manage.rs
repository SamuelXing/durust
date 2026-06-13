//! Workflow management tests: retrieve, list (filters), cancel, resume, fork,
//! and version-gated recovery. Backend-free (in-memory provider).

use durust::{
    DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result, StateProvider,
    WorkflowOptions, WorkflowStatus, STATUS_CANCELLED, STATUS_PENDING, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// retrieve_workflow returns a handle to an already-run workflow; list_workflows
/// filters by name and status.
#[tokio::test]
async fn retrieve_and_list() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("add", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });

    engine.start_typed::<_, i64>("add", "wf-a", 1_i64).await?;
    engine.start_typed::<_, i64>("add", "wf-b", 2_i64).await?;

    let mut h = engine.retrieve_workflow::<i64>("wf-a").await?;
    assert_eq!(h.get_result().await?, 2);

    assert!(engine.retrieve_workflow::<i64>("ghost").await.is_err());

    let all = engine
        .list_workflows(&ListFilter {
            name: Some("add".to_string()),
            ..Default::default()
        })
        .await?;
    assert_eq!(all.len(), 2);

    let done = engine
        .list_workflows(&ListFilter {
            status: vec![STATUS_SUCCESS.to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(done.len(), 2);

    // Prefix + limit.
    let one = engine
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("wf-".to_string()),
            limit: Some(1),
            ..Default::default()
        })
        .await?;
    assert_eq!(one.len(), 1);
    Ok(())
}

/// A cancelled workflow refuses further steps; resume re-runs it from its
/// checkpoints (the already-recorded step is not re-executed).
#[tokio::test]
async fn cancel_then_resume() -> Result<()> {
    static STEP_RUNS: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;

    // Workflow: record step 0, then (on first run) observe cancellation at step 1.
    engine.register("two_step", |ctx: DurableContext, _: ()| async move {
        let a = ctx
            .step("first", || async {
                STEP_RUNS.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(1_i64)
            })
            .await?;
        let b = ctx
            .step("second", || async { Ok::<_, Error>(a + 1) })
            .await?;
        Ok::<_, Error>(b)
    });

    // Seed a PENDING workflow with step 0 already checkpointed, then cancel it
    // so the next execution stops cooperatively.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-cancel",
            "two_step",
            serde_json::Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;
    provider
        .record_step_result("wf-cancel", 0, "first", serde_json::json!(1))
        .await?;
    engine.cancel_workflow("wf-cancel").await?;
    assert_eq!(
        engine
            .retrieve_workflow::<i64>("wf-cancel")
            .await?
            .get_status()
            .await?
            .status,
        STATUS_CANCELLED
    );

    // Resume re-runs from checkpoints: step 0 is replayed (not re-run), step 1
    // proceeds, workflow completes.
    let mut h = engine.resume_workflow::<i64>("wf-cancel").await?;
    assert_eq!(h.get_result().await?, 2);
    assert_eq!(
        STEP_RUNS.load(Ordering::SeqCst),
        0,
        "the checkpointed step must be replayed, not re-executed"
    );

    // Resuming a completed workflow is an error.
    assert!(engine.resume_workflow::<i64>("wf-cancel").await.is_err());
    Ok(())
}

/// fork_workflow reuses checkpoints before start_step and re-executes the rest.
#[tokio::test]
async fn fork_reuses_checkpoints() -> Result<()> {
    static SECOND_RUNS: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;

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

    // Original run completes; both steps execute once.
    let orig: i64 = engine.start_typed("pipeline", "wf-orig", ()).await?;
    assert_eq!(orig, 15);
    assert_eq!(SECOND_RUNS.load(Ordering::SeqCst), 1);

    // Fork from step 1: step 0 ("first") is reused, step 1 re-executes.
    let mut forked = engine
        .fork_workflow::<i64>("wf-orig", 1, WorkflowOptions::with_id("wf-fork"))
        .await?;
    assert_eq!(forked.get_result().await?, 15);
    assert_eq!(
        SECOND_RUNS.load(Ordering::SeqCst),
        2,
        "fork must re-execute steps at/after start_step"
    );

    let row = provider.get_workflow_status("wf-fork").await?.unwrap();
    assert_eq!(row.forked_from.as_deref(), Some("wf-orig"));
    Ok(())
}

/// recover() only re-runs workflows of the engine's application version.
#[tokio::test]
async fn recover_is_version_gated() -> Result<()> {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new_with_version(provider.clone(), "v2").await?;
    engine.register("w", |_ctx: DurableContext, _: ()| async move {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });

    // One PENDING workflow of the current version, one of an old version.
    for (id, ver) in [("wf-cur", "v2"), ("wf-old", "v1")] {
        provider
            .insert_workflow_status(WorkflowStatus::new(
                id,
                "w",
                serde_json::Value::Null,
                STATUS_PENDING,
                "",
                ver,
            ))
            .await?;
    }

    let n = engine.recover().await?;
    assert_eq!(n, 1, "only the matching-version workflow is recovered");
    assert_eq!(RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider
            .get_workflow_status("wf-old")
            .await?
            .unwrap()
            .status,
        STATUS_PENDING,
        "the old-version workflow is left untouched"
    );
    Ok(())
}

/// A workflow recovered past the attempt cap is parked in
/// MAX_RECOVERY_ATTEMPTS_EXCEEDED rather than re-run forever.
#[tokio::test]
async fn recover_caps_attempts() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(
        "always_panic_ish",
        |ctx: DurableContext, _: ()| async move {
            // Never completes successfully: errors every attempt so it stays
            // recoverable... except we cap it.
            ctx.step("boom", || async { Err::<(), _>(Error::app("nope")) })
                .await
        },
    );

    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-loop",
            "always_panic_ish",
            serde_json::Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;

    // Bump the recovery_attempts to the cap, then set back to PENDING so the
    // next recover() pushes it over the edge.
    for _ in 0..100 {
        provider.bump_recovery_attempts("wf-loop", 100).await?;
    }
    provider
        .set_workflow_status("wf-loop", STATUS_PENDING, None, None)
        .await?;

    engine.recover().await?;
    assert_eq!(
        provider
            .get_workflow_status("wf-loop")
            .await?
            .unwrap()
            .status,
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    );
    Ok(())
}

/// Cancelling a queued workflow removes it from the queue (it never runs).
#[tokio::test]
async fn cancel_removes_from_queue() -> Result<()> {
    use durust::WorkflowQueue;
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));

    // Enqueue but cancel before launching the dispatcher.
    let handle = engine
        .enqueue::<_, ()>("q", "noop", (), WorkflowOptions::with_id("wf-q"))
        .await?;
    engine.cancel_workflow("wf-q").await?;
    assert_eq!(handle.get_status().await?.status, STATUS_CANCELLED);
    assert_eq!(handle.get_status().await?.queue_name, None);
    Ok(())
}
