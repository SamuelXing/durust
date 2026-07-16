//! The metrics snapshot: queue depths from the backend, process-lifetime
//! counters from the engine — each driven by the real event it counts.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepOptions, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Depth counts ENQUEUED rows per registered queue. A queue this process does
/// not listen to holds its work indefinitely, so the count is deterministic;
/// a listened-but-idle queue still appears, at zero, for stable exporter keys.
#[tokio::test]
async fn queue_depth_counts_enqueued_work() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("mq-task", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("obs-mq"));
    engine.register_queue(WorkflowQueue::new("obs-mq-live"));
    engine.listen_queues(["obs-mq-live"]);
    engine.launch().await?;

    for n in 0..3 {
        let opts = WorkflowOptions {
            workflow_id: Some(format!("wf-mq-{n}")),
            queue: Some("obs-mq".into()),
            ..Default::default()
        };
        // Enqueue only — nothing dispatches this queue, so the rows stay put.
        let _ = engine.start::<(), ()>("mq-task", (), opts).await?;
    }

    let m = engine.metrics().await?;
    assert_eq!(m.queue_depth.get("obs-mq"), Some(&3));
    assert_eq!(
        m.queue_depth.get("obs-mq-live"),
        Some(&0),
        "idle but present"
    );
    assert_eq!(m.workflows_in_flight, 0);
    assert_eq!(m.dequeue_errors_total, 0);
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// Each backoff-and-rerun of a retrying step bumps the retry counter — the
/// final failure that exhausts the budget does not.
#[tokio::test]
async fn step_retries_counter_counts_reruns() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("flaky", |ctx: DurableContext, (): ()| async move {
        ctx.step_with(
            StepOptions::new("always-fails")
                .max_retries(2)
                .base_interval(Duration::from_millis(1)),
            || async { Err::<(), _>(Error::app("nope")) },
        )
        .await
    });
    engine.launch().await?;

    let err = engine
        .start::<(), ()>("flaky", (), WorkflowOptions::with_id("wf-retries"))
        .await?
        .await
        .expect_err("retries exhausted");
    assert!(err.to_string().contains("nope"));

    let m = engine.metrics().await?;
    assert_eq!(
        m.step_retries_total, 2,
        "two reruns after the first failure"
    );
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// Recovery re-dispatch and dead-lettering each tick their counter: a stalled
/// workflow recovered once counts 1; a deterministic panic recovered past its
/// attempt cap parks and counts as dead-lettered.
#[tokio::test]
async fn recovery_and_dead_letter_counters() -> Result<()> {
    static STALL: AtomicBool = AtomicBool::new(true);

    // Part 1: a stalled run, recovered to completion.
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("stalls-once", |_ctx: DurableContext, (): ()| async move {
        if STALL.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    let _parked = engine
        .start::<(), ()>("stalls-once", (), WorkflowOptions::with_id("wf-stalled"))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    STALL.store(false, Ordering::SeqCst);
    let recovered = engine
        .recover_pending_for(&[engine.executor_id().to_string()])
        .await?;
    assert_eq!(recovered, vec!["wf-stalled".to_string()]);
    let m = engine.metrics().await?;
    assert_eq!(m.workflows_recovered_total, 1);
    assert_eq!(m.dead_lettered_total, 0);

    // Part 2: a deterministic panic dead-letters at the attempt cap.
    let provider: Arc<InMemoryProvider> = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register("panicky", |_ctx: DurableContext, (): ()| async move {
        panic!("kaboom");
        #[allow(unreachable_code)]
        Ok::<_, Error>(())
    });
    b.max_recovery_attempts(1);
    let engine2 = b.build().await?;
    engine2.launch().await?;

    let first = engine2
        .start::<(), ()>("panicky", (), WorkflowOptions::with_id("wf-panics"))
        .await?
        .await;
    assert!(first.is_err(), "the panic surfaces to the owning caller");

    // Attempt 1: within the cap, re-runs (and panics again).
    let recovered = engine2
        .recover_pending_for(&[engine2.executor_id().to_string()])
        .await?;
    assert_eq!(recovered, vec!["wf-panics".to_string()]);
    // Attempt 2: over the cap — parked, not re-run.
    let recovered = engine2
        .recover_pending_for(&[engine2.executor_id().to_string()])
        .await?;
    assert!(recovered.is_empty(), "parked, not re-dispatched");

    let m = engine2.metrics().await?;
    assert_eq!(m.workflows_recovered_total, 1);
    assert_eq!(m.dead_lettered_total, 1);

    engine.shutdown(Duration::from_secs(2)).await?;
    engine2.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}
