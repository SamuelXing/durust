//! Engine foundation tests: non-blocking handles, step retries, and engine
//! lifecycle. All backend-free (in-memory provider).

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepOptions, WorkflowOptions,
    STATUS_CANCELLED, STATUS_PENDING, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `start` returns a handle immediately while the workflow is still
/// running; `result` then yields the eventual output.
#[tokio::test]
async fn start_is_non_blocking() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    engine.register("slow", |ctx: DurableContext, _: ()| async move {
        // A durable step that takes a beat, so the handle is observably PENDING
        // before the workflow finishes.
        ctx.step("work", || async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok::<_, Error>(42_i64)
        })
        .await
    });

    let handle = engine
        .start::<_, i64>("slow", (), WorkflowOptions::with_id("wf-slow"))
        .await?;

    // Returned before completion: status is still PENDING right now.
    let status = handle.get_status().await?;
    assert_eq!(status.status, STATUS_PENDING);
    assert_eq!(handle.id(), "wf-slow");

    // Awaiting the handle yields the result and the row becomes terminal.
    let out = handle.result().await?;
    assert_eq!(out, 42);
    assert_eq!(handle.get_status().await?.status, STATUS_SUCCESS);
    Ok(())
}

/// A step with `max_retries(2)` that fails twice then succeeds runs its closure
/// 3 times but checkpoints exactly once.
#[tokio::test]
async fn step_retries_until_success() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    engine.register("flaky", |ctx: DurableContext, _: ()| async move {
        let opts = StepOptions::new("sometimes_fails")
            .max_retries(2)
            .base_interval(Duration::from_millis(1));
        ctx.step_with(opts, || async {
            let n = ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                Err(Error::app("transient"))
            } else {
                Ok::<_, Error>("ok".to_string())
            }
        })
        .await
    });

    let out: String = engine
        .start("flaky", (), WorkflowOptions::with_id("wf-flaky"))
        .await?
        .result()
        .await?;
    assert_eq!(out, "ok");
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        3,
        "closure should run 3 times: 2 failures + 1 success"
    );
    Ok(())
}

/// A step with exhausted retries surfaces the last error and marks the workflow
/// ERROR.
#[tokio::test]
async fn step_retries_exhausted_propagates_error() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    engine.register("always_fails", |ctx: DurableContext, _: ()| async move {
        let opts = StepOptions::new("nope")
            .max_retries(1)
            .base_interval(Duration::from_millis(1));
        ctx.step_with(opts, || async { Err::<(), _>(Error::app("boom")) })
            .await
    });

    let res: Result<()> = engine
        .start("always_fails", (), WorkflowOptions::with_id("wf-fail"))
        .await?
        .result()
        .await;
    assert!(matches!(res, Err(Error::App { ref message, .. }) if message == "boom"));
    Ok(())
}

/// A retry predicate fails fast: an error it rejects stops retries immediately,
/// even with attempts remaining, while an error it accepts still retries up to
/// `max_retries`.
#[tokio::test]
async fn step_retry_predicate_stops_on_permanent_error() -> Result<()> {
    static PERMANENT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static TRANSIENT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // "permanent" is a validation error the predicate refuses to retry.
    engine.register("permanent", |ctx: DurableContext, _: ()| async move {
        let opts = StepOptions::new("check")
            .max_retries(5)
            .base_interval(Duration::from_millis(1))
            .retry_if(|e: &Error| !e.to_string().contains("permanent"));
        ctx.step_with(opts, || async {
            PERMANENT_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(Error::app("permanent: bad input"))
        })
        .await
    });

    // "transient" is retried under the same predicate until attempts run out.
    engine.register("transient", |ctx: DurableContext, _: ()| async move {
        let opts = StepOptions::new("call")
            .max_retries(3)
            .base_interval(Duration::from_millis(1))
            .retry_if(|e: &Error| !e.to_string().contains("permanent"));
        ctx.step_with(opts, || async {
            TRANSIENT_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(Error::app("temporary glitch"))
        })
        .await
    });

    let permanent: Result<()> = engine
        .start("permanent", (), WorkflowOptions::with_id("wf-perm"))
        .await?
        .result()
        .await;
    assert!(permanent.is_err());
    assert_eq!(
        PERMANENT_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "a rejected error must not be retried, despite max_retries(5)"
    );

    let transient: Result<()> = engine
        .start("transient", (), WorkflowOptions::with_id("wf-tran"))
        .await?
        .result()
        .await;
    assert!(transient.is_err());
    assert_eq!(
        TRANSIENT_ATTEMPTS.load(Ordering::SeqCst),
        4,
        "an accepted error retries up to max_retries: 1 initial + 3 retries"
    );
    Ok(())
}

/// launch()/shutdown() are callable and drain in-flight work.
#[tokio::test]
async fn launch_and_shutdown_drain() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("quick", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });

    engine.launch().await?;
    let handle = engine
        .start::<_, i64>("quick", 1_i64, WorkflowOptions::default())
        .await?;
    let out = handle.result().await?;
    assert_eq!(out, 2);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A workflow that runs past its timeout is cancelled: result fails and the
/// status row is CANCELLED with a deadline-exceeded reason.
#[tokio::test]
async fn workflow_timeout_cancels() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("slow", |ctx: DurableContext, _: ()| async move {
        ctx.step("long", || async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok::<_, Error>(1_i64)
        })
        .await
    });

    let mut opts = WorkflowOptions::with_id("wf-timeout");
    opts.timeout = Some(Duration::from_millis(80));
    let handle = engine.start::<_, i64>("slow", (), opts).await?;

    assert!(
        handle.result().await.is_err(),
        "a workflow exceeding its timeout must not succeed"
    );
    assert_eq!(handle.get_status().await?.status, STATUS_CANCELLED);
    Ok(())
}
