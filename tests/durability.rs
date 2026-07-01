//! Backend-free tests using the in-memory provider.

use durust::{DurableContext, DurableEngine, Error, InMemoryProvider, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A step's side effect must run exactly once even if the workflow is executed
/// again under the same id (the core durable-execution guarantee).
#[tokio::test]
async fn step_runs_once_across_replays() -> Result<()> {
    static CHARGES: AtomicUsize = AtomicUsize::new(0);

    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("charge", |ctx: DurableContext, _: ()| async move {
        let amount = ctx
            .step("charge_card", || async {
                CHARGES.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(4999_i64)
            })
            .await?;
        Ok::<_, Error>(amount)
    });

    // First execution runs the step.
    let a: i64 = engine.start_typed("charge", "wf-1", ()).await?;
    // Re-executing the same workflow id replays from checkpoints.
    let b: i64 = engine.start_typed("charge", "wf-1", ()).await?;

    assert_eq!(a, 4999);
    assert_eq!(b, 4999);
    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        1,
        "the charge side effect must execute exactly once across replays"
    );
    Ok(())
}

/// A step's *failure* is durable: a workflow that catches a step error and keeps
/// going observes the same recorded error on replay, and the failed step is not
/// re-run — so a non-deterministic step cannot succeed the second time.
#[tokio::test]
async fn caught_step_failure_replays_without_rerunning() -> Result<()> {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    // Errors on the first closure run, would succeed on any later run.
    engine.register("flaky_caught", |ctx: DurableContext, _: ()| async move {
        let r: Result<i64> = ctx
            .step("maybe", || async {
                let n = RUNS.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(Error::app("transient"))
                } else {
                    Ok(7)
                }
            })
            .await;
        Ok::<_, Error>(if r.is_ok() { "ok" } else { "caught-error" }.to_string())
    });

    let a: String = engine
        .start_typed("flaky_caught", "wf-step-err", ())
        .await?;
    // Re-execute the same id: the recorded failure replays without re-running.
    let b: String = engine
        .start_typed("flaky_caught", "wf-step-err", ())
        .await?;

    assert_eq!(a, "caught-error");
    assert_eq!(b, "caught-error", "replay observes the recorded step error");
    assert_eq!(
        RUNS.load(Ordering::SeqCst),
        1,
        "a checkpointed failed step is not re-run on replay"
    );

    // The failure is visible in step introspection.
    let steps = engine.get_workflow_steps("wf-step-err").await?;
    let maybe = steps
        .iter()
        .find(|s| s.name == "maybe")
        .expect("step recorded");
    assert_eq!(maybe.error.as_deref(), Some("transient"));
    Ok(())
}

/// Multiple steps keep their order and individual results across a replay.
#[tokio::test]
async fn multi_step_results_are_stable() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("pipeline", |ctx: DurableContext, start: i64| async move {
        let a = ctx
            .step("double", || async { Ok::<_, Error>(start * 2) })
            .await?;
        let b = ctx
            .step("plus_one", || async { Ok::<_, Error>(a + 1) })
            .await?;
        Ok::<_, Error>(b)
    });

    let out: i64 = engine.start_typed("pipeline", "wf-2", 10_i64).await?;
    assert_eq!(out, 21);

    // Replay yields the identical answer.
    let out2: i64 = engine.start_typed("pipeline", "wf-2", 10_i64).await?;
    assert_eq!(out2, 21);
    Ok(())
}

/// A durable `sleep` is a checkpoint, not a real delay: the first run records the
/// absolute wake instant, so a replay reads it back and returns immediately
/// instead of sleeping again — a workflow that napped before a crash does not
/// re-wait the whole duration on recovery. (Mirrors the other SDKs' durable-sleep
/// recovery guarantee.)
#[tokio::test]
async fn durable_sleep_is_not_repeated_on_replay() -> Result<()> {
    static AFTER_SLEEP: AtomicUsize = AtomicUsize::new(0);
    let nap = Duration::from_millis(500);

    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("napper", move |ctx: DurableContext, _: ()| async move {
        ctx.sleep(nap).await?;
        // A checkpointed step after the sleep, to prove the body resumed past it.
        ctx.step("after_nap", || async {
            AFTER_SLEEP.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(1_i64)
        })
        .await?;
        Ok::<_, Error>(())
    });

    // First execution actually waits out the nap and records the wake instant.
    let t0 = Instant::now();
    engine.start_typed::<_, ()>("napper", "wf-nap", ()).await?;
    let first = t0.elapsed();
    assert!(
        first >= Duration::from_millis(400),
        "first run should really sleep (~500ms), took {first:?}"
    );

    // Replaying the same id reads the stored wake instant — already in the past —
    // so it must return promptly rather than sleeping another ~500ms.
    let t1 = Instant::now();
    engine.start_typed::<_, ()>("napper", "wf-nap", ()).await?;
    let replay = t1.elapsed();
    assert!(
        replay < Duration::from_millis(200),
        "replay must not re-sleep the durable timer, took {replay:?}"
    );

    // The post-sleep step ran exactly once (recorded on the first run, replayed
    // from its checkpoint on the second).
    assert_eq!(
        AFTER_SLEEP.load(Ordering::SeqCst),
        1,
        "the step after the sleep runs once across the replay"
    );
    Ok(())
}
