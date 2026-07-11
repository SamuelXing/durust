//! Backend-free tests using the in-memory provider.

use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
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
    let a: i64 = engine
        .start("charge", (), WorkflowOptions::with_id("wf-1"))
        .await?
        .result()
        .await?;
    // Re-executing the same workflow id replays from checkpoints.
    let b: i64 = engine
        .start("charge", (), WorkflowOptions::with_id("wf-1"))
        .await?
        .result()
        .await?;

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
        .start("flaky_caught", (), WorkflowOptions::with_id("wf-step-err"))
        .await?
        .result()
        .await?;
    // Re-execute the same id: the recorded failure replays without re-running.
    let b: String = engine
        .start("flaky_caught", (), WorkflowOptions::with_id("wf-step-err"))
        .await?
        .result()
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

    let out: i64 = engine
        .start("pipeline", 10_i64, WorkflowOptions::with_id("wf-2"))
        .await?
        .result()
        .await?;
    assert_eq!(out, 21);

    // Replay yields the identical answer.
    let out2: i64 = engine
        .start("pipeline", 10_i64, WorkflowOptions::with_id("wf-2"))
        .await?
        .result()
        .await?;
    assert_eq!(out2, 21);
    Ok(())
}

/// Re-submitting a completed workflow that slept returns its result immediately
/// and does not run the body again — so it never sleeps a second time. The first
/// run really waits out the nap and completes; a second `start` under the same id
/// is short-circuited by once-and-only-once completion (the terminal output is
/// returned by polling), so the post-sleep step also stays at a single execution.
///
/// This exercises the OAOO completion guarantee, not mid-flight durable-sleep
/// replay: the body is not re-executed here, so the recorded wake instant is not
/// re-read. (Exercising the sleep checkpoint on replay would require forcing a
/// re-run of a still-`PENDING` workflow, as the recovery tests do.)
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
    engine
        .start::<_, ()>("napper", (), WorkflowOptions::with_id("wf-nap"))
        .await?
        .result()
        .await?;
    let first = t0.elapsed();
    assert!(
        first >= Duration::from_millis(400),
        "first run should really sleep (~500ms), took {first:?}"
    );

    // Re-submitting the same id: the workflow is already terminal, so it returns
    // the stored output at once instead of re-running the body (and re-sleeping).
    let t1 = Instant::now();
    engine
        .start::<_, ()>("napper", (), WorkflowOptions::with_id("wf-nap"))
        .await?
        .result()
        .await?;
    let resubmit = t1.elapsed();
    assert!(
        resubmit < Duration::from_millis(200),
        "a completed workflow must not re-sleep on resubmit, took {resubmit:?}"
    );

    // The post-sleep step ran exactly once: recorded on the first run, and the
    // body was not executed again on the resubmit.
    assert_eq!(
        AFTER_SLEEP.load(Ordering::SeqCst),
        1,
        "the step after the sleep runs once; the resubmit does not re-run the body"
    );
    Ok(())
}
