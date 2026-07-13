//! Backend-free tests using the in-memory provider.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, StepOptions,
    WorkflowOptions, STATUS_PENDING, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// F2 — a durable clock/UUID/RNG read is recorded on first execution and
/// replayed identically thereafter: a timestamp, id, or random draw taken in a
/// workflow body survives recovery without breaking determinism. (A bare
/// `Utc::now()` / `Uuid::new_v4()` would silently differ on the replay.)
#[tokio::test]
async fn durable_now_uuid_random_are_stable_across_replays() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("clocked", |ctx: DurableContext, _: ()| async move {
        let now = ctx.now().await?.timestamp_micros();
        let id = ctx.uuid().await?;
        let r = ctx.random().await?;
        Ok::<_, Error>((now, id, r))
    });

    let first = engine
        .start::<(), (i64, String, f64)>("clocked", (), WorkflowOptions::with_id("wf-1"))
        .await?
        .result()
        .await?;
    // Re-execute the same id: the recorded now/uuid/random replay identically.
    let second = engine
        .start::<(), (i64, String, f64)>("clocked", (), WorkflowOptions::with_id("wf-1"))
        .await?
        .result()
        .await?;

    assert_eq!(
        first, second,
        "now/uuid/random must be stable across replays"
    );
    assert!(
        (0.0..1.0).contains(&first.2),
        "random() is in [0, 1): {}",
        first.2
    );
    Ok(())
}

/// Distinct durable-value calls consume distinct seq slots and get independent
/// values, so two `ctx.uuid()` calls in one workflow mint different ids.
#[tokio::test]
async fn durable_uuid_calls_in_one_workflow_are_distinct() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("two_ids", |ctx: DurableContext, _: ()| async move {
        let a = ctx.uuid().await?;
        let b = ctx.uuid().await?;
        Ok::<_, Error>((a, b))
    });

    let (a, b) = engine
        .start::<(), (String, String)>("two_ids", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await?;
    assert_ne!(a, b, "two uuid() calls mint distinct ids");
    assert!(!a.is_empty() && !b.is_empty());
    Ok(())
}

/// F1 — a panic in a workflow body is caught and treated as a *recoverable*
/// failure (like a crash), not a terminal error: the row is left non-terminal so
/// a later `recover()` re-runs it from its checkpoints. A workflow that panics
/// once is recovered to completion. (The default hook prints the panic to stderr;
/// the owning caller observes it as an error.)
#[tokio::test]
async fn workflow_body_panic_is_recoverable() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("panicky", |_ctx: DurableContext, _: ()| async move {
        if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom on the first attempt");
        }
        Ok::<_, Error>(())
    });

    // First execution panics: the owning caller sees an error, but the row is
    // left recoverable (PENDING), not terminally failed.
    let res = engine
        .start::<(), ()>("panicky", (), WorkflowOptions::with_id("wf-panic"))
        .await?
        .result()
        .await;
    assert!(
        res.is_err(),
        "the owning caller observes the panic as an error"
    );
    assert_eq!(
        provider
            .get_workflow_status("wf-panic")
            .await?
            .unwrap()
            .status,
        STATUS_PENDING,
        "a panicked workflow is left recoverable, not terminally failed"
    );

    // recover() re-runs it; the second attempt does not panic and completes.
    assert!(
        engine.recover().await? >= 1,
        "recovery picks up the panicked run"
    );
    assert_eq!(
        provider
            .get_workflow_status("wf-panic")
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS,
        "the recovered run completes"
    );
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        2,
        "panicked once, then recovered"
    );
    Ok(())
}

/// `recover_on_launch(true)` (opt-in): `launch()` recovers this executor's
/// pending workflows in the background, so a crash-and-restart resumes
/// unfinished work with no separate `recover()` call. Opt-in because it is only
/// sound when each live process has a unique executor id.
#[tokio::test]
async fn launch_recovers_pending_workflows_when_opted_in() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let provider = Arc::new(InMemoryProvider::new());
    let register = |engine: &mut DurableEngine| {
        engine.register("crash-once", |_ctx: DurableContext, _: ()| async move {
            if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom on the first attempt");
            }
            Ok::<_, Error>(())
        });
    };

    // First "process": start a workflow that panics on its first attempt, so the
    // row is left recoverable (PENDING). It never launches or recovers.
    {
        let mut engine = DurableEngine::new(provider.clone()).await?;
        register(&mut engine);
        let _ = engine
            .start::<(), ()>(
                "crash-once",
                (),
                WorkflowOptions::with_id("wf-launch-recover"),
            )
            .await?
            .result()
            .await; // observes the panic as an error; the row stays PENDING
    }
    assert_eq!(
        provider
            .get_workflow_status("wf-launch-recover")
            .await?
            .unwrap()
            .status,
        STATUS_PENDING,
        "the panicked workflow is left recoverable"
    );

    // Second "process": a fresh engine over the same database. `launch()` alone —
    // no explicit `recover()` — must resume it, because it opts into
    // recover_on_launch.
    let mut builder = DurableEngine::builder(provider.clone());
    builder.recover_on_launch(true);
    builder.register("crash-once", |_ctx: DurableContext, _: ()| async move {
        if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom on the first attempt");
        }
        Ok::<_, Error>(())
    });
    let engine = builder.build().await?;
    engine.launch().await?;

    // Recovery runs on a background task, so poll for completion.
    let mut status = String::new();
    for _ in 0..100 {
        status = provider
            .get_workflow_status("wf-launch-recover")
            .await?
            .unwrap()
            .status;
        if status == STATUS_SUCCESS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        status, STATUS_SUCCESS,
        "launch() alone recovered the workflow"
    );
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        2,
        "crashed once, then recovered on launch"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// By default (recover_on_launch off): `launch()` does not recover, so a pending
/// workflow stays pending until an explicit `recover()`.
#[tokio::test]
async fn launch_does_not_recover_by_default() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let provider = Arc::new(InMemoryProvider::new());
    {
        let mut engine = DurableEngine::new(provider.clone()).await?;
        engine.register("crash-once-opt", |_ctx: DurableContext, _: ()| async move {
            if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom on the first attempt");
            }
            Ok::<_, Error>(())
        });
        let _ = engine
            .start::<(), ()>("crash-once-opt", (), WorkflowOptions::with_id("wf-opt-out"))
            .await?
            .result()
            .await;
    }

    // launch() with the default (recovery off) leaves the pending row untouched.
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("crash-once-opt", |_ctx: DurableContext, _: ()| async move {
        if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom on the first attempt");
        }
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    // Give any (unexpected) background recovery a chance to run.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        provider
            .get_workflow_status("wf-opt-out")
            .await?
            .unwrap()
            .status,
        STATUS_PENDING,
        "recovery is off, so the workflow stays pending"
    );

    // An explicit recover() still works.
    assert!(engine.recover().await? >= 1, "manual recovery picks it up");
    assert_eq!(
        provider
            .get_workflow_status("wf-opt-out")
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS,
        "the manually recovered run completes"
    );
    Ok(())
}

/// The graceful-shutdown contract holds for launch recovery: a run the
/// launch-recovery task re-dispatched counts as in-flight, so `shutdown` waits
/// it out rather than returning while it is mid-run.
#[tokio::test]
async fn shutdown_drains_a_launch_recovered_run() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static RUNNING: AtomicBool = AtomicBool::new(false);

    // First "process": leave a PENDING run behind.
    let provider = Arc::new(InMemoryProvider::new());
    {
        let mut engine = DurableEngine::new(provider.clone()).await?;
        engine.register("drain-probe", |_ctx: DurableContext, _: ()| async move {
            if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("boom on the first attempt");
            }
            RUNNING.store(true, Ordering::SeqCst);
            // Long enough that a shutdown which doesn't drain would return
            // while this run is still going.
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok::<_, Error>(())
        });
        let _ = engine
            .start::<(), ()>("drain-probe", (), WorkflowOptions::with_id("wf-drain"))
            .await?
            .result()
            .await;
    }

    // Second "process" opts in; launch() re-dispatches the run in the background.
    let mut builder = DurableEngine::builder(provider.clone());
    builder.recover_on_launch(true);
    builder.register("drain-probe", |_ctx: DurableContext, _: ()| async move {
        if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("boom on the first attempt");
        }
        RUNNING.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(400)).await;
        Ok::<_, Error>(())
    });
    let engine = builder.build().await?;
    engine.launch().await?;

    // Wait until the recovered run is genuinely mid-flight...
    for _ in 0..500 {
        if RUNNING.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(RUNNING.load(Ordering::SeqCst), "the recovered run started");

    // ...then shut down. Draining means the run finished (SUCCESS) by the time
    // shutdown returns; without the drain guard, shutdown would return
    // immediately and the row would still be PENDING here.
    engine.shutdown(Duration::from_secs(10)).await?;
    assert_eq!(
        provider
            .get_workflow_status("wf-drain")
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS,
        "shutdown waited for the launch-recovered run to finish"
    );
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        2,
        "crashed once, then recovered on launch"
    );
    Ok(())
}

/// F1 refinement — a panic in a step body is caught and turned into a step
/// error, so it is subject to the step's retry policy: a step that panics once
/// succeeds on retry (rather than failing the whole workflow immediately).
#[tokio::test]
async fn step_panic_is_caught_and_retried() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("retry_panic", |ctx: DurableContext, _: ()| async move {
        ctx.step_with(
            StepOptions::new("flaky")
                .max_retries(3)
                .base_interval(Duration::from_millis(1)),
            || async {
                if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("first attempt panics");
                }
                Ok::<_, Error>(42_i64)
            },
        )
        .await
    });

    let out: i64 = engine
        .start::<(), i64>("retry_panic", (), WorkflowOptions::with_id("wf-retry"))
        .await?
        .result()
        .await?;
    assert_eq!(out, 42);
    assert_eq!(
        ATTEMPTS.load(Ordering::SeqCst),
        2,
        "the step panicked once, then succeeded on retry"
    );
    Ok(())
}
