//! SQLite backend tests: durable state and crash-recovery across "restarts"
//! (separate engine + provider instances over the same database file).

use durust::{
    DurableContext, DurableEngine, Error, ListFilter, Result, ScheduledInput, SqliteProvider,
    TransactionOptions, WorkflowOptions, WorkflowQueue, STATUS_CANCELLED, STATUS_SUCCESS,
};
use std::future::Future;
use std::pin::Pin;
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

/// A step's *failure* is checkpointed: a workflow that catches a step error and
/// keeps going observes the **same** recorded error on replay, and the failed
/// step is not re-run — so a non-deterministic step cannot silently succeed the
/// second time. Without error checkpointing the step would re-run (here, succeed)
/// and replay would diverge.
#[tokio::test]
async fn sqlite_checkpoints_a_caught_step_failure() -> Result<()> {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("step-err");

    // The step errors the first time its closure runs, but would succeed on any
    // later run — so re-running on replay would change the outcome.
    let register = |engine: &mut DurableEngine| {
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
            // Catch the step error and report which branch we took, so a divergent
            // replay would surface as a different workflow output.
            Ok::<_, Error>(if r.is_ok() { "ok" } else { "caught-error" }.to_string())
        });
    };

    // Process 1: the step fails, the workflow catches it and completes.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine
            .start_typed("flaky_caught", "wf-step-err", ())
            .await?;
        assert_eq!(out, "caught-error");

        // The failed step is recorded with its error.
        let steps = engine.get_workflow_steps("wf-step-err").await?;
        let maybe = steps
            .iter()
            .find(|s| s.name == "maybe")
            .expect("step recorded");
        assert_eq!(maybe.error.as_deref(), Some("transient"));
        assert!(maybe.output.is_none());
    }

    // Process 2: a fresh engine over the same file replays. The recorded failure
    // is returned without re-running the step.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine
            .start_typed("flaky_caught", "wf-step-err", ())
            .await?;
        assert_eq!(
            out, "caught-error",
            "replay must observe the recorded error"
        );
    }

    assert_eq!(
        RUNS.load(Ordering::SeqCst),
        1,
        "a checkpointed failed step is not re-run on replay"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A patch decision is durable: replaying the workflow re-reads the marker and
/// stays on the new path, and the post-patch step runs exactly once.
#[tokio::test]
async fn sqlite_patch_is_durable_across_replay() -> Result<()> {
    static WORK_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("patch");

    let register = |engine: &mut DurableEngine| {
        engine.register("wf", |ctx: DurableContext, _: ()| async move {
            let patched = ctx.patch("feat").await?;
            let v = ctx
                .step("work", || async {
                    WORK_RUNS.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, Error>(10_i64)
                })
                .await?;
            Ok::<_, Error>((patched, v))
        });
    };

    for _ in 0..2 {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let (patched, v): (bool, i64) = engine.start_typed("wf", "w", ()).await?;
        assert!(
            patched,
            "the new path is taken on the first run and on replay"
        );
        assert_eq!(v, 10);
    }

    assert_eq!(
        WORK_RUNS.load(Ordering::Relaxed),
        1,
        "the post-patch step runs exactly once across the replay"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Deprecating a patch keeps step numbering aligned: a run that recorded the
/// patch consumes the marker's slot under the new (deprecated) code, so the
/// following step still replays instead of re-running.
#[tokio::test]
async fn sqlite_deprecate_patch_keeps_alignment() -> Result<()> {
    static WORK_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("deprecate");

    // Phase A — code carrying the patch: records the marker at seq 0, "work" at 1.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register("wf", |ctx: DurableContext, _: ()| async move {
            let _ = ctx.patch("feat").await?;
            let v = ctx
                .step("work", || async {
                    WORK_RUNS.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, Error>(5_i64)
                })
                .await?;
            Ok::<_, Error>(v)
        });
        let v: i64 = engine.start_typed("wf", "w", ()).await?;
        assert_eq!(v, 5);
    }

    // Phase B — patch deprecated: deprecate_patch consumes the recorded marker's
    // slot, so "work" stays at seq 1 and replays rather than re-running.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register("wf", |ctx: DurableContext, _: ()| async move {
            ctx.deprecate_patch("feat").await?;
            let v = ctx
                .step("work", || async {
                    WORK_RUNS.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, Error>(5_i64)
                })
                .await?;
            Ok::<_, Error>(v)
        });
        let v: i64 = engine.start_typed("wf", "w", ()).await?;
        assert_eq!(v, 5);
    }

    assert_eq!(
        WORK_RUNS.load(Ordering::Relaxed),
        1,
        "work runs once: deprecate_patch consumed the marker slot, keeping seq aligned"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Fan-out is durable: two steps run concurrently via `try_join!`, each
/// checkpoints independently, and a replay re-runs neither.
#[tokio::test]
async fn sqlite_concurrent_steps_are_durable() -> Result<()> {
    static A_RUNS: AtomicUsize = AtomicUsize::new(0);
    static B_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("fanout");

    let register = |engine: &mut DurableEngine| {
        engine.register("fanout", |ctx: DurableContext, _: ()| async move {
            let (a, b) = tokio::try_join!(
                ctx.step("a", || async {
                    A_RUNS.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, Error>(1_i64)
                }),
                ctx.step("b", || async {
                    B_RUNS.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, Error>(2_i64)
                }),
            )?;
            Ok::<_, Error>(a + b)
        });
    };

    for _ in 0..2 {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let v: i64 = engine.start_typed("fanout", "f", ()).await?;
        assert_eq!(v, 3);
    }

    assert_eq!(
        A_RUNS.load(Ordering::Relaxed),
        1,
        "step a runs once across replay"
    );
    assert_eq!(
        B_RUNS.load(Ordering::Relaxed),
        1,
        "step b runs once across replay"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A `select` winner is durable: replay returns the recorded outcome without
/// re-running any branch.
#[tokio::test]
async fn sqlite_select_winner_is_durable() -> Result<()> {
    static FAST_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("select");

    let register = |engine: &mut DurableEngine| {
        engine.register("racer", |ctx: DurableContext, _: ()| async move {
            let branches: Vec<Pin<Box<dyn Future<Output = i64> + Send>>> = vec![
                Box::pin(async {
                    FAST_RUNS.fetch_add(1, Ordering::Relaxed);
                    2_i64
                }),
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    1_i64
                }),
            ];
            ctx.select(branches).await
        });
    };

    for _ in 0..2 {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let (index, value): (usize, i64) = engine.start_typed("racer", "r", ()).await?;
        assert_eq!((index, value), (0, 2));
    }

    assert_eq!(
        FAST_RUNS.load(Ordering::Relaxed),
        1,
        "the winning branch runs once; replay reads the recorded outcome"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A durable stream round-trips through SQLite and survives replay: re-running
/// the producer over the same database does not re-append, so the reader still
/// sees exactly the values written once.
#[tokio::test]
async fn sqlite_stream_is_durable_across_replay() -> Result<()> {
    let (url, path) = temp_db_url("stream");

    let register = |engine: &mut DurableEngine| {
        engine.register("producer", |ctx: DurableContext, _: ()| async move {
            for i in 0..3_i64 {
                ctx.write_stream("nums", i).await?;
            }
            ctx.close_stream("nums").await?;
            Ok::<_, Error>(())
        });
    };

    for _ in 0..2 {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        engine.start_typed::<_, ()>("producer", "p", ()).await?;

        let (values, closed): (Vec<i64>, bool) = engine.read_stream("p", "nums").await?;
        assert_eq!(
            values,
            vec![0, 1, 2],
            "stream holds each value exactly once"
        );
        assert!(closed);
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Writing to a closed stream errors at the SQL layer too.
#[tokio::test]
async fn sqlite_write_to_closed_stream_errors() -> Result<()> {
    let (url, path) = temp_db_url("stream-closed");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("bad", |ctx: DurableContext, _: ()| async move {
        ctx.close_stream("s").await?;
        ctx.write_stream("s", 1_i64).await?;
        Ok::<_, Error>(())
    });

    let res: Result<()> = engine.start_typed("bad", "p", ()).await;
    assert!(res.is_err(), "writing after close must fail");

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

    // Enqueue before launching the dispatcher so wf-q-1 still holds its dedup
    // slot (it is ENQUEUED, not yet completed) when the duplicate is attempted.
    let mut opts = WorkflowOptions::with_id("wf-q-1");
    opts.dedup_id = Some("only-once".to_string());
    let mut handle = engine
        .enqueue::<_, i64>("q", "double", 21_i64, opts)
        .await?;

    // Different workflow id, same dedup id on the same queue → unique index
    // violation from the INSERT while wf-q-1 is still active.
    let mut opts = WorkflowOptions::with_id("wf-q-2");
    opts.dedup_id = Some("only-once".to_string());
    let err = match engine.enqueue::<_, i64>("q", "double", 1_i64, opts).await {
        Ok(_) => panic!("dedup id reuse on the same queue must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), durust::ErrorCode::QueueDeduplicated);

    // Now launch the dispatcher and let wf-q-1 run to completion.
    engine.launch().await?;
    assert_eq!(handle.get_result().await?, 42);

    // The destination FK is enforced: sending to an unknown id is typed.
    let err = engine
        .send("ghost", 1_i64, "topic")
        .await
        .expect_err("send to nonexistent workflow must fail");
    assert_eq!(err.code(), durust::ErrorCode::NonExistentWorkflow);

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

/// The run identity (authenticated user / assumed role / roles) is persisted on
/// the workflow row, readable from inside the workflow via the context, and
/// copied onto a fork.
#[tokio::test]
async fn sqlite_auth_context_persists_and_propagates() -> Result<()> {
    let (url, path) = temp_db_url("auth");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    // The workflow observes its own identity through the context and returns it,
    // proving the persisted fields are threaded into `DurableContext`.
    engine.register("whoami", |ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(format!(
            "{}/{}/{}",
            ctx.authenticated_user().unwrap_or("-"),
            ctx.assumed_role().unwrap_or("-"),
            ctx.authenticated_roles().join(","),
        ))
    });

    let opts = WorkflowOptions::with_id("wf-auth")
        .authenticated_user("alice")
        .assumed_role("admin")
        .authenticated_roles(["admin", "user"]);
    let mut handle = engine.run_workflow::<_, String>("whoami", (), opts).await?;
    assert_eq!(handle.get_result().await?, "alice/admin/admin,user");

    // The identity is durable on the row.
    let status = handle.get_status().await?;
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(status.assumed_role.as_deref(), Some("admin"));
    assert_eq!(status.authenticated_roles, vec!["admin", "user"]);

    // A fork inherits the originating identity.
    let forked = engine
        .fork_workflow::<String>("wf-auth", 0, WorkflowOptions::with_id("wf-auth-fork"))
        .await?;
    let fstatus = forked.get_status().await?;
    assert_eq!(fstatus.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(fstatus.assumed_role.as_deref(), Some("admin"));
    assert_eq!(fstatus.authenticated_roles, vec!["admin", "user"]);

    // A workflow started without an identity carries empty auth.
    let mut bare = engine
        .run_workflow::<_, String>("whoami", (), WorkflowOptions::with_id("wf-bare"))
        .await?;
    assert_eq!(bare.get_result().await?, "-/-/");
    let bstatus = bare.get_status().await?;
    assert_eq!(bstatus.authenticated_user, None);
    assert!(bstatus.authenticated_roles.is_empty());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A child workflow runs exactly once across a parent replay: re-running the
/// parent re-attaches to the recorded child instead of starting a new one.
#[tokio::test]
async fn sqlite_child_workflow_runs_once_across_restart() -> Result<()> {
    static CHILD_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("child");

    let register = |engine: &mut DurableEngine| {
        engine.register("child", |_ctx: DurableContext, n: i64| async move {
            CHILD_RUNS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(n + 100)
        });
        engine.register("parent", |ctx: DurableContext, n: i64| async move {
            let mut child = ctx
                .start_workflow::<_, i64>("child", n, WorkflowOptions::default())
                .await?;
            child.get_result().await
        });
    };

    // Process 1: run the parent to completion; the child runs once.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: i64 = engine.start_typed("parent", "wf-parent", 1_i64).await?;
        assert_eq!(out, 101);
    }

    // Process 2: a fresh engine over the same file re-runs the parent under the
    // same id. The recorded parent→child link is replayed, so the child is NOT
    // started again.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: i64 = engine.start_typed("parent", "wf-parent", 1_i64).await?;
        assert_eq!(out, 101);
    }

    assert_eq!(
        CHILD_RUNS.load(Ordering::SeqCst),
        1,
        "child workflow must run exactly once across a parent replay"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Step introspection: `get_workflow_steps` lists a workflow's recorded
/// operations (steps and a child invocation) in order, with names, outputs, and
/// the child link; `current_step_id` reflects the running counter.
#[tokio::test]
async fn sqlite_workflow_steps_introspection() -> Result<()> {
    let (url, path) = temp_db_url("steps");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("kid", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * 2)
    });
    engine.register("worker", |ctx: DurableContext, _: ()| async move {
        let a = ctx
            .step("alpha", || async { Ok::<_, Error>(1_i64) })
            .await?;
        let b = ctx.step("beta", || async { Ok::<_, Error>(a + 1) }).await?;
        let mut child = ctx
            .start_workflow::<_, i64>("kid", b, WorkflowOptions::default())
            .await?;
        child.get_result().await?;
        // Two steps + one child invocation consumed seqs 0,1,2 → next is 3.
        Ok::<_, Error>(ctx.current_step_id() as i64)
    });

    let next_seq: i64 = engine.start_typed("worker", "w1", ()).await?;
    assert_eq!(next_seq, 3);

    let steps = engine.get_workflow_steps("w1").await?;
    assert_eq!(steps.len(), 3);

    assert_eq!(steps[0].step_id, 0);
    assert_eq!(steps[0].name, "alpha");
    assert_eq!(steps[0].output, Some(serde_json::json!(1)));
    assert_eq!(steps[0].child_workflow_id, None);

    assert_eq!(steps[1].name, "beta");
    assert_eq!(steps[1].output, Some(serde_json::json!(2)));

    // The child invocation is recorded as step 2, carrying the child link.
    assert_eq!(steps[2].step_id, 2);
    assert_eq!(steps[2].name, "kid");
    assert_eq!(steps[2].child_workflow_id.as_deref(), Some("w1-2"));

    // An unknown workflow has no steps.
    assert!(engine.get_workflow_steps("nope").await?.is_empty());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Transactions and plain steps draw from the **same** per-workflow step counter:
/// interleaving them assigns sequential ids (step 0, transaction 1, step 2), so a
/// transaction is addressed by replay exactly like any other step.
#[tokio::test]
async fn sqlite_interleaved_step_and_transaction_share_seq() -> Result<()> {
    use durust::params;
    let (url, path) = temp_db_url("interleave");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("mix", |ctx: DurableContext, _: ()| async move {
        // seq 0: a plain step.
        ctx.step("before", || async { Ok::<_, Error>(1_i64) })
            .await?;
        // seq 1: a transaction step in between.
        ctx.transaction::<(), _>("tx", |tx| {
            Box::pin(async move {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS m (id INTEGER PRIMARY KEY)",
                    &params![],
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        // seq 2: another plain step.
        ctx.step("after", || async { Ok::<_, Error>(2_i64) })
            .await?;
        Ok::<_, Error>(ctx.current_step_id() as i64)
    });

    let next_seq: i64 = engine.start_typed("mix", "w-mix", ()).await?;
    assert_eq!(next_seq, 3, "step, transaction, step consumed seqs 0,1,2");

    let steps = engine.get_workflow_steps("w-mix").await?;
    assert_eq!(steps.len(), 3);
    assert_eq!((steps[0].step_id, steps[0].name.as_str()), (0, "before"));
    assert_eq!(
        (steps[1].step_id, steps[1].name.as_str()),
        (1, "tx"),
        "the transaction landed at seq 1, sharing the step counter"
    );
    assert_eq!((steps[2].step_id, steps[2].name.as_str()), (2, "after"));

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
    // Resume/fork re-queue work for a dispatcher, so the engine must be live.
    engine.launch().await?;

    engine.start_typed::<_, i64>("pipeline", "wf-1", ()).await?;

    // list filters via QueryBuilder.
    let listed = engine
        .list_workflows(&ListFilter {
            name: vec!["pipeline".to_string()],
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

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Bulk cancel / resume / delete through SQLite: exercises the `IN (...)` lists,
/// the `RETURNING` resume, and the recursive-CTE child delete (with FK cascade).
#[tokio::test]
async fn sqlite_bulk_ops() -> Result<()> {
    use durust::{StateProvider, WorkflowStatus, STATUS_PENDING};
    let (url, path) = temp_db_url("bulk");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    // Resume re-queues work for a dispatcher, so the engine must be live.
    engine.launch().await?;
    let provider = SqliteProvider::connect(&url).await?;

    let seed = |id: &str, status: &str, parent: Option<&str>| {
        let mut s = WorkflowStatus::new(id, "noop", serde_json::Value::Null, status, "", "0.1.0");
        s.parent_workflow_id = parent.map(|p| p.to_string());
        s
    };

    for id in ["wf-1", "wf-2", "wf-3"] {
        provider
            .insert_workflow_status(seed(id, STATUS_PENDING, None))
            .await?;
    }

    // Bulk cancel a subset + a missing id (skipped, no error).
    engine
        .cancel_workflows(&["wf-1".into(), "wf-2".into(), "ghost".into()])
        .await?;
    assert_eq!(
        provider.get_workflow_status("wf-1").await?.unwrap().status,
        STATUS_CANCELLED
    );
    assert_eq!(
        provider.get_workflow_status("wf-3").await?.unwrap().status,
        STATUS_PENDING
    );

    // Bulk resume returns a handle per transitioned id; they run to completion.
    let handles = engine
        .resume_workflows::<()>(&["wf-1".into(), "wf-2".into()])
        .await?;
    assert_eq!(handles.len(), 2);
    for mut h in handles {
        h.get_result().await?;
    }
    assert_eq!(
        provider.get_workflow_status("wf-1").await?.unwrap().status,
        STATUS_SUCCESS
    );

    // Recursive delete: parent → child → grandchild.
    provider
        .insert_workflow_status(seed("p", STATUS_SUCCESS, None))
        .await?;
    provider
        .insert_workflow_status(seed("c", STATUS_SUCCESS, Some("p")))
        .await?;
    provider
        .insert_workflow_status(seed("gc", STATUS_SUCCESS, Some("c")))
        .await?;
    engine.delete_workflows(&["p".into()], true).await?;
    assert!(provider.get_workflow_status("p").await?.is_none());
    assert!(provider.get_workflow_status("c").await?.is_none());
    assert!(provider.get_workflow_status("gc").await?.is_none());

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// set_workflow_delay reschedules a DELAYED workflow through SQLite: a far-future
/// delay is shortened so the dispatcher runs it promptly; a non-DELAYED row is a
/// no-op.
#[tokio::test]
async fn sqlite_set_workflow_delay() -> Result<()> {
    use durust::STATUS_DELAYED;
    let (url, path) = temp_db_url("set-delay");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register_queue(WorkflowQueue::new("d").base_polling_interval(Duration::from_millis(10)));
    engine.launch().await?;

    let mut opts = WorkflowOptions::with_id("wf-d");
    opts.delay = Some(Duration::from_secs(60));
    let mut handle = engine.enqueue::<_, i64>("d", "echo", 5_i64, opts).await?;
    assert_eq!(handle.get_status().await?.status, STATUS_DELAYED);

    let started = std::time::Instant::now();
    assert!(
        engine
            .set_workflow_delay("wf-d", Duration::from_millis(20))
            .await?
    );
    assert_eq!(handle.get_result().await?, 5);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "must run on the shortened delay, not the original 60s"
    );

    // Completed → no longer DELAYED → silent no-op.
    assert!(!engine.set_workflow_delay("wf-d", Duration::ZERO).await?);

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `queues_only` is enforced in SQL: only workflows with a non-null queue_name
/// come back.
#[tokio::test]
async fn sqlite_queues_only_filter() -> Result<()> {
    let (url, path) = temp_db_url("queues-only");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));
    engine.launch().await?;

    engine.start_typed::<_, ()>("noop", "direct", ()).await?;
    engine
        .enqueue::<_, ()>("q", "noop", (), WorkflowOptions::with_id("queued"))
        .await?
        .get_result()
        .await?;

    let queued: Vec<String> = engine
        .list_workflows(&ListFilter {
            queues_only: true,
            ..Default::default()
        })
        .await?
        .into_iter()
        .map(|w| w.id)
        .collect();
    assert_eq!(queued, vec!["queued".to_string()]);

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A partitioned queue persists each workflow's partition key and dispatches
/// every active partition independently.
#[tokio::test]
async fn sqlite_partitioned_queue_dispatch() -> Result<()> {
    let (url, path) = temp_db_url("partition");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register_queue(
        WorkflowQueue::new("pq")
            .partitioned()
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;

    let mut a = engine
        .enqueue::<_, i64>(
            "pq",
            "echo",
            1_i64,
            WorkflowOptions::with_id("a").partition_key("east"),
        )
        .await?;
    let mut b = engine
        .enqueue::<_, i64>(
            "pq",
            "echo",
            2_i64,
            WorkflowOptions::with_id("b").partition_key("west"),
        )
        .await?;
    assert_eq!(a.get_result().await?, 1);
    assert_eq!(b.get_result().await?, 2);

    // The partition key round-trips through the workflow_status row.
    assert_eq!(
        a.get_status().await?.queue_partition_key.as_deref(),
        Some("east")
    );
    assert_eq!(
        b.get_status().await?.queue_partition_key.as_deref(),
        Some("west")
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// The new list filters work through SQL: has_parent splits child from root, the
/// load flags blank input/output, and the completed/dequeued time bounds apply.
#[tokio::test]
async fn sqlite_list_filters_extended() -> Result<()> {
    let (url, path) = temp_db_url("list-filters");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("child", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * 10)
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        let mut h = ctx
            .start_workflow::<i64, i64>("child", 5_i64, WorkflowOptions::default())
            .await?;
        h.get_result().await
    });
    let out: i64 = engine.start_typed("parent", "p", ()).await?;
    assert_eq!(out, 50);

    // has_parent isolates the child.
    let children = engine
        .list_workflows(&ListFilter {
            has_parent: Some(true),
            ..Default::default()
        })
        .await?;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name, "child");
    let child_id = children[0].id.clone();

    // Load flags blank the heavy columns (NULL AS inputs/output).
    let lean = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![child_id.clone()],
            load_input: false,
            load_output: false,
            ..Default::default()
        })
        .await?;
    assert_eq!(lean[0].input, serde_json::Value::Null);
    assert!(lean[0].output.is_none());
    let full = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![child_id],
            ..Default::default()
        })
        .await?;
    assert_eq!(full[0].output, Some(serde_json::json!(50)));

    // Completion time bound: a far-future lower bound excludes everything.
    let now = chrono::Utc::now().timestamp_millis();
    let future = engine
        .list_workflows(&ListFilter {
            completed_after_ms: Some(now + 3_600_000),
            ..Default::default()
        })
        .await?;
    assert!(future.is_empty());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// get_workflow_aggregates groups in SQL: by status, by name, and with a
/// created_at time bucket.
#[tokio::test]
async fn sqlite_workflow_aggregates() -> Result<()> {
    use durust::WorkflowAggregateQuery;
    let (url, path) = temp_db_url("aggregates");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("ok", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register("boom", |_ctx: DurableContext, _: ()| async move {
        Err::<(), _>(Error::app("nope"))
    });
    engine.start_typed::<_, ()>("ok", "a", ()).await?;
    engine.start_typed::<_, ()>("ok", "b", ()).await?;
    let _ = engine.start_typed::<_, ()>("boom", "c", ()).await;

    // Group by status; also select the latency aggregates.
    let by_status = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            select_min_created_at: true,
            select_max_total_latency_ms: true,
            ..Default::default()
        })
        .await?;
    let success = by_status
        .iter()
        .find(|r| r.group.get("status") == Some(&Some(STATUS_SUCCESS.to_string())))
        .expect("a SUCCESS group");
    assert_eq!(success.count, Some(2));
    assert!(success.min_created_at.is_some());
    assert!(success.max_total_latency_ms.is_some_and(|l| l >= 0));

    // A one-hour time bucket collapses everything into a single group.
    let bucketed = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            select_count: true,
            time_bucket_ms: Some(3_600_000),
            ..Default::default()
        })
        .await?;
    assert_eq!(bucketed.len(), 1);
    assert_eq!(bucketed[0].count, Some(3));
    assert!(bucketed[0].group.contains_key("time_bucket"));

    // The completed_*/dequeued_* filters narrow which rows are counted.
    let now = chrono::Utc::now().timestamp_millis();
    let hour = 3_600_000;
    let sum =
        |rows: Vec<durust::WorkflowAggregate>| -> i64 { rows.iter().filter_map(|r| r.count).sum() };
    let recent = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            completed_after_ms: Some(now - hour),
            completed_before_ms: Some(now + hour),
            ..Default::default()
        })
        .await?;
    assert_eq!(sum(recent), 3, "all three completed within the window");
    let future = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            dequeued_after_ms: Some(now + hour),
            ..Default::default()
        })
        .await?;
    assert_eq!(sum(future), 0, "none dequeued in the future");

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Step start/finish timestamps are persisted and surface through
/// get_workflow_steps; an instantaneous DBOS.* op records no start time.
#[tokio::test]
async fn sqlite_step_timing_is_recorded() -> Result<()> {
    let (url, path) = temp_db_url("step-timing");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("work", |ctx: DurableContext, _: ()| async move {
        ctx.step("compute", || async { Ok::<_, Error>(1_i64) })
            .await?;
        ctx.sleep(Duration::from_millis(1)).await?;
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("work", "w", ()).await?;

    let steps = engine.get_workflow_steps("w").await?;
    let compute = steps
        .iter()
        .find(|s| s.name == "compute")
        .expect("compute step");
    let start = compute.started_at.expect("started_at persisted");
    let end = compute.completed_at.expect("completed_at persisted");
    assert!(start <= end);

    // The sleep marker is instantaneous: completed but no start time.
    let sleep = steps
        .iter()
        .find(|s| s.name == "DBOS.sleep")
        .expect("sleep step");
    assert!(sleep.started_at.is_none());
    assert!(sleep.completed_at.is_some());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Step aggregates work in SQL: count + max duration grouped by function name,
/// with the derived status filter.
#[tokio::test]
async fn sqlite_step_aggregates() -> Result<()> {
    use durust::StepAggregateQuery;
    let (url, path) = temp_db_url("step-aggregates");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("work", |ctx: DurableContext, _: ()| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.step("b", || async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, Error>(2_i64)
        })
        .await?;
        ctx.step("a", || async { Ok::<_, Error>(3_i64) }).await?;
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("work", "w", ()).await?;

    let by_fn = engine
        .get_step_aggregates(&StepAggregateQuery {
            by_function_name: true,
            select_count: true,
            select_max_duration_ms: true,
            ..Default::default()
        })
        .await?;
    let a = by_fn
        .iter()
        .find(|r| r.group.get("function_name") == Some(&Some("a".to_string())))
        .expect("group a");
    let b = by_fn
        .iter()
        .find(|r| r.group.get("function_name") == Some(&Some("b".to_string())))
        .expect("group b");
    assert_eq!(a.count, Some(2));
    assert_eq!(b.count, Some(1));
    assert!(b.max_duration_ms.unwrap_or(0) >= 10);

    // Filtering by the SUCCESS status keeps all steps (none errored).
    let success = engine
        .get_step_aggregates(&StepAggregateQuery {
            by_function_name: true,
            select_count: true,
            status: vec!["SUCCESS".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(success.iter().filter_map(|r| r.count).sum::<i64>(), 3);

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A schedule created on one engine survives a "restart" (a fresh engine over the
/// same database file): its row, status, and context come back, a pause persists,
/// and a delete removes it.
#[tokio::test]
async fn sqlite_schedule_persists_across_restart() -> Result<()> {
    use durust::{ScheduleFilter, ScheduleOptions, ScheduleStatus};
    let (url, path) = temp_db_url("schedule-crud");

    // Engine A creates the schedule.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register(
            "nightly_job",
            |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, Error>(()) },
        );
        engine
            .create_schedule(
                "nightly",
                "nightly_job",
                "0 0 0 * * *",
                ScheduleOptions::new()
                    .context(&serde_json::json!({"region": "us"}))
                    .queue_name("internal"),
            )
            .await?;
    }

    // Engine B reads it back, with context and queue intact, then pauses it.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        engine.register(
            "nightly_job",
            |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, Error>(()) },
        );
        let got = engine.get_schedule("nightly").await?.expect("persisted");
        assert_eq!(got.workflow_name, "nightly_job");
        assert_eq!(got.status, ScheduleStatus::Active);
        assert_eq!(got.queue_name.as_deref(), Some("internal"));
        assert_eq!(
            got.context.as_ref().and_then(|v| v.get("region")),
            Some(&serde_json::json!("us"))
        );
        assert!(engine.pause_schedule("nightly").await?);
    }

    // Engine C sees the pause and deletes the schedule.
    {
        let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        let paused = engine.get_schedule("nightly").await?.expect("persisted");
        assert_eq!(paused.status, ScheduleStatus::Paused);
        assert!(engine
            .list_schedules(&ScheduleFilter {
                statuses: vec![ScheduleStatus::Active],
                ..Default::default()
            })
            .await?
            .is_empty());
        assert!(engine.delete_schedule("nightly").await?);
        assert!(engine.get_schedule("nightly").await?.is_none());
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Backfilling a past range through SQLite persists one workflow row per cron
/// tick under the deterministic id; re-running the same range is idempotent (no
/// duplicate rows), and the backfilled runs complete.
#[tokio::test]
async fn sqlite_backfill_persists_each_tick_once() -> Result<()> {
    use chrono::{TimeZone, Utc};
    use durust::{ScheduleOptions, StateProvider};
    let (url, path) = temp_db_url("schedule-backfill");

    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(
        "nightly_job",
        |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, Error>(()) },
    );
    engine
        .create_schedule(
            "daily",
            "nightly_job",
            "0 0 12 * * *",
            ScheduleOptions::new(),
        )
        .await?;

    let start = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();
    let ids = engine.backfill_schedule("daily", start, end).await?;
    assert_eq!(ids.len(), 3, "one tick per day");

    let filter = ListFilter {
        workflow_id_prefix: vec!["sched-daily-".to_string()],
        ..Default::default()
    };

    // Wait for the backfilled direct runs to complete.
    for _ in 0..100 {
        let rows = provider.list_workflows(&filter).await?;
        if rows.len() == 3 && rows.iter().all(|r| r.status == STATUS_SUCCESS) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rows = provider.list_workflows(&filter).await?;
    assert_eq!(rows.len(), 3, "one persisted row per tick");
    assert!(rows.iter().all(|r| r.status == STATUS_SUCCESS));

    // Re-backfilling the same range creates no new rows.
    let again = engine.backfill_schedule("daily", start, end).await?;
    assert_eq!(again, ids, "same deterministic ids");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        provider.list_workflows(&filter).await?.len(),
        3,
        "no duplicate rows"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Application versions persist across "restarts": versions registered on launch
/// survive into a fresh engine over the same file, `set_latest` promotes one,
/// and the promotion is durable.
#[tokio::test]
async fn sqlite_application_versions_persist() -> Result<()> {
    let (url, path) = temp_db_url("app-versions");

    // Two launches over the same file register two versions; 2.0.0 is latest.
    {
        let a = DurableEngine::new_with_version(
            Arc::new(SqliteProvider::connect(&url).await?),
            "1.0.0",
        )
        .await?;
        a.launch().await?;
        a.shutdown(Duration::from_secs(1)).await?;
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    {
        let b = DurableEngine::new_with_version(
            Arc::new(SqliteProvider::connect(&url).await?),
            "2.0.0",
        )
        .await?;
        b.launch().await?;
        b.shutdown(Duration::from_secs(1)).await?;
    }

    // A fresh engine sees both, newest first, and promotes 1.0.0.
    {
        let c = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        let versions = c.list_application_versions().await?;
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version_name, "2.0.0");
        assert_eq!(
            c.get_latest_application_version()
                .await?
                .unwrap()
                .version_name,
            "2.0.0"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(c.set_latest_application_version("1.0.0").await?);
    }

    // The promotion survived the restart.
    {
        let d = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        assert_eq!(
            d.get_latest_application_version()
                .await?
                .unwrap()
                .version_name,
            "1.0.0"
        );
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A `Client` and an engine over the same SQLite file: the client enqueues, the
/// engine runs it, the client observes the result.
#[tokio::test]
async fn sqlite_client_enqueues_work_an_engine_runs() -> Result<()> {
    use durust::{Client, WorkflowQueue};
    let (url, path) = temp_db_url("client");

    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("double", |ctx: DurableContext, n: i64| async move {
        ctx.step("mul", || async { Ok::<_, Error>(n * 2) }).await
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("job-1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, i64>("q", "double", 21i64, opts).await?;
    assert_eq!(handle.get_result().await?, 42);
    assert_eq!(
        client.list_workflows(&ListFilter::default()).await?.len(),
        1
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A client backfills a direct schedule on SQLite: each tick lands ENQUEUED on
/// the internal queue, and re-running the same window is idempotent.
#[tokio::test]
async fn sqlite_client_backfills_a_schedule() -> Result<()> {
    use chrono::{TimeZone, Utc};
    use durust::{Client, ScheduleOptions, StateProvider};
    let (url, path) = temp_db_url("client-backfill");
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    // A client runs no engine, so initialize the schema directly (the engine's
    // constructor does this otherwise).
    provider.init().await?;
    let client = Client::new(provider.clone());

    client
        .create_schedule("daily", "report", "0 0 12 * * *", ScheduleOptions::new())
        .await?;

    let start = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();
    let ids = client.backfill_schedule("daily", start, end).await?;
    assert_eq!(ids.len(), 3);

    let prefix = || ListFilter {
        workflow_id_prefix: vec!["sched-daily-".to_string()],
        ..Default::default()
    };
    let rows = client.list_workflows(&prefix()).await?;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.status == "ENQUEUED"));
    assert!(rows
        .iter()
        .all(|r| r.queue_name.as_deref() == Some("_dbos_internal_queue")));

    // Idempotent re-backfill: same ids, no duplicate rows.
    assert_eq!(client.backfill_schedule("daily", start, end).await?, ids);
    assert_eq!(client.list_workflows(&prefix()).await?.len(), 3);

    drop(client);
    drop(provider);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Portable mode stores a workflow's input in the cross-language args envelope
/// on SQLite, and reads it back unwrapped to the bare input.
#[tokio::test]
async fn sqlite_portable_input_envelope() -> Result<()> {
    use durust::Serializer;
    let (url, path) = temp_db_url("portable-input");
    let provider = SqliteProvider::connect(&url)
        .await?
        .with_serializer(Serializer::Portable);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("echo", |_ctx: DurableContext, name: String| async move {
        Ok::<_, Error>(format!("echo:{name}"))
    });
    let out: String = engine
        .run_workflow::<_, String>(
            "echo",
            "ada".to_string(),
            WorkflowOptions::with_id("wf-env"),
        )
        .await?
        .get_result()
        .await?;
    assert_eq!(out, "echo:ada");

    // The raw input column holds the args envelope.
    let pool = sqlx::SqlitePool::connect(&url).await?;
    let raw_inputs: String =
        sqlx::query_scalar("SELECT inputs FROM workflow_status WHERE workflow_uuid = ?")
            .bind("wf-env")
            .fetch_one(&pool)
            .await?;
    assert_eq!(raw_inputs, r#"{"positionalArgs":["ada"],"namedArgs":{}}"#);

    // Through the provider, the input reads back unwrapped.
    let status = engine
        .retrieve_workflow::<String>("wf-env")
        .await?
        .get_status()
        .await?;
    assert_eq!(status.input, serde_json::json!("ada"));

    drop(pool);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A user-supplied serializer codec encodes stored values in its own format and
/// is routed back on read by the format tag, so a workflow's input, step output,
/// and result round-trip through it on SQLite.
#[tokio::test]
async fn sqlite_custom_serializer_roundtrips() -> Result<()> {
    use durust::{Serializer, SerializerCodec};
    use std::sync::Arc;

    /// Stores values as `hex:<lowercase-hex-of-the-JSON-bytes>` — deliberately
    /// unlike the built-in base64/plain-JSON forms so the raw column proves the
    /// custom codec ran.
    struct HexCodec;
    impl SerializerCodec for HexCodec {
        fn name(&self) -> &str {
            "DBOS_HEX"
        }
        fn encode(&self, value: &serde_json::Value) -> Result<String> {
            let bytes = serde_json::to_vec(value).map_err(Error::from)?;
            let mut s = String::from("hex:");
            for b in bytes {
                s.push_str(&format!("{b:02x}"));
            }
            Ok(s)
        }
        fn decode(&self, stored: &str) -> Result<serde_json::Value> {
            let hex = stored.strip_prefix("hex:").ok_or_else(|| {
                Error::Serialization("custom value missing hex: prefix".to_string())
            })?;
            let bytes: std::result::Result<Vec<u8>, _> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                .collect();
            let bytes = bytes.map_err(|e| Error::Serialization(format!("bad hex: {e}")))?;
            serde_json::from_slice(&bytes).map_err(Error::from)
        }
    }

    let (url, path) = temp_db_url("custom-ser");
    let provider = SqliteProvider::connect(&url)
        .await?
        .with_serializer(Serializer::custom(Arc::new(HexCodec)));
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("greet", |ctx: DurableContext, name: String| async move {
        // A step output also flows through the custom codec.
        let upper = ctx
            .step("shout", {
                let name = name.clone();
                move || {
                    let name = name.clone();
                    async move { Ok::<_, Error>(name.to_uppercase()) }
                }
            })
            .await?;
        Ok::<_, Error>(format!("hi {upper}"))
    });

    let out: String = engine
        .run_workflow::<_, String>(
            "greet",
            "ada".to_string(),
            WorkflowOptions::with_id("wf-hex"),
        )
        .await?
        .get_result()
        .await?;
    assert_eq!(out, "hi ADA");

    // The raw columns are stored in the custom hex format, tagged DBOS_HEX.
    let pool = sqlx::SqlitePool::connect(&url).await?;
    let (raw_input, raw_output, fmt): (String, Option<String>, String) = sqlx::query_as(
        "SELECT inputs, output, serialization FROM workflow_status WHERE workflow_uuid = ?",
    )
    .bind("wf-hex")
    .fetch_one(&pool)
    .await?;
    assert_eq!(fmt, "DBOS_HEX");
    assert!(
        raw_input.starts_with("hex:"),
        "input stored via custom codec"
    );
    assert!(raw_output.as_deref().is_some_and(|o| o.starts_with("hex:")));

    // Read back through the provider: input, output, and the recorded step all
    // decode via the configured codec.
    let status = engine
        .retrieve_workflow::<String>("wf-hex")
        .await?
        .get_status()
        .await?;
    assert_eq!(status.input, serde_json::json!("ada"));
    assert_eq!(status.output, Some(serde_json::json!("hi ADA")));

    let steps = engine.get_workflow_steps("wf-hex").await?;
    let shout = steps
        .iter()
        .find(|s| s.name == "shout")
        .expect("step recorded");
    assert_eq!(shout.output, Some(serde_json::json!("ADA")));

    drop(pool);
    drop(engine);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// On SQLite, the client's `ReturnExisting` dedup policy returns the workflow
/// already holding the slot, while the default rejects the collision.
#[tokio::test]
async fn sqlite_enqueue_dedup_return_existing() -> Result<()> {
    use durust::{Client, DeduplicationPolicy, StateProvider, WorkflowHandle};
    let (url, path) = temp_db_url("dedup");
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    provider.init().await?;
    let client = Client::new(provider.clone());

    let first: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            1i64,
            WorkflowOptions::with_id("d1").dedup_id("once"),
        )
        .await?;
    // Default policy rejects a colliding dedup id.
    assert!(client
        .enqueue::<_, i64>(
            "dq",
            "wf",
            2i64,
            WorkflowOptions::with_id("d2").dedup_id("once")
        )
        .await
        .is_err());
    // ReturnExisting hands back the holder.
    let again: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            3i64,
            WorkflowOptions::with_id("d3")
                .dedup_id("once")
                .dedup_policy(DeduplicationPolicy::ReturnExisting),
        )
        .await?;
    assert_eq!(again.id(), first.id());

    drop(client);
    drop(provider);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A deduplication id is released once its holder reaches a terminal state, so
/// the same id can be enqueued again afterward.
#[tokio::test]
async fn sqlite_dedup_slot_frees_on_completion() -> Result<()> {
    use durust::{Client, StateProvider, WorkflowHandle};
    let (url, path) = temp_db_url("dedupfree");
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    provider.init().await?;
    let client = Client::new(provider.clone());

    let first: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            1i64,
            WorkflowOptions::with_id("d1").dedup_id("once"),
        )
        .await?;
    // The partial unique index rejects a colliding dedup id while d1 is active.
    assert!(client
        .enqueue::<_, i64>(
            "dq",
            "wf",
            2i64,
            WorkflowOptions::with_id("d2").dedup_id("once")
        )
        .await
        .is_err());

    // Completing d1 nulls its deduplication_id, freeing the slot.
    provider
        .set_workflow_status(first.id(), STATUS_SUCCESS, None, None)
        .await?;

    let third: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            3i64,
            WorkflowOptions::with_id("d3").dedup_id("once"),
        )
        .await?;
    assert_eq!(third.id(), "d3");

    drop(client);
    drop(provider);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A workflow cancelled during its final step must stay cancelled: a late
/// SUCCESS/ERROR completion is rejected and does not overwrite the status.
#[tokio::test]
async fn sqlite_completion_cannot_overwrite_cancelled() -> Result<()> {
    use durust::{Client, StateProvider, WorkflowHandle};
    let (url, path) = temp_db_url("cancelguard");
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    provider.init().await?;
    let client = Client::new(provider.clone());

    let h: WorkflowHandle<i64> = client
        .enqueue("q", "wf", 1i64, WorkflowOptions::with_id("c1"))
        .await?;
    provider
        .set_workflow_status(h.id(), STATUS_CANCELLED, None, Some("cancelled"))
        .await?;

    // A completion racing the cancellation must error, not flip it to SUCCESS.
    let late = provider
        .set_workflow_status(h.id(), STATUS_SUCCESS, None, None)
        .await;
    assert!(late.is_err(), "completing a cancelled workflow must error");

    let row = provider.get_workflow_status(h.id()).await?.unwrap();
    assert_eq!(row.status, STATUS_CANCELLED);

    drop(client);
    drop(provider);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `apply_schedules` is atomic: a mid-batch failure rolls the whole batch back,
/// leaving any schedule it would have replaced at its original value.
#[tokio::test]
async fn sqlite_apply_schedules_is_atomic() -> Result<()> {
    use durust::{ScheduleFilter, ScheduleStatus, StateProvider, WorkflowSchedule};
    let (url, path) = temp_db_url("applytx");
    let provider = SqliteProvider::connect(&url).await?;
    provider.init().await?;

    let make = |id: &str, name: &str, cron: &str| WorkflowSchedule {
        schedule_id: id.to_string(),
        schedule_name: name.to_string(),
        workflow_name: "wf".to_string(),
        schedule: cron.to_string(),
        status: ScheduleStatus::Active,
        context: None,
        last_fired_at: None,
        automatic_backfill: false,
        cron_timezone: None,
        queue_name: None,
    };

    // Seed an existing schedule "keep".
    provider
        .apply_schedules(&[make("id-keep", "keep", "0 0 1 * * *")])
        .await?;

    // A batch that would replace "keep" then fails on a duplicate schedule_id:
    // the second insert violates the PRIMARY KEY, so the whole batch rolls back.
    let bad = vec![
        make("dup", "keep", "0 0 2 * * *"),
        make("dup", "other", "0 0 3 * * *"),
    ];
    assert!(
        provider.apply_schedules(&bad).await.is_err(),
        "a duplicate schedule_id must fail the batch"
    );

    // "keep" still has its original id and cron; "other" was never created.
    let all = provider.list_schedules(&ScheduleFilter::default()).await?;
    assert_eq!(all.len(), 1, "rollback left exactly the original schedule");
    assert_eq!(all[0].schedule_name, "keep");
    assert_eq!(all[0].schedule_id, "id-keep", "original id preserved");
    assert_eq!(all[0].schedule, "0 0 1 * * *", "original cron preserved");

    drop(provider);
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A transactional step commits the user's SQL writes and the step checkpoint
/// together: across a restart the debit replays from the checkpoint and is not
/// reapplied (exactly-once).
#[tokio::test]
async fn sqlite_transaction_step_exactly_once() -> Result<()> {
    use durust::params;
    let (url, path) = temp_db_url("txn");

    fn register(engine: &mut DurableEngine) {
        engine.register("acct", |ctx: DurableContext, _: ()| async move {
            ctx.transaction::<(), _>("setup", |tx| {
                Box::pin(async move {
                    tx.execute(
                        "CREATE TABLE IF NOT EXISTS acct (id INTEGER PRIMARY KEY, bal INTEGER)",
                        &params![],
                    )
                    .await?;
                    tx.execute(
                        "INSERT INTO acct (id, bal) VALUES (1, 100) ON CONFLICT (id) DO NOTHING",
                        &params![],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await?;
            let bal: i64 = ctx
                .transaction("debit", |tx| {
                    Box::pin(async move {
                        tx.execute(
                            "UPDATE acct SET bal = bal - ? WHERE id = ?",
                            &params![10_i64, 1_i64],
                        )
                        .await?;
                        let row = tx
                            .query_one("SELECT bal FROM acct WHERE id = ?", &params![1_i64])
                            .await?;
                        Ok(row.get::<i64>("bal"))
                    })
                })
                .await?;
            Ok::<_, Error>(bal)
        });
    }

    // First run: seed 100, debit 10 -> 90.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let bal: i64 = engine.start_typed("acct", "wf-txn", ()).await?;
        assert_eq!(bal, 90);
    }
    // Restart and re-run the same id: both transaction steps replay (the body is
    // not run), so the debit is not reapplied — still 90, not 80.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let bal: i64 = engine.start_typed("acct", "wf-txn", ()).await?;
        assert_eq!(
            bal, 90,
            "transactional step is exactly-once across a restart"
        );
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A transaction started inside another transaction's body is rejected with a
/// clear error rather than deadlocking on the outer's write lock (the inner would
/// otherwise open a second connection and block forever). The workflow fails fast.
#[tokio::test]
async fn sqlite_nested_transaction_is_rejected() -> Result<()> {
    use durust::params;
    use std::time::Duration;
    let (url, path) = temp_db_url("txnnest");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("nest", |ctx: DurableContext, _: ()| async move {
        let inner_ctx = ctx.clone();
        ctx.transaction::<(), _>("outer", move |tx| {
            let inner_ctx = inner_ctx.clone();
            Box::pin(async move {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)",
                    &params![],
                )
                .await?;
                // Nesting a transaction via a captured context must be refused.
                inner_ctx
                    .transaction::<(), _>("inner", |tx2| {
                        Box::pin(async move {
                            tx2.execute("INSERT INTO t (id) VALUES (1)", &params![])
                                .await?;
                            Ok(())
                        })
                    })
                    .await?;
                Ok(())
            })
        })
        .await?;
        Ok::<_, Error>(())
    });

    // Must complete (fail) promptly — a deadlock would hang until the timeout.
    let run = engine.start_typed::<_, ()>("nest", "wf-nest", ());
    let res = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("nested transaction must be rejected, not deadlock");
    let err = res.expect_err("nesting a transaction is an error");
    assert!(
        err.to_string()
            .contains("transaction inside another transaction"),
        "clear nesting error, got: {err}"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A transactional step whose body returns an error rolls back its writes: a
/// later step reads the original value, proving the failed write did not commit.
#[tokio::test]
async fn sqlite_transaction_step_rolls_back_on_error() -> Result<()> {
    use durust::params;
    let (url, path) = temp_db_url("txnrb");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("rb", |ctx: DurableContext, _: ()| async move {
        ctx.transaction::<(), _>("setup", |tx| {
            Box::pin(async move {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS r (id INTEGER PRIMARY KEY, v INTEGER)",
                    &params![],
                )
                .await?;
                tx.execute(
                    "INSERT INTO r (id, v) VALUES (1, 0) ON CONFLICT (id) DO NOTHING",
                    &params![],
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        // Write, then fail: the write must roll back with the transaction.
        let failed = ctx
            .transaction::<(), _>("bad", |tx| {
                Box::pin(async move {
                    tx.execute("UPDATE r SET v = 999 WHERE id = 1", &params![])
                        .await?;
                    Err(Error::app("boom"))
                })
            })
            .await
            .is_err();
        let v: i64 = ctx
            .transaction("read", |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_one("SELECT v FROM r WHERE id = 1", &params![])
                        .await?;
                    Ok(row.get::<i64>("v"))
                })
            })
            .await?;
        Ok::<_, Error>((failed, v))
    });
    let (failed, v): (bool, i64) = engine.start_typed("rb", "wf-rb", ()).await?;
    assert!(failed, "the failing transaction returned its error");
    assert_eq!(v, 0, "the failed transaction's write rolled back");
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A transactional step's *failure* is checkpointed (outside the rolled-back
/// body tx): a workflow that catches it and a fresh engine that replays both
/// observe the recorded error without re-running the body — so a non-deterministic
/// transaction step cannot silently succeed the second time.
#[tokio::test]
async fn sqlite_checkpoints_a_caught_transaction_failure() -> Result<()> {
    static TX_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("txn-err");

    let register = |engine: &mut DurableEngine| {
        engine.register("txn_flaky", |ctx: DurableContext, _: ()| async move {
            let r: Result<i64> = ctx
                .transaction::<i64, _>("maybe", |_tx| {
                    Box::pin(async move {
                        let n = TX_RUNS.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            Err(Error::app("transient"))
                        } else {
                            Ok(7)
                        }
                    })
                })
                .await;
            Ok::<_, Error>(if r.is_ok() { "ok" } else { "caught-error" }.to_string())
        });
    };

    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine.start_typed("txn_flaky", "wf-txn-err", ()).await?;
        assert_eq!(out, "caught-error");
        let steps = engine.get_workflow_steps("wf-txn-err").await?;
        let maybe = steps
            .iter()
            .find(|s| s.name == "maybe")
            .expect("step recorded");
        assert_eq!(maybe.error.as_deref(), Some("transient"));
    }
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine.start_typed("txn_flaky", "wf-txn-err", ()).await?;
        assert_eq!(
            out, "caught-error",
            "replay observes the recorded transaction error"
        );
    }
    assert_eq!(
        TX_RUNS.load(Ordering::SeqCst),
        1,
        "a checkpointed failed transaction step is not re-run on replay"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `TransactionOptions::max_retries` re-runs the whole body on an application
/// error, on a fresh transaction, until it succeeds. The successful attempt is
/// the one that's checkpointed, so nothing is recorded as failed.
#[tokio::test]
async fn sqlite_transaction_retries_body_error() -> Result<()> {
    use durust::params;
    static TX_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("txn-retry");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("retry", |ctx: DurableContext, _: ()| async move {
        let opts = TransactionOptions::new("flaky")
            .max_retries(3)
            .base_interval(Duration::from_millis(1));
        let n: i64 = ctx
            .transaction_with(opts, |tx| {
                Box::pin(async move {
                    // Touch the tx so each attempt really opens one.
                    tx.execute("SELECT 1", &params![]).await?;
                    let run = TX_RUNS.fetch_add(1, Ordering::SeqCst);
                    if run < 2 {
                        Err(Error::app("transient"))
                    } else {
                        Ok(42_i64)
                    }
                })
            })
            .await?;
        Ok::<_, Error>(n)
    });
    let out: i64 = engine.start_typed("retry", "wf-txn-retry", ()).await?;
    assert_eq!(
        out, 42,
        "the body eventually succeeds and returns its value"
    );
    assert_eq!(
        TX_RUNS.load(Ordering::SeqCst),
        3,
        "the body re-runs twice then succeeds (1 + 2 retries)"
    );
    // The checkpointed step is the success, not a failure.
    let steps = engine.get_workflow_steps("wf-txn-retry").await?;
    let step = steps.iter().find(|s| s.name == "flaky").expect("recorded");
    assert!(step.error.is_none(), "the recorded outcome is the success");
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A `retry_if` predicate that rejects the error stops retries immediately, even
/// with `max_retries` remaining: the body runs exactly once and the error is
/// surfaced (and checkpointed) without burning the budget.
#[tokio::test]
async fn sqlite_transaction_retry_predicate_fails_fast() -> Result<()> {
    use durust::params;
    static TX_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("txn-nofast");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("nofast", |ctx: DurableContext, _: ()| async move {
        let opts = TransactionOptions::new("permanent")
            .max_retries(5)
            .base_interval(Duration::from_millis(1))
            .retry_if(|e: &Error| e.is_retryable());
        let r: Result<i64> = ctx
            .transaction_with(opts, |tx| {
                Box::pin(async move {
                    tx.execute("SELECT 1", &params![]).await?;
                    TX_RUNS.fetch_add(1, Ordering::SeqCst);
                    // A plain app error is not retryable, so the predicate rejects it.
                    Err(Error::app("permanent failure"))
                })
            })
            .await;
        Ok::<_, Error>(r.is_err())
    });
    let failed: bool = engine.start_typed("nofast", "wf-txn-nofast", ()).await?;
    assert!(failed, "the error is surfaced to the caller");
    assert_eq!(
        TX_RUNS.load(Ordering::SeqCst),
        1,
        "a rejected error is not retried despite max_retries(5)"
    );
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A transaction body that fails with a *retryable* DB error (here a closed pool,
/// standing in for a transient connection blip) is retried on a fresh transaction
/// until it clears — matching Go/Python, which retry the transient set, not just
/// serialization/deadlock conflicts. Before this, durust retried only
/// `is_tx_conflict` errors, so a connection-class error fell straight through to
/// the (default: no-op) user-retry policy and failed immediately.
#[tokio::test]
async fn sqlite_transaction_retries_transient_db_error() -> Result<()> {
    use durust::{params, TransactionOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    RUNS.store(0, Ordering::SeqCst);
    let (url, path) = temp_db_url("txn-transient");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("flaky", |ctx: DurableContext, _: ()| async move {
        let n: i64 = ctx
            .transaction_with(TransactionOptions::new("t"), |tx| {
                Box::pin(async move {
                    tx.execute("SELECT 1", &params![]).await?;
                    // Fail with a retryable (connection-class) error three times,
                    // then succeed. The old conflict loop would not retry this at all.
                    if RUNS.fetch_add(1, Ordering::SeqCst) < 3 {
                        Err(durust::Error::Db(sqlx::Error::PoolClosed))
                    } else {
                        Ok(7_i64)
                    }
                })
            })
            .await?;
        Ok::<_, Error>(n)
    });
    let out: i64 = engine.start_typed("flaky", "wf-transient", ()).await?;
    assert_eq!(out, 7, "the transaction eventually commits");
    assert_eq!(
        RUNS.load(Ordering::SeqCst),
        4,
        "the body re-ran past the retryable error (3 failures + 1 success)"
    );
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// The transaction conflict/transient retry is now *unbounded* (matching Go/Python,
/// which retry until the conflict clears rather than capping). A body that always
/// fails with a retryable error would spin forever; cancelling the workflow must
/// make the loop observe the cancellation and stop with a `Cancelled` error rather
/// than hang. This exercises both the unboundedness (it retries well past the old
/// 10-cap) and the cancellation-awareness.
#[tokio::test]
async fn sqlite_transaction_conflict_retry_is_unbounded_but_cancellable() -> Result<()> {
    use durust::{params, TransactionOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};
    static SPINS: AtomicUsize = AtomicUsize::new(0);
    SPINS.store(0, Ordering::SeqCst);
    let (url, path) = temp_db_url("txn-spin-cancel");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("spinner", |ctx: DurableContext, _: ()| async move {
        ctx.transaction_with(TransactionOptions::new("t"), |tx| {
            Box::pin(async move {
                tx.execute("SELECT 1", &params![]).await?;
                SPINS.fetch_add(1, Ordering::SeqCst);
                Err::<i64, _>(durust::Error::Db(sqlx::Error::PoolClosed))
            })
        })
        .await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    let engine = Arc::new(engine);

    // Run it in the background; the conflict loop spins on the retryable error.
    let bg = engine.clone();
    let run = tokio::spawn(async move { bg.start_typed::<_, ()>("spinner", "wf-spin", ()).await });

    // Once it has spun past the old 10-attempt cap, cancel it. The loop must stop.
    for _ in 0..400 {
        if SPINS.load(Ordering::SeqCst) > 10 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        SPINS.load(Ordering::SeqCst) > 10,
        "the conflict loop retried past the old 10-attempt cap (unbounded)"
    );
    engine.cancel_workflow("wf-spin").await?;

    let res = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("cancelled transaction must stop promptly, not spin forever")
        .expect("workflow task joins");
    assert!(
        res.is_err(),
        "a cancelled spinning transaction surfaces an error, not success"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Replaying a transaction step that recorded a *failure* must return that
/// failure **immediately**, even when the step is configured with `max_retries`:
/// a durable outcome is terminal, not a fresh error to re-run against the retry
/// policy. Exercises the provider's `run_transaction_step` directly (an engine
/// `start` on an already-completed workflow short-circuits above this path, so it
/// wouldn't reach step replay at all). A large `base_interval` makes a regression
/// — spinning the user-retry loop over the recorded failure — sleep for tens of
/// seconds; the replay is asserted near-instant, with the body never re-run.
#[tokio::test]
async fn sqlite_recorded_transaction_failure_replays_immediately() -> Result<()> {
    use durust::{params, StateProvider, TxBody};
    static REC_RUNS: AtomicUsize = AtomicUsize::new(0);
    static REPLAY_BODY_RUNS: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("txn-replay");

    // Keep a handle on the provider so we can call it directly after the engine
    // has created the workflow row and the recorded failure.
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("rec", |ctx: DurableContext, _: ()| async move {
        let r: Result<i64> = ctx
            .transaction_with(
                TransactionOptions::new("boom").base_interval(Duration::from_millis(1)),
                |tx| {
                    Box::pin(async move {
                        tx.execute("SELECT 1", &params![]).await?;
                        REC_RUNS.fetch_add(1, Ordering::SeqCst);
                        Err(Error::app("always"))
                    })
                },
            )
            .await;
        Ok::<_, Error>(if r.is_err() { "caught" } else { "ok" }.to_string())
    });
    let id = "wf-txn-replay";
    let out: String = engine.start_typed("rec", id, ()).await?;
    assert_eq!(out, "caught");
    assert_eq!(
        REC_RUNS.load(Ordering::SeqCst),
        1,
        "the body ran once on record"
    );

    // Find the transaction step's function_id, then replay it directly with a
    // *retrying* policy (max_retries(3), 30s backoff). The recorded failure must
    // short-circuit ahead of the retry loop: near-instant, body not re-run. A
    // regression would sleep ~15s (three backoffs capped at max_interval) first.
    let steps = engine.get_workflow_steps(id).await?;
    let step = steps.iter().find(|s| s.name == "boom").expect("recorded");
    let seq = step.step_id;

    let opts = TransactionOptions::new("boom")
        .max_retries(3)
        .base_interval(Duration::from_secs(30));
    let body: TxBody = Box::new(|_tx| {
        Box::pin(async move {
            REPLAY_BODY_RUNS.fetch_add(1, Ordering::SeqCst);
            Err(Error::app("always"))
        })
    });
    let started = std::time::Instant::now();
    // started_at_ms is only used by the checkpoint insert, which the replay path
    // never reaches — any value works.
    let res = provider.run_transaction_step(id, seq, 0, &opts, body).await;
    let elapsed = started.elapsed();
    assert!(res.is_err(), "replay returns the recorded failure");
    assert!(
        elapsed < Duration::from_secs(5),
        "recorded failure replayed in {elapsed:?}; the retry loop must be skipped"
    );
    assert_eq!(
        REPLAY_BODY_RUNS.load(Ordering::SeqCst),
        0,
        "the body is not re-run on replay of a recorded failure"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// export_workflow captures a workflow's full durable state (status, steps,
/// events, streams); import_workflow restores it byte-for-byte after deletion.
#[tokio::test]
async fn sqlite_export_import_round_trip() -> Result<()> {
    let (url, path) = temp_db_url("export");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("expo", |ctx: DurableContext, n: i64| async move {
        let doubled = ctx
            .step("double", || async { Ok::<_, Error>(n * 2) })
            .await?;
        ctx.set_event("k", "v").await?;
        ctx.write_stream("s", 1_i64).await?;
        ctx.write_stream("s", 2_i64).await?;
        ctx.close_stream("s").await?;
        Ok::<_, Error>(doubled)
    });

    let id = "wf-export-1";
    let out: i64 = engine
        .run_workflow::<_, i64>("expo", 21_i64, WorkflowOptions::with_id(id))
        .await?
        .get_result()
        .await?;
    assert_eq!(out, 42);

    // Export, then delete the workflow (FK cascade clears its dependent rows).
    let exported = engine.export_workflow(id, false).await?;
    assert_eq!(exported.len(), 1);
    engine.delete_workflows(&[id.to_string()], false).await?;
    assert!(engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![id.to_string()],
            ..Default::default()
        })
        .await?
        .is_empty());

    // Re-import and verify every table came back.
    engine.import_workflow(&exported).await?;
    let rows = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![id.to_string()],
            load_output: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, STATUS_SUCCESS);
    assert_eq!(rows[0].output, Some(serde_json::json!(42)));

    let steps = engine.get_workflow_steps(id).await?;
    let double = steps.iter().find(|s| s.name == "double").expect("step");
    assert_eq!(double.output, Some(serde_json::json!(42)));

    let events = engine.list_workflow_events(id).await?;
    assert_eq!(events, vec![("k".to_string(), serde_json::json!("v"))]);

    let streams = engine.list_workflow_streams(id).await?;
    assert_eq!(
        streams,
        vec![(
            "s".to_string(),
            vec![serde_json::json!(1), serde_json::json!(2)]
        )]
    );

    // Importing the same workflow again fails: import never overwrites.
    assert!(engine.import_workflow(&exported).await.is_err());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// `was_forked_from` survives an export/import round trip with its correct
/// semantics: it marks the fork *source*, not the fork. The exported payload now
/// carries the flag (Python-style), so import restores it verbatim — a source
/// re-imported *alone* keeps `was_forked_from=true`, and the fork stays `false`
/// (the old bug set it from the row's own `forked_from`, which stamped the fork).
#[tokio::test]
async fn sqlite_was_forked_from_survives_import() -> Result<()> {
    let (url, path) = temp_db_url("wff-import");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("wff", |ctx: DurableContext, n: i64| async move {
        ctx.step("s", || async { Ok::<_, Error>(n) }).await
    });
    engine.launch().await?;

    // Source runs; a fork is taken from it (marks the source `was_forked_from`).
    engine
        .run_workflow::<_, i64>("wff", 1i64, WorkflowOptions::with_id("src"))
        .await?
        .get_result()
        .await?;
    // fork_workflow stamps the source and creates the fork row synchronously; the
    // fork itself need not run for the import-reconstruction check.
    engine
        .fork_workflow::<i64>("src", 0, WorkflowOptions::with_id("fork"))
        .await?;

    // Export each alone (a fork is not a child, so they export separately) and
    // delete both.
    let src_export = engine.export_workflow("src", false).await?;
    let fork_export = engine.export_workflow("fork", false).await?;
    engine
        .delete_workflows(&["src".to_string(), "fork".to_string()], false)
        .await?;

    // Import the source ALONE: carry-over restores `was_forked_from=true` from the
    // payload even with no fork in the batch to reconstruct from (the earlier
    // reconstruction-only path would have left it false here).
    engine.import_workflow(&src_export).await?;
    let sources = engine
        .list_workflows(&ListFilter {
            was_forked_from: Some(true),
            workflow_ids: vec!["src".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(
        sources.len(),
        1,
        "the source's was_forked_from carried over"
    );
    assert_eq!(sources[0].id, "src");

    // The fork imports as not-a-source.
    engine.import_workflow(&fork_export).await?;
    let not_sources = engine
        .list_workflows(&ListFilter {
            was_forked_from: Some(false),
            workflow_ids: vec!["fork".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(not_sources.len(), 1, "the fork is not a source");
    assert_eq!(not_sources[0].id, "fork");

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Fallback path: a payload that *omits* `was_forked_from` (a Go export, or a
/// Rust one from before the field was added) still recovers the flag — import
/// reconstructs it from the fork links when the source+fork pair is imported
/// together.
#[tokio::test]
async fn sqlite_was_forked_from_reconstructed_when_payload_omits_it() -> Result<()> {
    let (url, path) = temp_db_url("wff-fallback");
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("wff", |ctx: DurableContext, n: i64| async move {
        ctx.step("s", || async { Ok::<_, Error>(n) }).await
    });
    engine.launch().await?;

    engine
        .run_workflow::<_, i64>("wff", 1i64, WorkflowOptions::with_id("src"))
        .await?
        .get_result()
        .await?;
    engine
        .fork_workflow::<i64>("src", 0, WorkflowOptions::with_id("fork"))
        .await?;

    // Export the pair, then strip `was_forked_from` from every payload to mimic a
    // Go/old export that never carried the column.
    let mut exported = engine.export_workflow("src", false).await?;
    exported.extend(engine.export_workflow("fork", false).await?);
    for wf in &mut exported {
        wf.workflow_status.remove("was_forked_from");
    }
    engine
        .delete_workflows(&["src".to_string(), "fork".to_string()], false)
        .await?;
    engine.import_workflow(&exported).await?;

    // Reconstruction marks the source (a fork in the batch points at it); the fork
    // stays false.
    let sources = engine
        .list_workflows(&ListFilter {
            was_forked_from: Some(true),
            workflow_ids: vec!["src".to_string(), "fork".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(sources.len(), 1, "reconstruction marked the source");
    assert_eq!(sources[0].id, "src");

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Genuine mid-flight replay: recovering a `PENDING` workflow re-executes the
/// workflow body but replays each already-checkpointed step from storage instead
/// of re-running it.
///
/// This is the path the double-`start(sameId)` conformance tests can't reach:
/// re-submitting a *completed* id short-circuits via once-and-only-once (the
/// stored output is returned, the body never re-runs). Here the row is forced
/// back to `PENDING` — simulating a crash between the step checkpoint and
/// completion — so recovery genuinely re-runs the body; the step's checkpoint,
/// read back through real SQLite storage, must suppress its side effect.
#[tokio::test]
async fn sqlite_recovery_replays_checkpointed_step_without_rerunning() -> Result<()> {
    use durust::{StateProvider, STATUS_PENDING};

    static BODY_RUNS: AtomicUsize = AtomicUsize::new(0);
    static STEP_RUNS: AtomicUsize = AtomicUsize::new(0);

    let (url, path) = temp_db_url("recover-replay");
    let id = "wf-rec";

    // Register the same workflow on each engine instance (a fresh "process").
    let register = |engine: &mut DurableEngine| {
        engine.register("replay_me", |ctx: DurableContext, _: ()| async move {
            // Runs on every execution of the body (including a replay).
            BODY_RUNS.fetch_add(1, Ordering::SeqCst);
            let half = ctx
                .step("compute", || async {
                    // Runs only when the step actually executes, never on replay.
                    STEP_RUNS.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(21_i64)
                })
                .await?;
            Ok::<_, Error>(half * 2)
        });
    };

    // First run to completion: body runs once, the step runs and checkpoints.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: i64 = engine.start_typed("replay_me", id, ()).await?;
        assert_eq!(out, 42);
    }
    assert_eq!(BODY_RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(STEP_RUNS.load(Ordering::SeqCst), 1);

    // Simulate a crash mid-flight: flip the now-SUCCESS row back to PENDING so
    // recovery treats it as unfinished. Its step checkpoint stays in storage.
    let provider = SqliteProvider::connect(&url).await?;
    provider
        .set_workflow_status(id, STATUS_PENDING, None, None)
        .await?;

    // A fresh engine (new process) recovers the PENDING workflow.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        assert_eq!(engine.recover().await?, 1, "the PENDING workflow recovers");
    }

    // The body re-executed — a genuine replay, not an OAOO short-circuit...
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        2,
        "recovery re-runs the workflow body"
    );
    // ...but the checkpointed step was replayed from SQLite, not re-run.
    assert_eq!(
        STEP_RUNS.load(Ordering::SeqCst),
        1,
        "the checkpointed step is replayed from storage, not re-executed"
    );
    // ...and the workflow completes SUCCESS with the same result.
    let status = provider.get_workflow_status(id).await?.expect("row exists");
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(status.output, Some(serde_json::json!(42)));

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// Dequeue version gating: a claimed row's version must match the executor's
/// exactly, and an unversioned ('') row is claimable only by the executor
/// running the LATEST registered application version (no registered versions =
/// this executor counts as latest).
#[tokio::test]
async fn sqlite_dequeue_gates_by_version_and_latest() -> Result<()> {
    use durust::{DequeueRequest, StateProvider, WorkflowStatus};
    let (url, path) = temp_db_url("vgate");
    let provider = SqliteProvider::connect(&url).await?;
    provider.init().await?;

    let mk = |id: &str, ver: &str| {
        let mut s = WorkflowStatus::new(
            id,
            "wf",
            serde_json::Value::Null,
            durust::STATUS_ENQUEUED,
            "",
            ver,
        );
        s.queue_name = Some("q".into());
        s
    };
    let req = |ver: &str| DequeueRequest {
        queue_name: "q".into(),
        executor_id: "exec".into(),
        app_version: ver.into(),
        partition_key: None,
        max_tasks: 10,
        global_concurrency: None,
        rate_limit_max: None,
        rate_limit_period_ms: None,
    };
    let claim_ids = |claimed: Vec<WorkflowStatus>| {
        let mut ids: Vec<String> = claimed.into_iter().map(|w| w.id).collect();
        ids.sort();
        ids
    };

    // No versions registered: the executor counts as latest — it claims its own
    // version's row and the unversioned one, but never another version's.
    provider.insert_workflow_status(mk("r-own", "v1")).await?;
    provider.insert_workflow_status(mk("r-bare", "")).await?;
    provider.insert_workflow_status(mk("r-other", "v9")).await?;
    assert_eq!(
        claim_ids(provider.dequeue_workflows(&req("v1")).await?),
        vec!["r-bare".to_string(), "r-own".to_string()],
        "with no registered versions the executor is latest"
    );

    // Register v1 then v2 (later ⇒ latest). A v1 executor is no longer latest:
    // strict matches only. A v2 executor claims unversioned rows too.
    provider.create_application_version("v1").await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    provider.create_application_version("v2").await?;
    provider.insert_workflow_status(mk("r-own2", "v1")).await?;
    provider.insert_workflow_status(mk("r-bare2", "")).await?;
    assert_eq!(
        claim_ids(provider.dequeue_workflows(&req("v1")).await?),
        vec!["r-own2".to_string()],
        "a non-latest executor claims only exact version matches"
    );
    assert_eq!(
        claim_ids(provider.dequeue_workflows(&req("v2")).await?),
        vec!["r-bare2".to_string()],
        "the latest executor also claims unversioned rows"
    );

    // The mismatched-version row was never claimable by anyone here.
    let other = provider.get_workflow_status("r-other").await?.expect("row");
    assert_eq!(other.status, durust::STATUS_ENQUEUED);

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// An engine that claims a queued workflow it has no handler for RELEASES the
/// claim (row back to ENQUEUED) instead of stranding it PENDING under an
/// executor that can never run it — so an executor that has the handler can
/// pick it up.
#[tokio::test]
async fn sqlite_unhandled_claim_is_released_not_stranded() -> Result<()> {
    use durust::{StateProvider, WorkflowStatus};
    let (url, path) = temp_db_url("release");
    let provider = Arc::new(SqliteProvider::connect(&url).await?);

    // Engine X dispatches the queue but does NOT register "ghost".
    let mut x = DurableEngine::new(provider.clone()).await?;
    x.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));
    x.launch().await?;

    // Enqueue "ghost" by hand (an engine refuses to enqueue what it doesn't
    // know), stamped with X's version so X will claim it.
    let mut s = WorkflowStatus::new(
        "wf-ghost",
        "ghost",
        serde_json::Value::Null,
        durust::STATUS_ENQUEUED,
        "",
        x.app_version(),
    );
    s.queue_name = Some("q".into());
    provider.insert_workflow_status(s).await?;

    // Let X claim (and release) it. With the stranding bug, X's first claim
    // pins the row PENDING forever; with the release it returns to ENQUEUED
    // within a poll cycle. Poll while X keeps running — do NOT shut X down
    // first: `shutdown` aborts dispatchers and can legitimately cut a claim
    // mid-flight (documented; recovery handles it), which would race this
    // assertion.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = provider
            .get_workflow_status("wf-ghost")
            .await?
            .expect("row");
        if row.status == durust::STATUS_ENQUEUED {
            break; // released (or between claims) — never observed once stranded
        }
        assert!(
            std::time::Instant::now() < deadline,
            "row stayed {} — an unhandled claim was stranded, not released",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // An engine that has the handler completes it — impossible if X had
    // stranded the claim (the row would never be claimable again).
    let mut y = DurableEngine::new(provider.clone()).await?;
    y.register(
        "ghost",
        |_ctx: DurableContext, _: serde_json::Value| async { Ok::<_, Error>(7_i64) },
    );
    y.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));
    y.launch().await?;
    let mut h = y.retrieve_workflow::<i64>("wf-ghost").await?;
    let out = tokio::time::timeout(Duration::from_secs(20), h.get_result())
        .await
        .expect("the released workflow must be claimable by an engine with the handler")?;
    assert_eq!(out, 7);

    x.shutdown(Duration::from_secs(1)).await?;
    y.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(path);
    Ok(())
}
