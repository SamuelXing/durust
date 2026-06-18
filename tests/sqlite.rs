//! SQLite backend tests: durable state and crash-recovery across "restarts"
//! (separate engine + provider instances over the same database file).

use durust::{
    DurableContext, DurableEngine, Error, ListFilter, Result, SqliteProvider, WorkflowOptions,
    WorkflowQueue, STATUS_CANCELLED, STATUS_SUCCESS,
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
    let err = match engine.enqueue::<_, i64>("q", "double", 1_i64, opts).await {
        Ok(_) => panic!("dedup id reuse on the same queue must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), durust::ErrorCode::QueueDeduplicated);

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
