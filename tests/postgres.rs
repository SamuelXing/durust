//! Postgres backend tests. Skipped unless `DATABASE_URL` points at a reachable
//! Postgres instance (ideally an empty database — `init` runs the migrations).
//!
//!   createdb durust_test && DATABASE_URL=postgres://localhost/durust_test cargo test --test postgres

use durust::{
    DurableContext, DurableEngine, Error, ErrorCode, ListFilter, PortableWorkflowError,
    PostgresProvider, Result, Serializer, StateProvider, WorkflowOptions, WorkflowQueue,
    WorkflowStatus, STATUS_PENDING,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty())
}

async fn engine_with(url: &str, fmt: Serializer) -> Result<DurableEngine> {
    let provider = PostgresProvider::connect(url).await?.with_serializer(fmt);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("greet", |ctx: DurableContext, name: String| async move {
        let msg = ctx
            .step("build", || async { Ok::<_, Error>(format!("hi {name}")) })
            .await?;
        Ok::<_, Error>(msg)
    });
    Ok(engine)
}

/// Round-trip a workflow's input/step-output/result through Postgres, and prove
/// a provider in a different serialization format still decodes them.
#[tokio::test]
async fn pg_serialization_cross_format() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_serialization_cross_format: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("wf-ser-{}", uuid::Uuid::new_v4());

    {
        let engine = engine_with(&url, Serializer::Portable).await?;
        let out: String = engine
            .run_workflow::<_, String>("greet", "ada".to_string(), WorkflowOptions::with_id(&id))
            .await?
            .get_result()
            .await?;
        assert_eq!(out, "hi ada");
    }
    {
        let engine = engine_with(&url, Serializer::Json).await?;
        let mut handle = engine.retrieve_workflow::<String>(&id).await?;
        let status = handle.get_status().await?;
        assert_eq!(status.input, serde_json::json!("ada"));
        assert_eq!(status.output, Some(serde_json::json!("hi ada")));
        assert_eq!(handle.get_result().await?, "hi ada");
    }
    Ok(())
}

/// A workflow that fails under `portable_json` stores its error as the
/// cross-language envelope, so a reader recovers the structured `error_info`
/// (name/message); under the default format the error stays a bare string.
#[tokio::test]
async fn pg_portable_error_envelope_round_trip() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_portable_error_envelope_round_trip: DATABASE_URL unset");
        return Ok(());
    };

    let run = |fmt: Serializer, id: String| {
        let url = url.clone();
        async move {
            let provider = PostgresProvider::connect(&url).await?.with_serializer(fmt);
            let mut engine = DurableEngine::new(Arc::new(provider)).await?;
            engine.register("boom", |_ctx: DurableContext, _: ()| async move {
                Err::<(), _>(Error::app("kaboom"))
            });
            let outcome = engine
                .run_workflow::<_, ()>("boom", (), WorkflowOptions::with_id(&id))
                .await?
                .get_result()
                .await;
            assert!(outcome.is_err());
            let handle = engine.retrieve_workflow::<()>(&id).await?;
            handle.get_status().await
        }
    };

    // Portable: the structured envelope is persisted and read back.
    let portable = run(
        Serializer::Portable,
        format!("wf-errp-{}", uuid::Uuid::new_v4()),
    )
    .await?;
    assert_eq!(portable.error.as_deref(), Some("kaboom"));
    let info = portable
        .error_info
        .expect("a portable error carries structured info");
    assert_eq!(info.name, "Portable Error");
    assert_eq!(info.message, "kaboom");

    // Default: bare string, no structured info.
    let default = run(
        Serializer::Json,
        format!("wf-errd-{}", uuid::Uuid::new_v4()),
    )
    .await?;
    assert_eq!(default.error.as_deref(), Some("kaboom"));
    assert!(default.error_info.is_none());

    // A typed Error::Portable keeps its own name/code through storage, and a
    // separate reader reconstructs it from get_result.
    let typed_id = format!("wf-errt-{}", uuid::Uuid::new_v4());
    {
        let provider = PostgresProvider::connect(&url)
            .await?
            .with_serializer(Serializer::Portable);
        let mut engine = DurableEngine::new(Arc::new(provider)).await?;
        engine.register("validate", |_ctx: DurableContext, _: ()| async move {
            Err::<(), _>(Error::Portable(PortableWorkflowError {
                name: "ValidationError".to_string(),
                message: "bad email".to_string(),
                code: Some(serde_json::json!(400)),
                data: None,
            }))
        });
        let _ = engine
            .run_workflow::<_, ()>("validate", (), WorkflowOptions::with_id(&typed_id))
            .await?
            .get_result()
            .await;
    }
    {
        let provider = PostgresProvider::connect(&url)
            .await?
            .with_serializer(Serializer::Portable);
        let engine = DurableEngine::new(Arc::new(provider)).await?;
        let mut handle = engine.retrieve_workflow::<()>(&typed_id).await?;
        let info = handle
            .get_status()
            .await?
            .error_info
            .expect("typed error survives storage");
        assert_eq!(info.name, "ValidationError");
        assert_eq!(info.code, Some(serde_json::json!(400)));
        assert!(
            matches!(handle.get_result().await, Err(Error::Portable(pe)) if pe.name == "ValidationError")
        );
    }

    Ok(())
}

/// A caught step failure is checkpointed across a real restart: a fresh engine
/// over the same database replays the recorded error without re-running the step.
#[tokio::test]
async fn pg_checkpoints_a_caught_step_failure() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let Some(url) = database_url() else {
        eprintln!("skipping pg_checkpoints_a_caught_step_failure: DATABASE_URL unset");
        return Ok(());
    };
    RUNS.store(0, Ordering::SeqCst);
    let id = format!("wf-step-err-{}", uuid::Uuid::new_v4());

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
            Ok::<_, Error>(if r.is_ok() { "ok" } else { "caught-error" }.to_string())
        });
    };

    {
        let mut engine =
            DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine.start_typed("flaky_caught", &id, ()).await?;
        assert_eq!(out, "caught-error");
        let steps = engine.get_workflow_steps(&id).await?;
        let maybe = steps
            .iter()
            .find(|s| s.name == "maybe")
            .expect("step recorded");
        assert_eq!(maybe.error.as_deref(), Some("transient"));
    }
    {
        let mut engine =
            DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
        register(&mut engine);
        let out: String = engine.start_typed("flaky_caught", &id, ()).await?;
        assert_eq!(out, "caught-error", "replay observes the recorded error");
    }
    assert_eq!(
        RUNS.load(Ordering::SeqCst),
        1,
        "a checkpointed failed step is not re-run on replay"
    );
    Ok(())
}

/// The run identity round-trips through Postgres: the three auth columns are
/// written on insert (roles as a JSON array in the text column), read back into
/// the status, threaded into the workflow context, and copied onto a fork.
#[tokio::test]
async fn pg_auth_context_round_trip() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_auth_context_round_trip: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("whoami", |ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(format!(
            "{}/{}/{}",
            ctx.authenticated_user().unwrap_or("-"),
            ctx.assumed_role().unwrap_or("-"),
            ctx.authenticated_roles().join(","),
        ))
    });

    let id = format!("wf-auth-{tag}");
    let opts = WorkflowOptions::with_id(&id)
        .authenticated_user("alice")
        .assumed_role("admin")
        .authenticated_roles(["admin", "user"]);
    let mut handle = engine.run_workflow::<_, String>("whoami", (), opts).await?;
    assert_eq!(handle.get_result().await?, "alice/admin/admin,user");

    let status = handle.get_status().await?;
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(status.assumed_role.as_deref(), Some("admin"));
    assert_eq!(status.authenticated_roles, vec!["admin", "user"]);

    let fork_id = format!("wf-auth-fork-{tag}");
    let forked = engine
        .fork_workflow::<String>(&id, 0, WorkflowOptions::with_id(&fork_id))
        .await?;
    let fstatus = forked.get_status().await?;
    assert_eq!(fstatus.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(fstatus.authenticated_roles, vec!["admin", "user"]);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Child workflows round-trip through Postgres: a parent starts a child, the
/// child is linked via `parent_workflow_id` and the `child_workflow_id`
/// checkpoint, and inherits the parent's identity.
#[tokio::test]
async fn pg_child_workflow() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_child_workflow: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("child", |ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(format!("{n}:{}", ctx.authenticated_user().unwrap_or("-")))
    });
    engine.register("parent", |ctx: DurableContext, n: i64| async move {
        let mut child = ctx
            .start_workflow::<_, String>("child", n, WorkflowOptions::default())
            .await?;
        child.get_result().await
    });

    let parent_id = format!("parent-{tag}");
    let opts = WorkflowOptions::with_id(&parent_id).authenticated_user("alice");
    let mut handle = engine
        .run_workflow::<_, String>("parent", 5_i64, opts)
        .await?;
    assert_eq!(handle.get_result().await?, "5:alice");

    let child = engine
        .retrieve_workflow::<String>(&format!("{parent_id}-0"))
        .await?;
    let status = child.get_status().await?;
    assert_eq!(
        status.parent_workflow_id.as_deref(),
        Some(parent_id.as_str())
    );
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Step introspection round-trips through Postgres: `get_workflow_steps` returns
/// recorded steps (output decoded per serialization) and the child link, ordered.
#[tokio::test]
async fn pg_workflow_steps() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_workflow_steps: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("kid", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register("worker", |ctx: DurableContext, _: ()| async move {
        let v = ctx
            .step("compute", || async { Ok::<_, Error>(42_i64) })
            .await?;
        let mut child = ctx
            .start_workflow::<_, i64>("kid", v, WorkflowOptions::default())
            .await?;
        child.get_result().await
    });

    let id = format!("steps-{tag}");
    let _: i64 = engine
        .run_workflow::<_, i64>("worker", (), WorkflowOptions::with_id(&id))
        .await?
        .get_result()
        .await?;

    let steps = engine.get_workflow_steps(&id).await?;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_id, 0);
    assert_eq!(steps[0].name, "compute");
    assert_eq!(steps[0].output, Some(serde_json::json!(42)));
    assert_eq!(steps[1].name, "kid");
    assert_eq!(
        steps[1].child_workflow_id.as_deref(),
        Some(format!("{id}-1").as_str())
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Code patching round-trips through Postgres: a new workflow takes the patched
/// path and the marker is recorded; a pre-patch workflow takes the old path.
#[tokio::test]
async fn pg_patch() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_patch: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("wf", |ctx: DurableContext, _: ()| async move {
        ctx.patch("feature").await
    });

    // A brand-new workflow takes the new path and records the marker.
    let fresh = format!("patch-new-{tag}");
    let patched: bool = engine
        .run_workflow::<_, bool>("wf", (), WorkflowOptions::with_id(&fresh))
        .await?
        .get_result()
        .await?;
    assert!(patched);
    let steps = engine.get_workflow_steps(&fresh).await?;
    assert_eq!(steps[0].name, "DBOS.patch-feature");

    // A workflow with a different step already at seq 0 takes the old path.
    let old = format!("patch-old-{tag}");
    let provider2 = PostgresProvider::connect(&url).await?;
    provider2
        .insert_workflow_status(WorkflowStatus::new(
            &old,
            "wf",
            serde_json::Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;
    provider2
        .record_step_result(&old, 0, "legacy_step", serde_json::json!(1), None, None)
        .await?;
    let patched: bool = engine
        .run_workflow::<_, bool>("wf", (), WorkflowOptions::with_id(&old))
        .await?
        .get_result()
        .await?;
    assert!(!patched, "a pre-patch workflow stays on the old path");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A durable `select` round-trips through Postgres: the winning index and value
/// are recorded as a `DBOS.select` step.
#[tokio::test]
async fn pg_select() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_select: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("racer", |ctx: DurableContext, _: ()| async move {
        let branches: Vec<Pin<Box<dyn Future<Output = i64> + Send>>> = vec![
            Box::pin(async { 7_i64 }),
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                8_i64
            }),
        ];
        ctx.select(branches).await
    });

    let id = format!("select-{tag}");
    let (index, value): (usize, i64) = engine
        .run_workflow::<_, (usize, i64)>("racer", (), WorkflowOptions::with_id(&id))
        .await?
        .get_result()
        .await?;
    assert_eq!((index, value), (0, 7));

    let steps = engine.get_workflow_steps(&id).await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].name, "DBOS.select");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A durable stream round-trips through Postgres: a producer writes values and
/// closes the stream, and an external reader drains them in order and sees the
/// close.
#[tokio::test]
async fn pg_stream_round_trip() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_stream_round_trip: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("stream-{}", uuid::Uuid::new_v4());

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("producer", |ctx: DurableContext, _: ()| async move {
        for i in 0..3_i64 {
            ctx.write_stream("nums", i).await?;
        }
        ctx.close_stream("nums").await?;
        Ok::<_, Error>(())
    });

    engine
        .run_workflow::<_, ()>("producer", (), WorkflowOptions::with_id(&id))
        .await?
        .get_result()
        .await?;

    let (values, closed): (Vec<i64>, bool) = engine.read_stream(&id, "nums").await?;
    assert_eq!(values, vec![0, 1, 2]);
    assert!(closed);

    // A snapshot read from an offset returns only the tail, without blocking.
    let (tail, closed): (Vec<i64>, bool) = engine.read_stream_snapshot(&id, "nums", 2).await?;
    assert_eq!(tail, vec![2]);
    assert!(closed);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Postgres surfaces the dedup unique-violation and the destination FK violation
/// as typed, classifiable errors (verifies the sqlx Postgres driver mapping).
#[tokio::test]
async fn pg_typed_db_errors() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_typed_db_errors: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new(format!("q-{tag}")));

    let dedup = format!("once-{tag}");
    let mut opts = WorkflowOptions::with_id(format!("wf-a-{tag}"));
    opts.dedup_id = Some(dedup.clone());
    engine
        .enqueue::<_, ()>(&format!("q-{tag}"), "noop", (), opts)
        .await?;

    let mut opts = WorkflowOptions::with_id(format!("wf-b-{tag}"));
    opts.dedup_id = Some(dedup);
    let err = match engine
        .enqueue::<_, ()>(&format!("q-{tag}"), "noop", (), opts)
        .await
    {
        Ok(_) => panic!("dedup reuse must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::QueueDeduplicated);

    let err = engine
        .send(&format!("ghost-{tag}"), 1_i64, "topic")
        .await
        .expect_err("send to nonexistent workflow must fail");
    assert_eq!(err.code(), ErrorCode::NonExistentWorkflow);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Global concurrency caps how many of a queue's workflows run at once, even
/// under the snapshot-isolation (`REPEATABLE READ` + `FOR UPDATE NOWAIT`) dequeue
/// path it triggers. With a cap of 1, the observed peak must stay 1.
#[tokio::test]
async fn pg_global_concurrency_caps_running() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CURRENT: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    let Some(url) = database_url() else {
        eprintln!("skipping pg_global_concurrency_caps_running: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("track", |ctx: DurableContext, _: ()| async move {
        let now = CURRENT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(now, Ordering::SeqCst);
        ctx.sleep(Duration::from_millis(60)).await?;
        CURRENT.fetch_sub(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(
        WorkflowQueue::new("gc")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;

    let mut handles = Vec::new();
    for i in 0..4 {
        handles.push(
            engine
                .enqueue::<_, ()>(
                    "gc",
                    "track",
                    (),
                    WorkflowOptions::with_id(format!("gc-{tag}-{i}")),
                )
                .await?,
        );
    }
    for mut h in handles {
        h.get_result().await?;
    }
    assert_eq!(
        PEAK.load(Ordering::SeqCst),
        1,
        "global_concurrency(1) must keep at most one workflow running"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Bulk cancel / resume / delete through Postgres: exercises the `ANY($1)`
/// statements, the `RETURNING` resume, and the recursive-CTE child delete (with
/// FK cascade).
#[tokio::test]
async fn pg_bulk_ops() -> Result<()> {
    use durust::{STATUS_CANCELLED, STATUS_SUCCESS};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_bulk_ops: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let id = |s: &str| format!("{s}-{tag}");
    // A unique app version isolates this test's internal-queue work from other
    // parallel tests' engines sharing the database (the dispatch version gate).
    let ver = format!("v-{tag}");

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new_with_version(Arc::new(provider), &ver).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    // Resume re-queues work for a dispatcher, so the engine must be live.
    engine.launch().await?;
    let provider = PostgresProvider::connect(&url).await?;

    let seed = |wid: String, status: &str, parent: Option<String>| {
        let mut s = WorkflowStatus::new(wid, "noop", serde_json::Value::Null, status, "", &ver);
        s.parent_workflow_id = parent;
        s
    };

    for s in ["wf-1", "wf-2", "wf-3"] {
        provider
            .insert_workflow_status(seed(id(s), STATUS_PENDING, None))
            .await?;
    }

    // Bulk cancel a subset + a missing id (skipped, no error).
    engine
        .cancel_workflows(&[id("wf-1"), id("wf-2"), id("ghost")])
        .await?;
    assert_eq!(
        provider
            .get_workflow_status(&id("wf-1"))
            .await?
            .unwrap()
            .status,
        STATUS_CANCELLED
    );
    assert_eq!(
        provider
            .get_workflow_status(&id("wf-3"))
            .await?
            .unwrap()
            .status,
        STATUS_PENDING
    );

    // Bulk resume returns a handle per transitioned id.
    let handles = engine
        .resume_workflows::<()>(&[id("wf-1"), id("wf-2")])
        .await?;
    assert_eq!(handles.len(), 2);
    for mut h in handles {
        h.get_result().await?;
    }
    assert_eq!(
        provider
            .get_workflow_status(&id("wf-1"))
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS
    );

    // Recursive delete: parent → child → grandchild.
    provider
        .insert_workflow_status(seed(id("p"), STATUS_SUCCESS, None))
        .await?;
    provider
        .insert_workflow_status(seed(id("c"), STATUS_SUCCESS, Some(id("p"))))
        .await?;
    provider
        .insert_workflow_status(seed(id("gc"), STATUS_SUCCESS, Some(id("c"))))
        .await?;
    engine.delete_workflows(&[id("p")], true).await?;
    assert!(provider.get_workflow_status(&id("p")).await?.is_none());
    assert!(provider.get_workflow_status(&id("c")).await?.is_none());
    assert!(provider.get_workflow_status(&id("gc")).await?.is_none());

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A partitioned queue persists the partition key and dispatches each partition
/// independently through Postgres.
#[tokio::test]
async fn pg_partitioned_queue_dispatch() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_partitioned_queue_dispatch: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register_queue(
        WorkflowQueue::new("pq")
            .partitioned()
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;

    let east = format!("east-{tag}");
    let west = format!("west-{tag}");
    let mut a = engine
        .enqueue::<_, i64>(
            "pq",
            "echo",
            1_i64,
            WorkflowOptions::with_id(&east).partition_key("east"),
        )
        .await?;
    let mut b = engine
        .enqueue::<_, i64>(
            "pq",
            "echo",
            2_i64,
            WorkflowOptions::with_id(&west).partition_key("west"),
        )
        .await?;
    assert_eq!(a.get_result().await?, 1);
    assert_eq!(b.get_result().await?, 2);
    assert_eq!(
        a.get_status().await?.queue_partition_key.as_deref(),
        Some("east")
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// set_workflow_delay reschedules a DELAYED workflow through Postgres: a
/// far-future delay is shortened so the dispatcher runs it promptly; a
/// non-DELAYED row is a no-op.
#[tokio::test]
async fn pg_set_workflow_delay() -> Result<()> {
    use durust::STATUS_DELAYED;
    let Some(url) = database_url() else {
        eprintln!("skipping pg_set_workflow_delay: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let wid = format!("wf-delay-{tag}");

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine
        .register_queue(WorkflowQueue::new("dq").base_polling_interval(Duration::from_millis(10)));
    engine.launch().await?;

    let mut opts = WorkflowOptions::with_id(&wid);
    opts.delay = Some(Duration::from_secs(60));
    let mut handle = engine.enqueue::<_, i64>("dq", "echo", 8_i64, opts).await?;
    assert_eq!(handle.get_status().await?.status, STATUS_DELAYED);

    let started = std::time::Instant::now();
    assert!(
        engine
            .set_workflow_delay(&wid, Duration::from_millis(20))
            .await?
    );
    assert_eq!(handle.get_result().await?, 8);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "must run on the shortened delay, not the original 60s"
    );

    // Completed → no longer DELAYED → silent no-op.
    assert!(!engine.set_workflow_delay(&wid, Duration::ZERO).await?);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The extended list filters work through Postgres: has_parent and the
/// load_input/load_output column substitution.
#[tokio::test]
async fn pg_list_filters_extended() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_list_filters_extended: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("child", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * 10)
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        let mut h = ctx
            .start_workflow::<i64, i64>("child", 5_i64, WorkflowOptions::default())
            .await?;
        h.get_result().await
    });
    let pid = format!("parent-{tag}");
    let out: i64 = engine.start_typed("parent", &pid, ()).await?;
    assert_eq!(out, 50);

    let child_id = format!("{pid}-0");
    let lean = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![child_id.clone()],
            has_parent: Some(true),
            load_input: false,
            load_output: false,
            ..Default::default()
        })
        .await?;
    assert_eq!(lean.len(), 1);
    assert_eq!(lean[0].id, child_id);
    assert_eq!(lean[0].input, serde_json::Value::Null);
    assert!(lean[0].output.is_none());

    let full = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![child_id],
            ..Default::default()
        })
        .await?;
    assert_eq!(full[0].output, Some(serde_json::json!(50)));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// get_workflow_aggregates groups through Postgres (by status, with a time
/// bucket).
#[tokio::test]
async fn pg_workflow_aggregates() -> Result<()> {
    use durust::{WorkflowAggregateQuery, STATUS_SUCCESS};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_workflow_aggregates: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("agg_ok", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    let prefix = format!("agg-{tag}-");
    for i in 0..3 {
        engine
            .start_typed::<_, ()>("agg_ok", &format!("{prefix}{i}"), ())
            .await?;
    }

    // Group by status, scoped to this run's id prefix; select count + latency.
    let by_status = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            select_min_created_at: true,
            select_max_total_latency_ms: true,
            workflow_id_prefix: Some(prefix.clone()),
            ..Default::default()
        })
        .await?;
    assert_eq!(by_status.len(), 1);
    assert_eq!(
        by_status[0].group.get("status"),
        Some(&Some(STATUS_SUCCESS.to_string()))
    );
    assert_eq!(by_status[0].count, Some(3));
    assert!(by_status[0].min_created_at.is_some());
    assert!(by_status[0].max_total_latency_ms.is_some_and(|l| l >= 0));

    // A wide time bucket collapses them into one group with a time_bucket key.
    let bucketed = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            select_count: true,
            time_bucket_ms: Some(3_600_000),
            workflow_id_prefix: Some(prefix),
            ..Default::default()
        })
        .await?;
    assert_eq!(bucketed.len(), 1);
    assert_eq!(bucketed[0].count, Some(3));
    assert!(bucketed[0].group.contains_key("time_bucket"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Step aggregates through Postgres: count + max duration grouped by function
/// name, scoped to this run via the id prefix.
#[tokio::test]
async fn pg_step_aggregates() -> Result<()> {
    use durust::StepAggregateQuery;
    let Some(url) = database_url() else {
        eprintln!("skipping pg_step_aggregates: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("stepagg-{}", uuid::Uuid::new_v4());

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
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
    engine.start_typed::<_, ()>("work", &id, ()).await?;

    let by_fn = engine
        .get_step_aggregates(&StepAggregateQuery {
            by_function_name: true,
            select_count: true,
            select_max_duration_ms: true,
            workflow_id_prefix: Some(id.clone()),
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

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Schedule CRUD round-trips through Postgres: create with context + queue, read
/// it back, pause (reflected in a status filter), and delete. A uuid-suffixed
/// name keeps it isolated from other tests sharing the database.
#[tokio::test]
async fn pg_schedule_crud() -> Result<()> {
    use durust::{ScheduleFilter, ScheduleOptions, ScheduleStatus};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_schedule_crud: DATABASE_URL unset");
        return Ok(());
    };

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register(
        "nightly_job",
        |_ctx: DurableContext, _: String| async move { Ok::<_, Error>(()) },
    );

    let name = format!("nightly-{}", uuid::Uuid::new_v4());
    engine
        .create_schedule(
            &name,
            "nightly_job",
            "0 0 0 * * *",
            ScheduleOptions::new()
                .context(&serde_json::json!({"region": "us"}))
                .queue_name("internal"),
        )
        .await?;

    let got = engine.get_schedule(&name).await?.expect("persisted");
    assert_eq!(got.workflow_name, "nightly_job");
    assert_eq!(got.status, ScheduleStatus::Active);
    assert_eq!(got.queue_name.as_deref(), Some("internal"));
    assert_eq!(
        got.context.as_ref().and_then(|v| v.get("region")),
        Some(&serde_json::json!("us"))
    );

    // A duplicate name is rejected.
    assert!(engine
        .create_schedule(&name, "nightly_job", "0 0 0 * * *", ScheduleOptions::new())
        .await
        .is_err());

    assert!(engine.pause_schedule(&name).await?);
    let active = engine
        .list_schedules(&ScheduleFilter {
            statuses: vec![ScheduleStatus::Active],
            name_prefixes: vec![name.clone()],
            ..Default::default()
        })
        .await?;
    assert!(
        active.is_empty(),
        "paused schedule drops out of the active filter"
    );

    assert!(engine.delete_schedule(&name).await?);
    assert!(engine.get_schedule(&name).await?.is_none());
    Ok(())
}

/// Through Postgres: `apply_schedules` creates/replaces a schedule,
/// `backfill_schedule` persists one run per past tick (idempotently), and
/// `trigger_schedule` runs it once now. A uuid-suffixed name isolates the test.
#[tokio::test]
async fn pg_schedule_backfill_apply_trigger() -> Result<()> {
    use chrono::{TimeZone, Utc};
    use durust::{ApplySchedule, ScheduleOptions, WorkflowHandle};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_schedule_backfill_apply_trigger: DATABASE_URL unset");
        return Ok(());
    };

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register(
        "nightly_job",
        |_ctx: DurableContext, _: String| async move { Ok::<_, Error>(()) },
    );

    let name = format!("bf-{}", uuid::Uuid::new_v4());
    engine
        .apply_schedules(vec![ApplySchedule::new(
            &name,
            "nightly_job",
            "0 0 12 * * *",
        )])
        .await?;
    let created = engine.get_schedule(&name).await?.expect("applied");
    assert_eq!(created.schedule, "0 0 12 * * *");

    // Re-apply with a new spec: replaced with a fresh schedule_id.
    engine
        .apply_schedules(vec![ApplySchedule::new(
            &name,
            "nightly_job",
            "0 0 6 * * *",
        )
        .options(ScheduleOptions::new())])
        .await?;
    let replaced = engine.get_schedule(&name).await?.expect("replaced");
    assert_ne!(replaced.schedule_id, created.schedule_id);
    assert_eq!(replaced.schedule, "0 0 6 * * *");

    // Backfill three past ticks (06:00 daily).
    let start = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();
    let ids = engine.backfill_schedule(&name, start, end).await?;
    assert_eq!(ids.len(), 3, "one tick per day");

    let prefix = format!("sched-{name}-");
    let filter = ListFilter {
        workflow_id_prefix: Some(prefix.clone()),
        ..Default::default()
    };
    for _ in 0..100 {
        let rows = engine.list_workflows(&filter).await?;
        if rows.len() == 3 && rows.iter().all(|r| r.status == "SUCCESS") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let rows = engine.list_workflows(&filter).await?;
    assert_eq!(rows.len(), 3, "one persisted row per backfilled tick");

    // Re-backfill is idempotent.
    assert_eq!(engine.backfill_schedule(&name, start, end).await?, ids);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(engine.list_workflows(&filter).await?.len(), 3);

    // Trigger runs once now, under a distinct -trigger- id.
    let mut handle: WorkflowHandle<()> = engine.trigger_schedule(&name).await?;
    assert!(handle.id().starts_with(&format!("sched-{name}-trigger-")));
    handle.get_result().await?;

    engine.delete_schedule(&name).await?;
    Ok(())
}

/// Application version registry through Postgres. The DB is shared and tests run
/// in parallel, so versions are uuid-suffixed and only the *relative* order of
/// this test's own two versions is asserted (global "latest" would be racy).
#[tokio::test]
async fn pg_application_versions() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_application_versions: DATABASE_URL unset");
        return Ok(());
    };
    let v_a = format!("va-{}", uuid::Uuid::new_v4());
    let v_b = format!("vb-{}", uuid::Uuid::new_v4());

    // Index of a version_name within the (newest-first) list.
    async fn index_of(engine: &DurableEngine, name: &str) -> Result<usize> {
        let versions = engine.list_application_versions().await?;
        Ok(versions
            .iter()
            .position(|v| v.version_name == name)
            .expect("version present"))
    }

    // Register v_a, then v_b later, so v_b sorts ahead of v_a.
    let a = DurableEngine::new_with_version(
        Arc::new(PostgresProvider::connect(&url).await?),
        v_a.clone(),
    )
    .await?;
    a.launch().await?;
    a.shutdown(Duration::from_secs(1)).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let b = DurableEngine::new_with_version(
        Arc::new(PostgresProvider::connect(&url).await?),
        v_b.clone(),
    )
    .await?;
    b.launch().await?;
    b.shutdown(Duration::from_secs(1)).await?;

    assert!(
        index_of(&a, &v_b).await? < index_of(&a, &v_a).await?,
        "newer version sorts first"
    );

    // Promote v_a: it now sorts ahead of v_b.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(a.set_latest_application_version(&v_a).await?);
    assert!(
        index_of(&a, &v_a).await? < index_of(&a, &v_b).await?,
        "promoted version sorts first"
    );

    // Unknown version is a no-op.
    assert!(
        !a.set_latest_application_version(&format!("nope-{}", uuid::Uuid::new_v4()))
            .await?
    );
    Ok(())
}

/// An out-of-process `Client` enqueues work over Postgres that a separate engine
/// (sharing the database) claims and runs; the client observes the result. The
/// queue and workflow names are uuid-suffixed so parallel tests don't cross-claim.
#[tokio::test]
async fn pg_client_enqueues_work_an_engine_runs() -> Result<()> {
    use durust::{Client, WorkflowHandle, WorkflowQueue};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_client_enqueues_work_an_engine_runs: DATABASE_URL unset");
        return Ok(());
    };
    let wf = format!("double-{}", uuid::Uuid::new_v4());
    let queue = format!("q-{}", uuid::Uuid::new_v4());
    let job = format!("job-{}", uuid::Uuid::new_v4());

    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(&wf, |ctx: DurableContext, n: i64| async move {
        ctx.step("mul", || async { Ok::<_, Error>(n * 2) }).await
    });
    engine.register_queue(WorkflowQueue::new(&queue));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some(job.clone()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, i64>(&queue, &wf, 21i64, opts).await?;
    assert_eq!(handle.get_result().await?, 42);

    let steps = client.get_workflow_steps(&job).await?;
    assert!(steps.iter().any(|s| s.name == "mul"));
    let mut again: WorkflowHandle<i64> = client.retrieve_workflow(&job).await?;
    assert_eq!(again.get_result().await?, 42);

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// Client management over Postgres: reschedule a DELAYED workflow so the engine
/// runs it, and cancel + delete another. Names are uuid-scoped for isolation.
#[tokio::test]
async fn pg_client_manages_workflows() -> Result<()> {
    use durust::WorkflowQueue;
    let Some(url) = database_url() else {
        eprintln!("skipping pg_client_manages_workflows: DATABASE_URL unset");
        return Ok(());
    };
    let wf = format!("ping-{}", uuid::Uuid::new_v4());
    let queue = format!("q-{}", uuid::Uuid::new_v4());
    let run_id = format!("run-{}", uuid::Uuid::new_v4());
    let cancel_id = format!("cancel-{}", uuid::Uuid::new_v4());

    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(&wf, |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new(&queue));
    engine.launch().await?;
    let client = durust::Client::new(provider.clone());

    // Enqueue far in the future, then pull it in: the engine runs it.
    let mut run = client
        .enqueue::<_, ()>(
            &queue,
            &wf,
            (),
            WorkflowOptions {
                workflow_id: Some(run_id.clone()),
                delay: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await?;
    assert!(
        client
            .set_workflow_delay(&run_id, Duration::from_millis(10))
            .await?
    );
    run.get_result().await?;

    // Enqueue another, cancel it, then delete it.
    client
        .enqueue::<_, ()>(
            &queue,
            &wf,
            (),
            WorkflowOptions {
                workflow_id: Some(cancel_id.clone()),
                delay: Some(Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await?;
    client.cancel_workflow(&cancel_id).await?;
    let one = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some(cancel_id.clone()),
            ..Default::default()
        })
        .await?;
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].status, "CANCELLED");
    client
        .delete_workflows(std::slice::from_ref(&cancel_id), false)
        .await?;
    assert!(client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some(cancel_id),
            ..Default::default()
        })
        .await?
        .is_empty());

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// Client schedule management persists through Postgres: create with context +
/// queue, read back, pause (drops from the active filter), apply (replace), and
/// delete. A uuid-suffixed name isolates it; no engine runs, so nothing fires.
#[tokio::test]
async fn pg_client_manages_schedules() -> Result<()> {
    use durust::{ApplySchedule, Client, ScheduleFilter, ScheduleOptions, ScheduleStatus};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_client_manages_schedules: DATABASE_URL unset");
        return Ok(());
    };
    let name = format!("nightly-{}", uuid::Uuid::new_v4());
    let client = Client::new(Arc::new(PostgresProvider::connect(&url).await?));

    client
        .create_schedule(
            &name,
            "report",
            "0 0 0 * * *",
            ScheduleOptions::new()
                .context(&serde_json::json!({"region": "us"}))
                .queue_name("internal"),
        )
        .await?;
    let got = client.get_schedule(&name).await?.expect("created");
    assert_eq!(got.workflow_name, "report");
    assert_eq!(got.queue_name.as_deref(), Some("internal"));

    // Duplicate rejected.
    assert!(client
        .create_schedule(&name, "report", "0 0 0 * * *", ScheduleOptions::new())
        .await
        .is_err());

    assert!(client.pause_schedule(&name).await?);
    assert!(client
        .list_schedules(&ScheduleFilter {
            statuses: vec![ScheduleStatus::Active],
            name_prefixes: vec![name.clone()],
            ..Default::default()
        })
        .await?
        .is_empty());

    // apply replaces it with a fresh id.
    let before = got.schedule_id;
    client
        .apply_schedules(vec![ApplySchedule::new(&name, "report", "0 0 1 * * *")])
        .await?;
    let after = client.get_schedule(&name).await?.expect("replaced");
    assert_ne!(after.schedule_id, before);
    assert_eq!(after.schedule, "0 0 1 * * *");

    assert!(client.delete_schedule(&name).await?);
    assert!(client.get_schedule(&name).await?.is_none());
    Ok(())
}

/// A client backfills and triggers a direct schedule on Postgres: backfilled
/// ticks and the trigger land ENQUEUED on the internal queue under their
/// deterministic ids, and re-backfilling the same window is idempotent. The
/// client pins a unique application version so the dequeue version gate keeps
/// any other test's running engine from claiming these ticks off the shared
/// internal queue — the rows stay enqueued, as asserted.
#[tokio::test]
async fn pg_client_backfills_and_triggers_a_schedule() -> Result<()> {
    use chrono::{TimeZone, Utc};
    use durust::{Client, ScheduleOptions};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_client_backfills_and_triggers_a_schedule: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let name = format!("bf-{tag}");
    let client = Client::new(Arc::new(PostgresProvider::connect(&url).await?))
        .with_app_version(format!("v-{tag}"));

    client
        .create_schedule(&name, "report", "0 0 12 * * *", ScheduleOptions::new())
        .await?;

    let start = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();
    let ids = client.backfill_schedule(&name, start, end).await?;
    assert_eq!(ids.len(), 3, "one tick per day");

    let backfilled = ListFilter {
        workflow_id_prefix: Some(format!("sched-{name}-")),
        ..Default::default()
    };
    let rows = client.list_workflows(&backfilled).await?;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.status == "ENQUEUED"));
    assert!(rows
        .iter()
        .all(|r| r.queue_name.as_deref() == Some("_dbos_internal_queue")));

    // Idempotent re-backfill: same ids, no new rows.
    assert_eq!(client.backfill_schedule(&name, start, end).await?, ids);

    // Trigger enqueues one more run under a distinct `-trigger-` id.
    let h: durust::WorkflowHandle<()> = client.trigger_schedule(&name).await?;
    assert!(h.id().starts_with(&format!("sched-{name}-trigger-")));
    let trig = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some(format!("sched-{name}-trigger-")),
            ..Default::default()
        })
        .await?;
    assert_eq!(trig.len(), 1, "trigger enqueued exactly one run");

    client.delete_schedule(&name).await?;
    Ok(())
}

/// Portable mode stores a workflow's input in the cross-language args envelope
/// `{"positionalArgs":[…],"namedArgs":{}}` (so a DBOS app in another language can
/// run it), while the output stays a bare portable value; the input reads back
/// unwrapped to what the workflow receives.
#[tokio::test]
async fn pg_portable_input_envelope() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_portable_input_envelope: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("wf-env-{}", uuid::Uuid::new_v4());

    let engine = engine_with(&url, Serializer::Portable).await?;
    let out: String = engine
        .run_workflow::<_, String>("greet", "ada".to_string(), WorkflowOptions::with_id(&id))
        .await?
        .get_result()
        .await?;
    assert_eq!(out, "hi ada");

    // Read the raw columns: input is the args envelope, output is a bare value.
    let pool = sqlx::PgPool::connect(&url).await?;
    let raw_inputs: String =
        sqlx::query_scalar("SELECT inputs FROM workflow_status WHERE workflow_uuid = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(raw_inputs, r#"{"positionalArgs":["ada"],"namedArgs":{}}"#);
    let raw_output: String =
        sqlx::query_scalar("SELECT output FROM workflow_status WHERE workflow_uuid = $1")
            .bind(&id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        raw_output, r#""hi ada""#,
        "output is a bare portable value, not enveloped"
    );

    // Through the provider, the envelope is unwrapped to the bare input.
    let status = engine
        .retrieve_workflow::<String>(&id)
        .await?
        .get_status()
        .await?;
    assert_eq!(status.input, serde_json::json!("ada"));
    Ok(())
}

/// On Postgres, the client honors a per-enqueue application-version override and
/// the `ReturnExisting` dedup policy (a colliding dedup id returns the holder).
#[tokio::test]
async fn pg_enqueue_dedup_and_app_version() -> Result<()> {
    use durust::{Client, DeduplicationPolicy, WorkflowHandle};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_enqueue_dedup_and_app_version: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let queue = format!("dq-{tag}");
    let dedup = format!("once-{tag}");
    let client = Client::new(Arc::new(PostgresProvider::connect(&url).await?));

    // Per-enqueue application-version override.
    let ver_id = format!("wf-ver-{tag}");
    let _: WorkflowHandle<i64> = client
        .enqueue(
            &queue,
            "wf",
            1i64,
            WorkflowOptions::with_id(&ver_id).app_version("v-special"),
        )
        .await?;
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some(ver_id.clone()),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows[0].app_version, "v-special");

    // ReturnExisting returns the holder; the default rejects.
    let first: WorkflowHandle<i64> = client
        .enqueue(
            &queue,
            "wf",
            1i64,
            WorkflowOptions::with_id(format!("d1-{tag}")).dedup_id(&dedup),
        )
        .await?;
    assert!(client
        .enqueue::<_, i64>(
            &queue,
            "wf",
            2i64,
            WorkflowOptions::with_id(format!("d2-{tag}")).dedup_id(&dedup),
        )
        .await
        .is_err());
    let again: WorkflowHandle<i64> = client
        .enqueue(
            &queue,
            "wf",
            3i64,
            WorkflowOptions::with_id(format!("d3-{tag}"))
                .dedup_id(&dedup)
                .dedup_policy(DeduplicationPolicy::ReturnExisting),
        )
        .await?;
    assert_eq!(again.id(), first.id());

    client
        .delete_workflows(&[first.id().to_string(), ver_id], false)
        .await?;
    Ok(())
}

/// A deduplication id is released once its holder reaches a terminal state, so
/// the same id can be enqueued again afterward.
#[tokio::test]
async fn pg_dedup_slot_frees_on_completion() -> Result<()> {
    use durust::{Client, WorkflowHandle, STATUS_SUCCESS};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_dedup_slot_frees_on_completion: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let queue = format!("dq-{tag}");
    let dedup = format!("once-{tag}");
    let d1 = format!("d1-{tag}");
    let d3 = format!("d3-{tag}");
    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let client = Client::new(provider.clone());

    let first: WorkflowHandle<i64> = client
        .enqueue(
            &queue,
            "wf",
            1i64,
            WorkflowOptions::with_id(&d1).dedup_id(&dedup),
        )
        .await?;
    // The partial unique index rejects a colliding dedup id while d1 is active.
    assert!(client
        .enqueue::<_, i64>(
            &queue,
            "wf",
            2i64,
            WorkflowOptions::with_id(format!("d2-{tag}")).dedup_id(&dedup),
        )
        .await
        .is_err());

    // Completing d1 nulls its deduplication_id, freeing the slot.
    provider
        .set_workflow_status(first.id(), STATUS_SUCCESS, None, None)
        .await?;

    let third: WorkflowHandle<i64> = client
        .enqueue(
            &queue,
            "wf",
            3i64,
            WorkflowOptions::with_id(&d3).dedup_id(&dedup),
        )
        .await?;
    assert_eq!(third.id(), d3);

    client.delete_workflows(&[d1, d3], false).await?;
    Ok(())
}

/// A workflow cancelled during its final step must stay cancelled: a late
/// SUCCESS/ERROR completion is rejected and does not overwrite the status.
#[tokio::test]
async fn pg_completion_cannot_overwrite_cancelled() -> Result<()> {
    use durust::{Client, WorkflowHandle, STATUS_CANCELLED, STATUS_SUCCESS};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_completion_cannot_overwrite_cancelled: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let id = format!("c1-{tag}");
    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let client = Client::new(provider.clone());

    let h: WorkflowHandle<i64> = client
        .enqueue(
            &format!("q-{tag}"),
            "wf",
            1i64,
            WorkflowOptions::with_id(&id),
        )
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

    client.delete_workflows(&[id], false).await?;
    Ok(())
}

/// Through Postgres: `apply_schedules` is atomic — a mid-batch failure rolls the
/// whole batch back, leaving any schedule it would have replaced untouched.
#[tokio::test]
async fn pg_apply_schedules_is_atomic() -> Result<()> {
    use durust::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_apply_schedules_is_atomic: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let prefix = format!("sch-{tag}-");
    let keep = format!("{prefix}keep");
    let other = format!("{prefix}other");
    let dup = format!("dup-{tag}");
    let provider = PostgresProvider::connect(&url).await?;

    let make = |id: String, name: String, cron: &str| WorkflowSchedule {
        schedule_id: id,
        schedule_name: name,
        workflow_name: "wf".to_string(),
        schedule: cron.to_string(),
        status: ScheduleStatus::Active,
        context: None,
        last_fired_at: None,
        automatic_backfill: false,
        cron_timezone: None,
        queue_name: None,
    };

    provider
        .apply_schedules(&[make(format!("idkeep-{tag}"), keep.clone(), "0 0 1 * * *")])
        .await?;

    // The second insert reuses schedule_id `dup` → PRIMARY KEY violation, so the
    // whole batch (including the replacement of `keep`) must roll back.
    let bad = vec![
        make(dup.clone(), keep.clone(), "0 0 2 * * *"),
        make(dup.clone(), other.clone(), "0 0 3 * * *"),
    ];
    assert!(
        provider.apply_schedules(&bad).await.is_err(),
        "a duplicate schedule_id must fail the batch"
    );

    let mine = provider
        .list_schedules(&ScheduleFilter {
            name_prefixes: vec![prefix],
            ..Default::default()
        })
        .await?;
    assert_eq!(mine.len(), 1, "rollback left exactly the original schedule");
    assert_eq!(mine[0].schedule_name, keep);
    assert_eq!(mine[0].schedule_id, format!("idkeep-{tag}"), "original id");
    assert_eq!(mine[0].schedule, "0 0 1 * * *", "original cron preserved");

    provider.delete_schedule(&keep).await?;
    provider.delete_schedule(&other).await?;
    Ok(())
}

/// Through Postgres: a transactional step commits the user's writes and the step
/// checkpoint together. Re-running the same workflow id replays the debit from
/// its checkpoint instead of reapplying it (exactly-once).
#[tokio::test]
async fn pg_transaction_step() -> Result<()> {
    use durust::params;
    let Some(url) = database_url() else {
        eprintln!("skipping pg_transaction_step: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("acct_{tag}");
    let wf = format!("wf-txn-{tag}");
    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;

    engine.register("acct", |ctx: DurableContext, table: String| async move {
        let t = table.clone();
        ctx.transaction::<(), _>("setup", move |tx| {
            let t = t.clone();
            Box::pin(async move {
                tx.execute(
                    &format!("CREATE TABLE IF NOT EXISTS {t} (id INT PRIMARY KEY, bal BIGINT)"),
                    &params![],
                )
                .await?;
                tx.execute(
                    &format!(
                        "INSERT INTO {t} (id, bal) VALUES (1, 100) ON CONFLICT (id) DO NOTHING"
                    ),
                    &params![],
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        let t = table.clone();
        let bal: i64 = ctx
            .transaction("debit", move |tx| {
                let t = t.clone();
                Box::pin(async move {
                    tx.execute(
                        &format!("UPDATE {t} SET bal = bal - ? WHERE id = ?"),
                        &params![10_i64, 1_i64],
                    )
                    .await?;
                    let row = tx
                        .query_one(
                            &format!("SELECT bal FROM {t} WHERE id = ?"),
                            &params![1_i64],
                        )
                        .await?;
                    Ok(row.get::<i64>("bal"))
                })
            })
            .await?;
        Ok::<_, Error>(bal)
    });

    // Register the cleanup workflow up front too: the shared runtime is built on
    // the first run, so a later registration would not be seen.
    engine.register("drop", |ctx: DurableContext, table: String| async move {
        ctx.transaction::<(), _>("drop", move |tx| {
            let table = table.clone();
            Box::pin(async move {
                tx.execute(&format!("DROP TABLE IF EXISTS {table}"), &params![])
                    .await?;
                Ok(())
            })
        })
        .await?;
        Ok::<_, Error>(())
    });

    let bal1: i64 = engine.start_typed("acct", &wf, table.clone()).await?;
    assert_eq!(bal1, 90);
    // Re-run the same id: the debit replays from its checkpoint, not reapplied.
    let bal2: i64 = engine.start_typed("acct", &wf, table.clone()).await?;
    assert_eq!(bal2, 90, "exactly-once: re-running does not debit again");

    // Clean up the user table and the workflow rows.
    let drop_wf = format!("wf-drop-{tag}");
    let _: () = engine.start_typed("drop", &drop_wf, table.clone()).await?;
    provider.delete_workflows(&[wf, drop_wf], false).await?;
    Ok(())
}

/// Two concurrent SERIALIZABLE transactional steps read-then-write the same row.
/// One hits a serialization conflict and retries on a fresh transaction, so
/// neither increment is lost (the counter ends at 2, results are {1, 2}).
#[tokio::test]
async fn pg_transaction_serializable_retries_on_conflict() -> Result<()> {
    use durust::{params, IsolationLevel, TransactionOptions};
    let Some(url) = database_url() else {
        eprintln!("skipping pg_transaction_serializable_retries_on_conflict: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("ctr_{tag}");
    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let mut engine = DurableEngine::new(provider.clone()).await?;

    // All workflows registered up front (the shared runtime is built on first run).
    engine.register("seed", |ctx: DurableContext, table: String| async move {
        ctx.transaction::<(), _>("seed", move |tx| {
            let table = table.clone();
            Box::pin(async move {
                tx.execute(
                    &format!("CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, v BIGINT)"),
                    &params![],
                )
                .await?;
                tx.execute(
                    &format!(
                        "INSERT INTO {table} (id, v) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"
                    ),
                    &params![],
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok::<_, Error>(())
    });
    engine.register("incr", |ctx: DurableContext, table: String| async move {
        let t = table.clone();
        let v: i64 = ctx
            .transaction_with(
                TransactionOptions::new("incr").isolation(IsolationLevel::Serializable),
                move |tx| {
                    let t = t.clone();
                    Box::pin(async move {
                        let row = tx
                            .query_one(&format!("SELECT v FROM {t} WHERE id = 1"), &params![])
                            .await?;
                        let cur: i64 = row.get("v");
                        // Widen the read-write window so the two overlap.
                        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                        tx.execute(
                            &format!("UPDATE {t} SET v = ? WHERE id = 1"),
                            &params![cur + 1],
                        )
                        .await?;
                        Ok(cur + 1)
                    })
                },
            )
            .await?;
        Ok::<_, Error>(v)
    });
    engine.register("drop", |ctx: DurableContext, table: String| async move {
        ctx.transaction::<(), _>("drop", move |tx| {
            let table = table.clone();
            Box::pin(async move {
                tx.execute(&format!("DROP TABLE IF EXISTS {table}"), &params![])
                    .await?;
                Ok(())
            })
        })
        .await?;
        Ok::<_, Error>(())
    });

    let _: () = engine
        .start_typed("seed", &format!("seed-{tag}"), table.clone())
        .await?;
    let (id_a, id_b) = (format!("a-{tag}"), format!("b-{tag}"));
    let (a, b) = tokio::join!(
        engine.start_typed::<_, i64>("incr", &id_a, table.clone()),
        engine.start_typed::<_, i64>("incr", &id_b, table.clone()),
    );
    let mut results = [a?, b?];
    results.sort();
    assert_eq!(
        results,
        [1, 2],
        "both increments applied — the serialization conflict was retried"
    );

    let _: () = engine
        .start_typed("drop", &format!("drop-{tag}"), table.clone())
        .await?;
    provider
        .delete_workflows(
            &[
                format!("seed-{tag}"),
                format!("a-{tag}"),
                format!("b-{tag}"),
                format!("drop-{tag}"),
            ],
            false,
        )
        .await?;
    Ok(())
}

/// export_workflow / import_workflow round-trip a workflow's full durable state
/// (status, steps, events, streams) through Postgres after deletion.
#[tokio::test]
async fn pg_export_import_round_trip() -> Result<()> {
    use durust::STATUS_SUCCESS;
    let Some(url) = database_url() else {
        eprintln!("skipping pg_export_import_round_trip: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let id = format!("wf-export-{tag}");

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
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

    let out: i64 = engine
        .run_workflow::<_, i64>("expo", 21_i64, WorkflowOptions::with_id(&id))
        .await?
        .get_result()
        .await?;
    assert_eq!(out, 42);

    let exported = engine.export_workflow(&id, false).await?;
    assert_eq!(exported.len(), 1);
    engine
        .delete_workflows(std::slice::from_ref(&id), false)
        .await?;
    assert!(engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![id.clone()],
            ..Default::default()
        })
        .await?
        .is_empty());

    engine.import_workflow(&exported).await?;
    let rows = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![id.clone()],
            load_output: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, STATUS_SUCCESS);
    assert_eq!(rows[0].output, Some(serde_json::json!(42)));

    let steps = engine.get_workflow_steps(&id).await?;
    let double = steps.iter().find(|s| s.name == "double").expect("step");
    assert_eq!(double.output, Some(serde_json::json!(42)));

    let events = engine.list_workflow_events(&id).await?;
    assert_eq!(events, vec![("k".to_string(), serde_json::json!("v"))]);

    let streams = engine.list_workflow_streams(&id).await?;
    assert_eq!(
        streams,
        vec![(
            "s".to_string(),
            vec![serde_json::json!(1), serde_json::json!(2)]
        )]
    );

    // Importing again fails: import never overwrites an existing workflow.
    assert!(engine.import_workflow(&exported).await.is_err());

    engine
        .delete_workflows(std::slice::from_ref(&id), false)
        .await?;
    Ok(())
}

/// A blocked recv wakes via LISTEN/NOTIFY: an external send returns the message
/// well within the long backstop interval (proving the push path, not polling).
#[tokio::test]
async fn pg_recv_wakes_via_listen_notify() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_recv_wakes_via_listen_notify: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let id = format!("wf-recv-{tag}");

    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    assert!(provider.supports_listen_notify());
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("waiter", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("topic", Duration::from_secs(10)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    let mut handle = engine
        .run_workflow::<_, String>("waiter", (), WorkflowOptions::with_id(&id))
        .await?;
    // Let the workflow reach recv and subscribe to the channel.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = std::time::Instant::now();
    engine.send(&id, "hello".to_string(), "topic").await?;
    assert_eq!(handle.get_result().await?, "hello");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "recv should wake via NOTIFY, not the backstop (took {elapsed:?})"
    );

    engine
        .delete_workflows(std::slice::from_ref(&id), false)
        .await?;
    Ok(())
}

/// A blocked get_event wakes via LISTEN/NOTIFY when another workflow sets the
/// event, returning well within the backstop interval.
#[tokio::test]
async fn pg_get_event_wakes_via_listen_notify() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_get_event_wakes_via_listen_notify: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let reader_id = format!("reader-{tag}");
    let setter_id = format!("setter-{tag}");

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register("reader", |ctx: DurableContext, target: String| async move {
        let v: Option<String> = ctx.get_event(&target, "k", Duration::from_secs(10)).await?;
        Ok::<_, Error>(v.unwrap_or_default())
    });
    engine.register("setter", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("k", "v").await?;
        Ok::<_, Error>(String::new())
    });

    let mut reader = engine
        .run_workflow::<_, String>(
            "reader",
            setter_id.clone(),
            WorkflowOptions::with_id(&reader_id),
        )
        .await?;
    // Let the reader reach get_event and subscribe.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = std::time::Instant::now();
    engine
        .run_workflow::<_, String>("setter", (), WorkflowOptions::with_id(&setter_id))
        .await?
        .get_result()
        .await?;
    assert_eq!(reader.get_result().await?, "v");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "get_event should wake via NOTIFY, not the backstop (took {elapsed:?})"
    );

    engine
        .delete_workflows(&[reader_id, setter_id], false)
        .await?;
    Ok(())
}
