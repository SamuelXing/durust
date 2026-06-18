//! Postgres backend tests. Skipped unless `DATABASE_URL` points at a reachable
//! Postgres instance (ideally an empty database — `init` runs the migrations).
//!
//!   createdb durust_test && DATABASE_URL=postgres://localhost/durust_test cargo test --test postgres

use durust::{
    DurableContext, DurableEngine, Error, ErrorCode, ListFilter, PostgresProvider, Result,
    Serializer, StateProvider, WorkflowOptions, WorkflowQueue, WorkflowStatus, STATUS_PENDING,
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
        .record_step_result(&old, 0, "legacy_step", serde_json::json!(1), None)
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

    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    let provider = PostgresProvider::connect(&url).await?;

    let seed = |wid: String, status: &str, parent: Option<String>| {
        let mut s = WorkflowStatus::new(wid, "noop", serde_json::Value::Null, status, "", "0.1.0");
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

    // Group by status, scoped to this run's id prefix.
    let by_status = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            workflow_id_prefix: Some(prefix.clone()),
            ..Default::default()
        })
        .await?;
    assert_eq!(by_status.len(), 1);
    assert_eq!(
        by_status[0].group.get("status"),
        Some(&Some(STATUS_SUCCESS.to_string()))
    );
    assert_eq!(by_status[0].count, 3);

    // A wide time bucket collapses them into one group with a time_bucket key.
    let bucketed = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            time_bucket_ms: Some(3_600_000),
            workflow_id_prefix: Some(prefix),
            ..Default::default()
        })
        .await?;
    assert_eq!(bucketed.len(), 1);
    assert_eq!(bucketed[0].count, 3);
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
