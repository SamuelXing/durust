//! Workflow management tests: retrieve, list (filters), cancel, resume, fork,
//! and version-gated recovery. Backend-free (in-memory provider).

use durust::{
    DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result, StateProvider,
    StepAggregateQuery, WorkflowAggregate, WorkflowAggregateQuery, WorkflowOptions, WorkflowStatus,
    STATUS_CANCELLED, STATUS_PENDING, STATUS_SUCCESS,
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
    // Resume re-queues the workflow for a dispatcher, so the engine must be live.
    engine.launch().await?;

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
        .record_step_result("wf-cancel", 0, "first", serde_json::json!(1), None, None)
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
    engine.shutdown(Duration::from_secs(1)).await?;
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
    // Fork re-queues the new workflow for a dispatcher, so the engine must be live.
    engine.launch().await?;

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
    engine.shutdown(Duration::from_secs(1)).await?;
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

/// cancel_workflows / resume_workflows act on many ids at once: non-terminal ids
/// transition, missing and already-terminal ids are silently skipped, and resume
/// returns a handle only for each id it actually transitioned.
#[tokio::test]
async fn bulk_cancel_and_resume() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    // Resume re-queues workflows for a dispatcher, so the engine must be live.
    engine.launch().await?;

    // Two pending workflows to act on, one pending left untouched, one already
    // completed.
    for id in ["wf-1", "wf-2", "wf-3"] {
        provider
            .insert_workflow_status(WorkflowStatus::new(
                id,
                "noop",
                serde_json::Value::Null,
                STATUS_PENDING,
                "",
                "0.1.0",
            ))
            .await?;
    }
    let () = engine.start_typed("noop", "wf-done", ()).await?;

    // Cancel a subset plus a missing id and the completed one (both skipped).
    engine
        .cancel_workflows(&[
            "wf-1".into(),
            "wf-2".into(),
            "ghost".into(),
            "wf-done".into(),
        ])
        .await?;
    assert_eq!(status_of(&provider, "wf-1").await, STATUS_CANCELLED);
    assert_eq!(status_of(&provider, "wf-2").await, STATUS_CANCELLED);
    assert_eq!(status_of(&provider, "wf-3").await, STATUS_PENDING);
    assert_eq!(status_of(&provider, "wf-done").await, STATUS_SUCCESS);

    // Resume the two cancelled plus the completed one: only the cancelled pair
    // transitions, so we get exactly two handles.
    let handles = engine
        .resume_workflows::<()>(&["wf-1".into(), "wf-2".into(), "wf-done".into()])
        .await?;
    assert_eq!(handles.len(), 2, "completed wf-done must not be resumed");
    for mut h in handles {
        h.get_result().await?;
    }
    assert_eq!(status_of(&provider, "wf-1").await, STATUS_SUCCESS);
    assert_eq!(status_of(&provider, "wf-2").await, STATUS_SUCCESS);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// delete_workflows removes rows regardless of state; with delete_children it
/// also removes descendants by parent lineage, otherwise it leaves them.
#[tokio::test]
async fn bulk_delete_with_children() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let engine = DurableEngine::new(provider.clone()).await?;

    let seed = |id: &str, parent: Option<&str>| {
        let mut s = WorkflowStatus::new(
            id,
            "w",
            serde_json::Value::Null,
            STATUS_SUCCESS,
            "",
            "0.1.0",
        );
        s.parent_workflow_id = parent.map(|p| p.to_string());
        s
    };

    // parent → child → grandchild lineage.
    provider.insert_workflow_status(seed("p", None)).await?;
    provider
        .insert_workflow_status(seed("c", Some("p")))
        .await?;
    provider
        .insert_workflow_status(seed("gc", Some("c")))
        .await?;

    // Without delete_children: only the parent goes.
    engine.delete_workflows(&["p".into()], false).await?;
    assert!(provider.get_workflow_status("p").await?.is_none());
    assert!(provider.get_workflow_status("c").await?.is_some());

    // With delete_children: the whole subtree under c is removed.
    engine.delete_workflows(&["c".into()], true).await?;
    assert!(provider.get_workflow_status("c").await?.is_none());
    assert!(provider.get_workflow_status("gc").await?.is_none());
    Ok(())
}

/// `has_parent` splits children from roots, and the load flags drop the heavy
/// input/output fields from results.
#[tokio::test]
async fn list_filters_parentage_and_load_flags() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
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

    // has_parent = true keeps only the child; false only the root.
    let children = engine
        .list_workflows(&ListFilter {
            has_parent: Some(true),
            ..Default::default()
        })
        .await?;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].name, "child");
    let child_id = children[0].id.clone();

    let roots = engine
        .list_workflows(&ListFilter {
            has_parent: Some(false),
            ..Default::default()
        })
        .await?;
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].id, "p");

    // Opting out of input/output omits them...
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

    // ...while the default loads them.
    let full = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![child_id],
            ..Default::default()
        })
        .await?;
    assert_eq!(full[0].input, serde_json::json!(5));
    assert_eq!(full[0].output, Some(serde_json::json!(50)));
    Ok(())
}

/// `completed_*` / `dequeued_*` bound the result by completion and start time.
#[tokio::test]
async fn list_filters_completed_and_dequeued_time() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("noop", "w", ()).await?;
    let now = chrono::Utc::now().timestamp_millis();
    let hour = 3_600_000;

    // Completed within the last hour.
    let recent = engine
        .list_workflows(&ListFilter {
            completed_after_ms: Some(now - hour),
            completed_before_ms: Some(now + hour),
            ..Default::default()
        })
        .await?;
    assert_eq!(recent.len(), 1);

    // Completed only in the future → none.
    let future = engine
        .list_workflows(&ListFilter {
            completed_after_ms: Some(now + hour),
            ..Default::default()
        })
        .await?;
    assert!(future.is_empty());

    // Dequeued (started) within the last hour — a direct run stamps started_at.
    let started = engine
        .list_workflows(&ListFilter {
            dequeued_after_ms: Some(now - hour),
            dequeued_before_ms: Some(now + hour),
            ..Default::default()
        })
        .await?;
    assert_eq!(started.len(), 1);
    Ok(())
}

/// Aggregate counts group by status/name and honor filters; an empty query is
/// rejected.
#[tokio::test]
async fn workflow_aggregates_group_and_filter() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("ok", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register("boom", |_ctx: DurableContext, _: ()| async move {
        Err::<(), _>(Error::app("nope"))
    });

    // Two successes, one failure.
    engine.start_typed::<_, ()>("ok", "a", ()).await?;
    engine.start_typed::<_, ()>("ok", "b", ()).await?;
    let _ = engine.start_typed::<_, ()>("boom", "c", ()).await;

    let count_for = |rows: &[WorkflowAggregate], key: &str, val: &str| -> i64 {
        rows.iter()
            .find(|r| r.group.get(key) == Some(&Some(val.to_string())))
            .and_then(|r| r.count)
            .unwrap_or(0)
    };

    // Group by status.
    let by_status = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(count_for(&by_status, "status", "SUCCESS"), 2);
    assert_eq!(count_for(&by_status, "status", "ERROR"), 1);

    // Group by name.
    let by_name = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_name: true,
            select_count: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(count_for(&by_name, "name", "ok"), 2);
    assert_eq!(count_for(&by_name, "name", "boom"), 1);

    // Filter to one name, group by status → just the two successes.
    let ok_only = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            select_count: true,
            name: vec!["ok".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(ok_only.len(), 1);
    assert_eq!(
        ok_only[0].group.get("status"),
        Some(&Some("SUCCESS".to_string()))
    );
    assert_eq!(ok_only[0].count, Some(2));

    // Grouping by status and name together yields one row per (status, name).
    let combined = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            by_name: true,
            select_count: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(combined.len(), 2);

    // Latency aggregates: every workflow has a created_at, a start (direct runs
    // start immediately, so queue-wait is ~0), and — being terminal — a total
    // latency. count is null when not selected.
    let latency = engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_name: true,
            select_min_created_at: true,
            select_max_queue_wait_ms: true,
            select_max_total_latency_ms: true,
            name: vec!["ok".to_string()],
            ..Default::default()
        })
        .await?;
    assert_eq!(latency.len(), 1);
    assert_eq!(latency[0].count, None);
    assert!(latency[0].min_created_at.is_some());
    assert!(latency[0].max_total_latency_ms.is_some_and(|l| l >= 0));
    assert!(latency[0].max_queue_wait_ms.is_some_and(|w| w >= 0));

    // A query that groups by nothing, or selects nothing, is rejected.
    assert!(engine
        .get_workflow_aggregates(&WorkflowAggregateQuery::default())
        .await
        .is_err());
    assert!(engine
        .get_workflow_aggregates(&WorkflowAggregateQuery {
            by_status: true,
            ..Default::default()
        })
        .await
        .is_err());
    Ok(())
}

/// Aggregate counts honor the `completed_*`/`dequeued_*` filters: a window that
/// contains the run keeps it, a future-only window excludes it.
#[tokio::test]
async fn workflow_aggregates_completed_and_dequeued_filter() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("ok", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("ok", "w", ()).await?;
    let now = chrono::Utc::now().timestamp_millis();
    let hour = 3_600_000;

    async fn total(engine: &DurableEngine, q: WorkflowAggregateQuery) -> i64 {
        engine
            .get_workflow_aggregates(&q)
            .await
            .unwrap()
            .iter()
            .filter_map(|r| r.count)
            .sum()
    }

    // Completed within the last hour → counted.
    assert_eq!(
        total(
            &engine,
            WorkflowAggregateQuery {
                by_status: true,
                select_count: true,
                completed_after_ms: Some(now - hour),
                completed_before_ms: Some(now + hour),
                ..Default::default()
            }
        )
        .await,
        1
    );
    // Completed only in the future → excluded.
    assert_eq!(
        total(
            &engine,
            WorkflowAggregateQuery {
                by_status: true,
                select_count: true,
                completed_after_ms: Some(now + hour),
                ..Default::default()
            }
        )
        .await,
        0
    );
    // Dequeued (started) within the last hour → counted (a direct run stamps it).
    assert_eq!(
        total(
            &engine,
            WorkflowAggregateQuery {
                by_status: true,
                select_count: true,
                dequeued_after_ms: Some(now - hour),
                dequeued_before_ms: Some(now + hour),
                ..Default::default()
            }
        )
        .await,
        1
    );
    Ok(())
}

/// Step aggregates count per function name and report the max step duration;
/// select/group validation is enforced.
#[tokio::test]
async fn step_aggregates_count_and_duration() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("work", |ctx: DurableContext, _: ()| async move {
        ctx.step("fast", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.step("slow", || async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, Error>(2_i64)
        })
        .await?;
        ctx.step("fast", || async { Ok::<_, Error>(3_i64) }).await?;
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("work", "w", ()).await?;

    // Group by function name; select count and max duration.
    let by_fn = engine
        .get_step_aggregates(&StepAggregateQuery {
            by_function_name: true,
            select_count: true,
            select_max_duration_ms: true,
            ..Default::default()
        })
        .await?;
    let row = |name: &str| {
        by_fn
            .iter()
            .find(|r| r.group.get("function_name") == Some(&Some(name.to_string())))
            .cloned()
            .unwrap_or_else(|| panic!("no group for {name}"))
    };
    assert_eq!(row("fast").count, Some(2));
    assert_eq!(row("slow").count, Some(1));
    assert!(
        row("slow").max_duration_ms.unwrap_or(0) >= 10,
        "the slow step's duration should reflect its ~20ms of work"
    );

    // Group by derived status: every step succeeded → one SUCCESS group of 3.
    let by_status = engine
        .get_step_aggregates(&StepAggregateQuery {
            by_status: true,
            select_count: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(by_status.len(), 1);
    assert_eq!(
        by_status[0].group.get("status"),
        Some(&Some("SUCCESS".to_string()))
    );
    assert_eq!(by_status[0].count, Some(3));
    // Duration was not selected, so it is absent.
    assert!(by_status[0].max_duration_ms.is_none());

    // A query that groups by nothing, or selects nothing, is rejected.
    assert!(engine
        .get_step_aggregates(&StepAggregateQuery {
            select_count: true,
            ..Default::default()
        })
        .await
        .is_err());
    assert!(engine
        .get_step_aggregates(&StepAggregateQuery {
            by_function_name: true,
            ..Default::default()
        })
        .await
        .is_err());
    Ok(())
}

async fn status_of(provider: &Arc<InMemoryProvider>, id: &str) -> String {
    provider
        .get_workflow_status(id)
        .await
        .unwrap()
        .unwrap()
        .status
}
