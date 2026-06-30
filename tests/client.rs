//! Out-of-process `Client`: enqueue work and observe it without a local
//! registry. A `Client` and a `DurableEngine` share one provider — the client
//! produces, the engine consumes.

use durust::{
    Client, DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A client enqueues a workflow it does not register; a separate engine claims
/// it, runs it, and the client observes the result, the row, and its steps.
#[tokio::test]
async fn client_enqueues_work_an_engine_runs() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("double", |ctx: DurableContext, n: i64| async move {
        ctx.step("mul", || async { Ok::<_, Error>(n * 2) }).await
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    // The client has no registry — it only enqueues and observes.
    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("job-1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, i64>("q", "double", 21i64, opts).await?;
    assert_eq!(handle.id(), "job-1");
    assert_eq!(
        handle.get_result().await?,
        42,
        "engine ran the enqueued work"
    );

    // The client observes the persisted row and its step.
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("job-".to_string()),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "double");
    let steps = client.get_workflow_steps("job-1").await?;
    assert!(steps.iter().any(|s| s.name == "mul"));

    // retrieve_workflow returns a handle; an unknown id errors.
    let mut again: durust::WorkflowHandle<i64> = client.retrieve_workflow("job-1").await?;
    assert_eq!(again.get_result().await?, 42);
    assert!(client.retrieve_workflow::<i64>("nope").await.is_err());

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A client sends a message to a workflow waiting in `recv`, then reads the
/// event the workflow sets — the cross-process messaging path.
#[tokio::test]
async fn client_sends_messages_and_reads_events() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("waiter", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("topic", Duration::from_secs(5)).await?;
        let msg = msg.unwrap_or_default();
        ctx.set_event("echo", &msg).await?;
        Ok::<_, Error>(msg)
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("waiter-1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, String>("q", "waiter", (), opts).await?;

    // Deliver the message the workflow is waiting for.
    client
        .send("waiter-1", "hello".to_string(), "topic")
        .await?;
    assert_eq!(handle.get_result().await?, "hello");

    // The event the workflow set is now readable.
    let event: Option<String> = client
        .get_event("waiter-1", "echo", Duration::from_secs(2))
        .await?;
    assert_eq!(event.as_deref(), Some("hello"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Enqueue rejects an empty queue or workflow name, and an incompatible
/// partition-key + deduplication-id pair.
#[tokio::test]
async fn client_enqueue_validates() -> Result<()> {
    let client = Client::new(Arc::new(InMemoryProvider::new()));
    assert!(client
        .enqueue::<_, ()>("", "w", 1i64, WorkflowOptions::default())
        .await
        .is_err());
    assert!(client
        .enqueue::<_, ()>("q", "", 1i64, WorkflowOptions::default())
        .await
        .is_err());
    let opts = WorkflowOptions {
        partition_key: Some("p".to_string()),
        dedup_id: Some("d".to_string()),
        ..Default::default()
    };
    assert!(client.enqueue::<_, ()>("q", "w", 1i64, opts).await.is_err());
    Ok(())
}

/// The client cancels a not-yet-run (delayed) workflow, then deletes it.
#[tokio::test]
async fn client_cancels_and_deletes() -> Result<()> {
    let client = Client::new(Arc::new(InMemoryProvider::new()));
    // Enqueue far in the future so it stays DELAYED while we manage it.
    let opts = WorkflowOptions {
        workflow_id: Some("c1".to_string()),
        delay: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    client.enqueue::<_, ()>("q", "noop", (), opts).await?;

    client.cancel_workflows(&["c1".to_string()]).await?;
    let rows = client.list_workflows(&ListFilter::default()).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "CANCELLED");

    client.delete_workflows(&["c1".to_string()], false).await?;
    assert!(client
        .list_workflows(&ListFilter::default())
        .await?
        .is_empty());
    Ok(())
}

/// `set_workflow_delay` from the client pulls a far-future DELAYED workflow
/// forward so a running engine's dispatcher claims and runs it promptly.
#[tokio::test]
async fn client_set_workflow_delay_pulls_in() -> Result<()> {
    static RAN: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("ping", |_ctx: DurableContext, _: ()| async move {
        RAN.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("d1".to_string()),
        delay: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    client.enqueue::<_, ()>("q", "ping", (), opts).await?;
    // Reschedule to ~now.
    assert!(
        client
            .set_workflow_delay("d1", Duration::from_millis(10))
            .await?
    );

    for _ in 0..100 {
        if RAN.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(RAN.load(Ordering::SeqCst), 1, "rescheduled workflow ran");

    // A non-DELAYED / missing id is a no-op.
    assert!(
        !client
            .set_workflow_delay("nope", Duration::from_secs(1))
            .await?
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The client reads a durable stream a workflow produced.
#[tokio::test]
async fn client_reads_a_stream() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("producer", |ctx: DurableContext, _: ()| async move {
        ctx.write_stream("s", 1i64).await?;
        ctx.write_stream("s", 2i64).await?;
        ctx.close_stream("s").await?;
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("p1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, ()>("q", "producer", (), opts).await?;
    handle.get_result().await?;

    let (values, closed) = client.read_stream::<i64>("p1", "s").await?;
    assert_eq!(values, vec![1, 2]);
    assert!(closed);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The client reads and promotes application versions an engine registered.
#[tokio::test]
async fn client_reads_version_registry() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let engine = DurableEngine::new_with_version(provider.clone(), "1.0.0").await?;
    engine.launch().await?;
    engine.shutdown(Duration::from_secs(1)).await?;

    let client = Client::new(provider.clone());
    let versions = client.list_application_versions().await?;
    assert!(versions.iter().any(|v| v.version_name == "1.0.0"));
    assert_eq!(
        client
            .get_latest_application_version()
            .await?
            .unwrap()
            .version_name,
        "1.0.0"
    );
    assert!(client.set_latest_application_version("1.0.0").await?);
    assert!(!client.set_latest_application_version("nope").await?);
    assert!(client.set_latest_application_version("").await.is_err());
    Ok(())
}

/// The client manages schedules (no local registry needed): create/get/list,
/// pause/resume, apply (create-or-replace), delete, with validation.
#[tokio::test]
async fn client_manages_schedules() -> Result<()> {
    use durust::{ApplySchedule, ScheduleFilter, ScheduleOptions, ScheduleStatus};
    let client = Client::new(Arc::new(InMemoryProvider::new()));

    client
        .create_schedule("nightly", "report", "0 0 0 * * *", ScheduleOptions::new())
        .await?;
    let got = client.get_schedule("nightly").await?.expect("created");
    assert_eq!(got.workflow_name, "report");
    assert_eq!(got.status, ScheduleStatus::Active);

    // Validation: bad cron, empty name, duplicate name.
    assert!(client
        .create_schedule("bad", "report", "not a cron", ScheduleOptions::new())
        .await
        .is_err());
    assert!(client
        .create_schedule("", "report", "0 0 0 * * *", ScheduleOptions::new())
        .await
        .is_err());
    assert!(client
        .create_schedule("nightly", "report", "0 0 0 * * *", ScheduleOptions::new())
        .await
        .is_err());

    // Pause drops it from the ACTIVE filter; resume restores it.
    assert!(client.pause_schedule("nightly").await?);
    assert!(client
        .list_schedules(&ScheduleFilter {
            statuses: vec![ScheduleStatus::Active],
            ..Default::default()
        })
        .await?
        .is_empty());
    assert!(client.resume_schedule("nightly").await?);

    // apply replaces by name (fresh schedule_id) and adds new ones.
    let before = client.get_schedule("nightly").await?.unwrap().schedule_id;
    client
        .apply_schedules(vec![
            ApplySchedule::new("nightly", "report", "0 0 1 * * *"),
            ApplySchedule::new("hourly", "report", "0 0 * * * *"),
        ])
        .await?;
    let after = client.get_schedule("nightly").await?.unwrap();
    assert_ne!(after.schedule_id, before, "replaced with a fresh id");
    assert_eq!(after.schedule, "0 0 1 * * *");
    assert_eq!(
        client
            .list_schedules(&ScheduleFilter::default())
            .await?
            .len(),
        2
    );

    assert!(client.delete_schedule("nightly").await?);
    assert!(client.get_schedule("nightly").await?.is_none());
    Ok(())
}

/// A schedule a client creates is picked up and fired by a running engine whose
/// reconciler discovers it — the cross-process scheduling path.
#[tokio::test]
async fn client_created_schedule_fires_on_engine() -> Result<()> {
    use durust::ScheduleOptions;
    static FIRED: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("tick_job", |_ctx: DurableContext, _at: String| async move {
        FIRED.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    // The client declares the schedule; the engine's reconciler installs it.
    let client = Client::new(provider.clone());
    client
        .create_schedule("tick", "tick_job", "* * * * * *", ScheduleOptions::new())
        .await?;

    tokio::time::sleep(Duration::from_millis(2200)).await;
    engine.shutdown(Duration::from_secs(1)).await?;

    assert!(
        FIRED.load(Ordering::SeqCst) >= 1,
        "engine fired the client-created schedule"
    );
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("sched-tick-".to_string()),
            ..Default::default()
        })
        .await?;
    assert!(!rows.is_empty(), "scheduled ticks were persisted");
    Ok(())
}

/// A client resumes a cancelled workflow: it is re-queued onto the internal
/// queue and a live engine's dispatcher re-runs it from its checkpoints.
#[tokio::test]
async fn client_resumes_a_cancelled_workflow() -> Result<()> {
    use durust::{StateProvider, WorkflowHandle, WorkflowStatus, STATUS_PENDING};
    static S1: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("two_step", |ctx: DurableContext, _: ()| async move {
        ctx.step("s0", || async { Ok::<_, Error>(1i64) }).await?;
        let v = ctx
            .step("s1", || async {
                S1.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(2i64)
            })
            .await?;
        Ok::<_, Error>(v)
    });
    engine.launch().await?;

    // A direct workflow with step 0 already checkpointed, then cancelled.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "w",
            "two_step",
            serde_json::Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;
    provider
        .record_step_result("w", 0, "s0", serde_json::json!(1), None, None)
        .await?;
    let client = Client::new(provider.clone());
    client.cancel_workflow("w").await?;

    // Resume from the client → the engine's internal dispatcher re-runs it.
    let mut h: WorkflowHandle<i64> = client.resume_workflow("w").await?;
    assert_eq!(h.get_result().await?, 2);
    assert_eq!(
        S1.load(Ordering::SeqCst),
        1,
        "s1 ran once; s0 replayed from its checkpoint"
    );

    // Resuming a completed workflow errors.
    assert!(client.resume_workflow::<i64>("w").await.is_err());

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Re-execution always uses the internal queue, never the workflow's own queue:
/// a workflow on a user queue this process does NOT listen to still re-runs on
/// resume, because the internal queue is always dispatched.
#[tokio::test]
async fn client_resume_runs_via_internal_queue_not_own_queue() -> Result<()> {
    use durust::WorkflowHandle;
    static RAN: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("job", |_ctx: DurableContext, _: ()| async move {
        RAN.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("orders"));
    // Listen to a different queue: "orders" gets no dispatcher here, but the
    // internal queue is always dispatched.
    engine.listen_queues(["something-else"]);
    engine.launch().await?;

    let client = Client::new(provider.clone());
    client
        .enqueue::<_, ()>(
            "orders",
            "job",
            (),
            WorkflowOptions {
                workflow_id: Some("j1".to_string()),
                ..Default::default()
            },
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        RAN.load(Ordering::SeqCst),
        0,
        "an un-listened queue is not dispatched, so it does not run"
    );

    // Cancel then resume: re-queued onto the internal queue, it runs — it would
    // hang here if resume put it back on the un-listened "orders" queue.
    client.cancel_workflow("j1").await?;
    let mut h: WorkflowHandle<()> = client.resume_workflow("j1").await?;
    h.get_result().await?;
    assert_eq!(
        RAN.load(Ordering::SeqCst),
        1,
        "resume ran it via the internal queue"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A client forks a workflow from a step; the fork is re-queued and a live engine
/// runs it, reusing checkpoints before the fork point.
#[tokio::test]
async fn client_forks_a_workflow() -> Result<()> {
    use durust::WorkflowHandle;
    static SECOND: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("pipeline", |ctx: DurableContext, _: ()| async move {
        let a = ctx
            .step("first", || async { Ok::<_, Error>(10i64) })
            .await?;
        let b = ctx
            .step("second", || async {
                SECOND.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(a + 5)
            })
            .await?;
        Ok::<_, Error>(b)
    });
    engine.launch().await?;

    // Original run via the engine.
    let _: i64 = engine
        .run_workflow::<_, i64>("pipeline", (), WorkflowOptions::with_id("orig"))
        .await?
        .get_result()
        .await?;
    assert_eq!(SECOND.load(Ordering::SeqCst), 1);

    // Client forks from step 1: step 0 reused, step 1 re-executes on the engine.
    let client = Client::new(provider.clone());
    let mut forked: WorkflowHandle<i64> = client
        .fork_workflow("orig", 1, WorkflowOptions::with_id("forked"))
        .await?;
    assert_eq!(forked.get_result().await?, 15);
    assert_eq!(SECOND.load(Ordering::SeqCst), 2, "fork re-ran step 1");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The client triggers a one-off run of a schedule; because the schedule is
/// direct (no queue of its own), the run is routed to the internal queue and a
/// live engine's always-on internal dispatcher executes it.
#[tokio::test]
async fn client_triggers_a_schedule_run_on_an_engine() -> Result<()> {
    use durust::{ScheduleOptions, WorkflowHandle};
    static FIRED: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("tick_job", |_ctx: DurableContext, at: String| async move {
        FIRED.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(at)
    });
    engine.launch().await?;

    // A cron far in the future so the reconciler never fires it on its own —
    // only the explicit trigger runs.
    let client = Client::new(provider.clone());
    client
        .create_schedule("yearly", "tick_job", "0 0 0 1 1 *", ScheduleOptions::new())
        .await?;

    let mut h: WorkflowHandle<String> = client.trigger_schedule("yearly").await?;
    let id = h.id().to_string();
    assert!(
        id.starts_with("sched-yearly-trigger-"),
        "distinct trigger id, not a regular tick"
    );
    let out = h.get_result().await?;
    assert_eq!(
        out,
        id.strip_prefix("sched-yearly-trigger-").unwrap(),
        "the tick instant is passed through to the workflow as its input"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(FIRED.load(Ordering::SeqCst), 1, "triggered exactly once");

    // Triggering an unknown schedule errors.
    assert!(client.trigger_schedule::<String>("nope").await.is_err());
    Ok(())
}

/// The client backfills a direct schedule's past ticks: each lands ENQUEUED on
/// the internal queue under the same deterministic per-tick id the live loop
/// uses, so re-running the same window is idempotent (no duplicate ticks).
#[tokio::test]
async fn client_backfills_a_schedule_onto_the_internal_queue() -> Result<()> {
    use chrono::{TimeZone, Utc};
    use durust::ScheduleOptions;
    let provider = Arc::new(InMemoryProvider::new());
    let client = Client::new(provider.clone());

    // A direct (queue-less) daily schedule; the client runs nothing itself.
    client
        .create_schedule("daily", "report", "0 0 12 * * *", ScheduleOptions::new())
        .await?;

    let start = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 3, 4, 0, 0, 0).unwrap();
    let ids = client.backfill_schedule("daily", start, end).await?;
    assert_eq!(ids.len(), 3, "one tick per day in the window");

    let prefix = || ListFilter {
        workflow_id_prefix: Some("sched-daily-".to_string()),
        ..Default::default()
    };
    let rows = client.list_workflows(&prefix()).await?;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.name == "report"));
    assert!(rows.iter().all(|r| r.status == "ENQUEUED"));
    assert!(rows
        .iter()
        .all(|r| r.queue_name.as_deref() == Some("_dbos_internal_queue")));

    // Same window again: same ids, no new rows.
    assert_eq!(client.backfill_schedule("daily", start, end).await?, ids);
    assert_eq!(client.list_workflows(&prefix()).await?.len(), 3);

    // Backfilling an unknown schedule errors.
    assert!(client.backfill_schedule("nope", start, end).await.is_err());
    Ok(())
}

/// The client honors a per-enqueue application-version override and the
/// deduplication policy: a colliding dedup id either errors (Reject, the
/// default) or returns the existing workflow (ReturnExisting).
#[tokio::test]
async fn client_enqueue_dedup_and_app_version() -> Result<()> {
    use durust::{DeduplicationPolicy, WorkflowHandle};
    let provider = Arc::new(InMemoryProvider::new());
    let client = Client::new(provider.clone()).with_app_version("v1");

    // Per-enqueue version override; the client default applies otherwise.
    let _: WorkflowHandle<i64> = client
        .enqueue(
            "q",
            "wf",
            1i64,
            WorkflowOptions::with_id("j1").app_version("v2"),
        )
        .await?;
    let _: WorkflowHandle<i64> = client
        .enqueue("q", "wf", 1i64, WorkflowOptions::with_id("j2"))
        .await?;
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("j".into()),
            ..Default::default()
        })
        .await?;
    let ver = |id: &str| {
        rows.iter()
            .find(|r| r.id == id)
            .unwrap()
            .app_version
            .clone()
    };
    assert_eq!(ver("j1"), "v2", "per-enqueue override");
    assert_eq!(ver("j2"), "v1", "client default");

    // Deduplication: the first enqueue holds the slot.
    let first: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            1i64,
            WorkflowOptions::with_id("d1").dedup_id("once"),
        )
        .await?;

    // Reject (default): a colliding dedup id errors.
    assert!(client
        .enqueue::<_, i64>(
            "dq",
            "wf",
            2i64,
            WorkflowOptions::with_id("d2").dedup_id("once")
        )
        .await
        .is_err());

    // ReturnExisting: a colliding dedup id returns the existing workflow.
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
    assert_eq!(again.id(), "d1");

    // Only the first row holds the slot.
    let dq = client
        .list_workflows(&ListFilter {
            queue_name: Some("dq".into()),
            ..Default::default()
        })
        .await?;
    assert_eq!(dq.len(), 1);

    // A non-default policy requires a dedup id.
    assert!(client
        .enqueue::<_, i64>(
            "dq",
            "wf",
            4i64,
            WorkflowOptions::with_id("d4").dedup_policy(DeduplicationPolicy::ReturnExisting),
        )
        .await
        .is_err());
    Ok(())
}

/// A deduplication id is released once its holder reaches a terminal state, so
/// the same id can be enqueued again afterward.
#[tokio::test]
async fn client_dedup_slot_frees_on_completion() -> Result<()> {
    use durust::{StateProvider, WorkflowHandle, STATUS_SUCCESS};
    let provider = Arc::new(InMemoryProvider::new());
    let client = Client::new(provider.clone());

    // The first enqueue holds the dedup slot.
    let first: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            1i64,
            WorkflowOptions::with_id("d1").dedup_id("once"),
        )
        .await?;
    // While the holder is active a colliding dedup id is rejected.
    assert!(client
        .enqueue::<_, i64>(
            "dq",
            "wf",
            2i64,
            WorkflowOptions::with_id("d2").dedup_id("once")
        )
        .await
        .is_err());

    // Completing the holder releases the slot.
    provider
        .set_workflow_status(first.id(), STATUS_SUCCESS, None, None)
        .await?;

    // The same dedup id now starts a fresh workflow rather than colliding.
    let third: WorkflowHandle<i64> = client
        .enqueue(
            "dq",
            "wf",
            3i64,
            WorkflowOptions::with_id("d3").dedup_id("once"),
        )
        .await?;
    assert_eq!(third.id(), "d3");
    Ok(())
}

/// A workflow cancelled during its final step must stay cancelled: a late
/// SUCCESS/ERROR completion is rejected and does not overwrite the status.
#[tokio::test]
async fn client_completion_cannot_overwrite_cancelled() -> Result<()> {
    use durust::{StateProvider, WorkflowHandle, STATUS_CANCELLED, STATUS_SUCCESS};
    let provider = Arc::new(InMemoryProvider::new());
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
    Ok(())
}
