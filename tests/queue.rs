//! Queue & dispatch tests, backend-free via the in-memory provider:
//! basic enqueue→dispatch, worker concurrency, priority ordering, delayed
//! enqueue, deduplication, and rate limiting.

use durust::{
    DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, ListFilter, RateLimiter,
    Result, WorkflowOptions, WorkflowQueue, STATUS_DELAYED, STATUS_ENQUEUED,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A queue that polls fast enough for tests.
fn test_queue(name: &str) -> WorkflowQueue {
    WorkflowQueue::new(name).base_polling_interval(Duration::from_millis(10))
}

#[tokio::test]
async fn enqueue_dispatches_and_completes() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("add_one", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    engine.register_queue(test_queue("q"));
    engine.launch().await?;

    let mut handle = engine
        .enqueue::<_, i64>("q", "add_one", 41_i64, WorkflowOptions::default())
        .await?;
    assert_eq!(handle.get_result().await?, 42);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn enqueue_to_unregistered_queue_errors() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    let res = engine
        .enqueue::<_, ()>("nope", "noop", (), WorkflowOptions::default())
        .await;
    assert!(matches!(res, Err(Error::UnknownQueue(ref q)) if q == "nope"));
    Ok(())
}

/// With worker_concurrency(1), at most one workflow from the queue runs at a
/// time on this executor.
#[tokio::test]
async fn worker_concurrency_is_enforced() -> Result<()> {
    static RUNNING: AtomicUsize = AtomicUsize::new(0);
    static MAX_SEEN: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("tracked", |_ctx: DurableContext, _: ()| async move {
        let now = RUNNING.fetch_add(1, Ordering::SeqCst) + 1;
        MAX_SEEN.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(40)).await;
        RUNNING.fetch_sub(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("serial").worker_concurrency(1));
    engine.launch().await?;

    let mut handles = Vec::new();
    for i in 0..3 {
        handles.push(
            engine
                .enqueue::<_, ()>(
                    "serial",
                    "tracked",
                    (),
                    WorkflowOptions::with_id(format!("wf-conc-{i}")),
                )
                .await?,
        );
    }
    for h in &mut handles {
        h.get_result().await?;
    }

    assert_eq!(
        MAX_SEEN.load(Ordering::SeqCst),
        1,
        "worker_concurrency(1) must serialize queue workflows"
    );
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// On a priority-enabled serialized queue, lower priority values run first
/// regardless of enqueue order.
#[tokio::test]
async fn priority_orders_execution() -> Result<()> {
    let order: Arc<tokio::sync::Mutex<Vec<i64>>> = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let order_wf = order.clone();
    engine.register("record", move |_ctx: DurableContext, n: i64| {
        let order = order_wf.clone();
        async move {
            order.lock().await.push(n);
            Ok::<_, Error>(n)
        }
    });
    engine.register_queue(test_queue("prio").worker_concurrency(1).priority_enabled());

    // Enqueue before launch so all three are pending when dispatch begins.
    for (i, prio) in [(0, 3), (1, 1), (2, 2)] {
        let mut opts = WorkflowOptions::with_id(format!("wf-prio-{i}"));
        opts.priority = prio;
        let _ = engine
            .enqueue::<_, i64>("prio", "record", prio as i64, opts)
            .await?;
    }
    engine.launch().await?;

    // Wait until all three have run.
    let deadline = Instant::now() + Duration::from_secs(3);
    while order.lock().await.len() < 3 {
        assert!(Instant::now() < deadline, "queue did not drain in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*order.lock().await, vec![1, 2, 3]);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A delayed enqueue parks in DELAYED, then runs once the delay expires.
#[tokio::test]
async fn delayed_enqueue_waits_then_runs() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register_queue(test_queue("later"));
    engine.launch().await?;

    let started = Instant::now();
    let mut opts = WorkflowOptions::with_id("wf-delayed");
    opts.delay = Some(Duration::from_millis(150));
    let mut handle = engine
        .enqueue::<_, i64>("later", "echo", 7_i64, opts)
        .await?;

    assert_eq!(handle.get_status().await?.status, STATUS_DELAYED);
    assert_eq!(handle.get_result().await?, 7);
    assert!(
        started.elapsed() >= Duration::from_millis(120),
        "workflow must not run before its delay expires"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// set_workflow_delay reschedules a DELAYED workflow: a far-future delay is
/// shortened so the workflow runs almost immediately. Non-DELAYED workflows are
/// a no-op (returns false, no error).
#[tokio::test]
async fn set_workflow_delay_reschedules() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("echo", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register_queue(test_queue("resched"));
    engine.launch().await?;

    // Enqueue with a 60s delay so it would never run during the test...
    let mut opts = WorkflowOptions::with_id("wf-resched");
    opts.delay = Some(Duration::from_secs(60));
    let mut handle = engine
        .enqueue::<_, i64>("resched", "echo", 9_i64, opts)
        .await?;
    assert_eq!(handle.get_status().await?.status, STATUS_DELAYED);

    // ...then pull it forward to ~20ms from now.
    let started = Instant::now();
    assert!(
        engine
            .set_workflow_delay("wf-resched", Duration::from_millis(20))
            .await?,
        "rescheduling a DELAYED workflow must report a match"
    );
    assert_eq!(handle.get_result().await?, 9);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the workflow must run on the shortened delay, not the original 60s"
    );

    // The now-completed workflow is no longer DELAYED: a further reschedule is a
    // silent no-op. A missing id likewise.
    assert!(
        !engine
            .set_workflow_delay("wf-resched", Duration::ZERO)
            .await?
    );
    assert!(!engine.set_workflow_delay("ghost", Duration::ZERO).await?);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Delay without a queue is rejected.
#[tokio::test]
async fn delay_requires_queue() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    let opts = WorkflowOptions {
        delay: Some(Duration::from_millis(10)),
        ..Default::default()
    };
    let res = engine.run_workflow::<_, ()>("noop", (), opts).await;
    assert!(res.is_err());
    Ok(())
}

/// Two different workflow ids with the same deduplication id on one queue:
/// the second enqueue is rejected.
#[tokio::test]
async fn dedup_id_rejects_duplicates() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("dedup"));

    let mut opts = WorkflowOptions::with_id("wf-dedup-1");
    opts.dedup_id = Some("once".to_string());
    let _first = engine.enqueue::<_, ()>("dedup", "noop", (), opts).await?;

    let mut opts = WorkflowOptions::with_id("wf-dedup-2");
    opts.dedup_id = Some("once".to_string());
    let err = match engine.enqueue::<_, ()>("dedup", "noop", (), opts).await {
        Ok(_) => panic!("same dedup id on the same queue must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::QueueDeduplicated);
    Ok(())
}

/// With `ReturnExisting`, a colliding deduplication id returns a handle to the
/// workflow already holding the slot instead of erroring; a non-default policy
/// without a dedup id is rejected.
#[tokio::test]
async fn dedup_return_existing_returns_the_holder() -> Result<()> {
    use durust::DeduplicationPolicy;
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("dedup"));

    let first = engine
        .enqueue::<_, ()>(
            "dedup",
            "noop",
            (),
            WorkflowOptions::with_id("wf-1").dedup_id("once"),
        )
        .await?;

    let again = engine
        .enqueue::<_, ()>(
            "dedup",
            "noop",
            (),
            WorkflowOptions::with_id("wf-2")
                .dedup_id("once")
                .dedup_policy(DeduplicationPolicy::ReturnExisting),
        )
        .await?;
    assert_eq!(again.id(), first.id(), "returned the slot holder");
    assert_eq!(again.id(), "wf-1");

    assert!(
        engine
            .enqueue::<_, ()>(
                "dedup",
                "noop",
                (),
                WorkflowOptions::with_id("wf-3").dedup_policy(DeduplicationPolicy::ReturnExisting),
            )
            .await
            .is_err(),
        "a non-default policy requires a dedup id"
    );
    Ok(())
}

/// Sending to a workflow id that does not exist is a typed, classifiable error.
#[tokio::test]
async fn send_to_nonexistent_workflow_is_typed() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let err = engine
        .send("no-such-workflow", "hi", "topic")
        .await
        .expect_err("sending to a nonexistent workflow must fail");
    assert_eq!(err.code(), ErrorCode::NonExistentWorkflow);
    Ok(())
}

/// With a rate limit of 2 per long period, only 2 of 4 enqueued workflows
/// start; the rest stay ENQUEUED.
#[tokio::test]
async fn rate_limit_caps_starts() -> Result<()> {
    static STARTED: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("counted", |_ctx: DurableContext, _: ()| async move {
        STARTED.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("limited").rate_limiter(RateLimiter {
        limit: 2,
        period: Duration::from_secs(60),
    }));

    let mut handles = Vec::new();
    for i in 0..4 {
        handles.push(
            engine
                .enqueue::<_, ()>(
                    "limited",
                    "counted",
                    (),
                    WorkflowOptions::with_id(format!("wf-rate-{i}")),
                )
                .await?,
        );
    }
    engine.launch().await?;

    // Give the dispatcher several iterations to (not) over-dispatch.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        STARTED.load(Ordering::SeqCst),
        2,
        "only `limit` workflows may start within the rate period"
    );
    let enqueued = futures_count_enqueued(&mut handles).await?;
    assert_eq!(enqueued, 2, "the overflow workflows must remain ENQUEUED");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The registry reports every registered queue, sorted by name, regardless of
/// which ones this process listens to.
#[tokio::test]
async fn list_registered_queues_is_sorted() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register_queue(WorkflowQueue::new("zebra").worker_concurrency(2));
    engine.register_queue(WorkflowQueue::new("alpha"));

    let names: Vec<String> = engine
        .list_registered_queues()
        .into_iter()
        .map(|q| q.name)
        .collect();
    assert_eq!(names, vec!["alpha".to_string(), "zebra".to_string()]);
    Ok(())
}

/// `listen_queues` dispatches only the named subset: an unlistened queue still
/// accepts enqueues, but nothing claims them in this process.
#[tokio::test]
async fn listen_queues_dispatches_only_listened() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("add_one", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    engine.register_queue(test_queue("listened"));
    engine.register_queue(test_queue("ignored"));
    engine.listen_queues(["listened"]);
    engine.launch().await?;

    // The listened queue runs to completion.
    let mut run = engine
        .enqueue::<_, i64>("listened", "add_one", 41_i64, WorkflowOptions::default())
        .await?;
    assert_eq!(run.get_result().await?, 42);

    // The ignored queue accepts the enqueue but never dispatches it here.
    let idle = engine
        .enqueue::<_, i64>("ignored", "add_one", 1_i64, WorkflowOptions::default())
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        idle.get_status().await?.status,
        STATUS_ENQUEUED,
        "an unlistened queue is not dispatched by this process"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// `ListFilter::queues_only` returns only workflows that are on a queue,
/// excluding directly-run ones.
#[tokio::test]
async fn queues_only_filters_to_queued_workflows() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("q"));
    engine.launch().await?;

    // One direct run, one enqueued.
    engine
        .run_workflow::<_, ()>("noop", (), WorkflowOptions::with_id("direct"))
        .await?
        .get_result()
        .await?;
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
    Ok(())
}

/// A partitioned queue applies worker concurrency *per partition*: with a limit
/// of 1 and two active partitions, one workflow from each runs at once, so two
/// run concurrently overall.
#[tokio::test]
async fn partitioned_queue_concurrency_is_per_partition() -> Result<()> {
    static CURRENT: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("work", |ctx: DurableContext, _: ()| async move {
        let now = CURRENT.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK.fetch_max(now, Ordering::SeqCst);
        ctx.sleep(Duration::from_millis(80)).await?;
        CURRENT.fetch_sub(1, Ordering::SeqCst);
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("pq").partitioned().worker_concurrency(1));
    engine.launch().await?;

    // Two workflows in each of two partitions.
    let mut handles = Vec::new();
    for part in ["a", "a", "b", "b"] {
        handles.push(
            engine
                .enqueue::<_, ()>(
                    "pq",
                    "work",
                    (),
                    WorkflowOptions::default().partition_key(part),
                )
                .await?,
        );
    }
    for h in &mut handles {
        h.get_result().await?;
    }

    assert_eq!(
        PEAK.load(Ordering::SeqCst),
        2,
        "each partition runs one at a time, but the two partitions run in parallel"
    );

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// A workflow enqueued to a partitioned queue without a partition key is never
/// dispatched (partition discovery only sees keyed rows).
#[tokio::test]
async fn partitioned_queue_ignores_keyless_enqueue() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(test_queue("pq").partitioned());
    engine.launch().await?;

    let idle = engine
        .enqueue::<_, ()>("pq", "noop", (), WorkflowOptions::default())
        .await?;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        idle.get_status().await?.status,
        STATUS_ENQUEUED,
        "no partition key means nothing dispatches it"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

async fn futures_count_enqueued(handles: &mut [durust::WorkflowHandle<()>]) -> Result<usize> {
    let mut n = 0;
    for h in handles {
        if h.get_status().await?.status == STATUS_ENQUEUED {
            n += 1;
        }
    }
    Ok(n)
}
