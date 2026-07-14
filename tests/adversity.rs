//! Adversarial multi-executor tests: several engines — each with its own
//! executor id and its own connection pool, as separate processes would be —
//! race over one Postgres database. These assert the fleet-level invariants
//! the single-engine suites only imply:
//!
//! - a queued workflow runs **exactly once** no matter how many dispatchers
//!   compete for it (no double-claim, no double-step);
//! - queue deduplication admits **exactly one** of many concurrent
//!   contenders, and the losers get the typed error;
//! - recovery honors **executor ownership**: an executor's launch-recovery
//!   never steals another live executor's pending work, while an explicit
//!   takeover by executor id still can.
//!
//! Skipped unless `DATABASE_URL` points at a reachable Postgres:
//!
//! ```text
//! createdb durare_test && DATABASE_URL=postgres://localhost/durare_test cargo test --test adversity
//! ```

use durare::{
    DurableContext, DurableEngine, EngineConfig, Error, ErrorCode, PostgresProvider, Result,
    StateProvider, WorkflowOptions, WorkflowQueue, STATUS_PENDING, STATUS_SUCCESS,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty())
}

/// An engine over its own pool with its own executor id — the in-process
/// stand-in for a separate fleet member.
async fn fleet_engine(url: &str, executor_id: &str) -> Result<DurableEngine> {
    let provider = PostgresProvider::connect(url).await?;
    let config = EngineConfig::default().executor_id(executor_id);
    DurableEngine::with_config(Arc::new(provider), config).await
}

/// Per-workflow-input execution counters, shared across every engine in the
/// process. Body entries count *executions* (a replayed body re-enters, so in
/// crash-free tests the expected count is exactly 1); step entries count side
/// effects (checkpointed — must be exactly 1 even across replays).
fn body_runs() -> &'static Mutex<HashMap<String, usize>> {
    static M: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    M.get_or_init(Default::default)
}
fn step_runs() -> &'static Mutex<HashMap<String, usize>> {
    static M: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    M.get_or_init(Default::default)
}
fn bump(map: &'static Mutex<HashMap<String, usize>>, key: &str) {
    *map.lock().unwrap().entry(key.to_string()).or_insert(0) += 1;
}

/// Three dispatchers, one queue, thirty tasks: every task must reach SUCCESS
/// having executed exactly once — `FOR UPDATE SKIP LOCKED` claiming means
/// competing dispatchers take disjoint sets, never the same row twice.
#[tokio::test]
async fn pg_queued_work_runs_exactly_once_across_executors() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_queued_work_runs_exactly_once_across_executors: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let queue = format!("race-q-{tag}");
    let wf = format!("race-task-{tag}");

    let mut engines = Vec::new();
    for i in 0..3 {
        let mut engine = fleet_engine(&url, &format!("exec-{i}-{tag}")).await?;
        engine.register(&wf, |ctx: DurableContext, task: String| async move {
            bump(body_runs(), &task);
            ctx.step("work", || async {
                // Wide enough for the other dispatchers to be polling while
                // this run is in flight.
                tokio::time::sleep(Duration::from_millis(50)).await;
                bump(step_runs(), &task);
                Ok::<_, Error>(())
            })
            .await
        });
        engine.register_queue(WorkflowQueue::new(&queue));
        engine.launch().await?;
        engines.push(engine);
    }

    // Enqueue thirty tasks; any engine can enqueue, every engine may claim.
    let mut handles = Vec::new();
    for n in 0..30 {
        let task = format!("task-{n}-{tag}");
        let opts = WorkflowOptions {
            workflow_id: Some(task.clone()),
            queue: Some(queue.clone()),
            ..Default::default()
        };
        handles.push((
            task.clone(),
            engines[0].start::<String, ()>(&wf, task, opts).await?,
        ));
    }
    for (_, handle) in &handles {
        handle.result().await?;
    }

    for (task, _) in &handles {
        assert_eq!(
            body_runs().lock().unwrap().get(task).copied(),
            Some(1),
            "{task}: dispatched exactly once across the fleet"
        );
        assert_eq!(
            step_runs().lock().unwrap().get(task).copied(),
            Some(1),
            "{task}: side effect ran exactly once"
        );
    }

    for engine in &engines {
        engine.shutdown(Duration::from_secs(10)).await?;
    }
    Ok(())
}

/// Twelve concurrent starts across three engines, one deduplication id:
/// exactly one is admitted, the other eleven get the typed
/// `QueueDeduplicated` error, and the workflow body runs once.
#[tokio::test]
async fn pg_dedup_admits_exactly_one_under_contention() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_dedup_admits_exactly_one_under_contention: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let queue = format!("dedup-q-{tag}");
    let wf = format!("dedup-task-{tag}");
    let wf_key = format!("dedup-winner-{tag}");

    let mut engines = Vec::new();
    for i in 0..3 {
        let mut engine = fleet_engine(&url, &format!("dexec-{i}-{tag}")).await?;
        let key = wf_key.clone();
        engine.register(&wf, move |ctx: DurableContext, _: ()| {
            let key = key.clone();
            async move {
                bump(body_runs(), &key);
                // Long enough that every contender races the *active* winner,
                // not a completed one (the dedup slot frees on completion).
                ctx.step("hold", || async {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Ok::<_, Error>(())
                })
                .await
            }
        });
        engine.register_queue(WorkflowQueue::new(&queue));
        engine.launch().await?;
        engines.push(engine);
    }
    let engines = Arc::new(engines);

    // Fire all twelve starts concurrently, four per engine.
    let admitted = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for n in 0..12 {
        let engines = engines.clone();
        let (queue, wf) = (queue.clone(), wf.clone());
        let (admitted, rejected) = (admitted.clone(), rejected.clone());
        tasks.push(tokio::spawn(async move {
            let opts = WorkflowOptions {
                queue: Some(queue),
                dedup_id: Some("golden".to_string()),
                ..Default::default()
            };
            match engines[n % 3].start::<(), ()>(&wf, (), opts).await {
                Ok(_) => admitted.fetch_add(1, Ordering::SeqCst),
                Err(e) => {
                    assert_eq!(
                        e.code(),
                        ErrorCode::QueueDeduplicated,
                        "losers fail with the typed dedup error, got: {e}"
                    );
                    rejected.fetch_add(1, Ordering::SeqCst)
                }
            }
        }));
    }
    for t in tasks {
        t.await.expect("contender task panicked");
    }

    assert_eq!(admitted.load(Ordering::SeqCst), 1, "exactly one admitted");
    assert_eq!(rejected.load(Ordering::SeqCst), 11, "the rest deduplicated");

    // Wait for a dispatcher to run the winner, give its hold step time to
    // finish, and confirm no second execution ever appeared.
    for _ in 0..100 {
        if body_runs().lock().unwrap().get(&wf_key).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        body_runs().lock().unwrap().get(&wf_key).copied(),
        Some(1),
        "the single winner executed exactly once"
    );

    for engine in engines.iter() {
        engine.shutdown(Duration::from_secs(10)).await?;
    }
    Ok(())
}

/// Recovery ownership: executor B's launch-recovery must not steal executor
/// A's pending workflow (A might still be running it) — but an explicit,
/// operator-style takeover of A's executor id re-dispatches it.
#[tokio::test]
async fn pg_recovery_honors_executor_ownership() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_recovery_honors_executor_ownership: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let wf = format!("crash-once-{tag}");
    let wf_id = format!("wf-owned-{tag}");
    let exec_a = format!("owner-a-{tag}");
    let exec_b = format!("owner-b-{tag}");
    let attempts = Arc::new(AtomicUsize::new(0));

    let register = |engine: &mut DurableEngine, attempts: Arc<AtomicUsize>| {
        engine.register(&wf, move |_ctx: DurableContext, _: ()| {
            let attempts = attempts.clone();
            async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("boom on the first attempt");
                }
                Ok::<_, Error>(())
            }
        });
    };

    // Executor A runs the workflow; it panics and is left PENDING, owned by A.
    {
        let mut a = fleet_engine(&url, &exec_a).await?;
        register(&mut a, attempts.clone());
        let _ = a
            .start::<(), ()>(&wf, (), WorkflowOptions::with_id(&wf_id))
            .await?
            .result()
            .await;
    }
    let probe = PostgresProvider::connect(&url).await?;
    assert_eq!(
        probe.get_workflow_status(&wf_id).await?.unwrap().status,
        STATUS_PENDING,
        "A's crashed workflow is left recoverable"
    );

    // Executor B launches with recover_on_launch: it recovers *its own*
    // pending work only, so A's row must stay untouched.
    let provider_b = PostgresProvider::connect(&url).await?;
    let config_b = EngineConfig::default()
        .executor_id(exec_b.as_str())
        .recover_on_launch(true);
    let mut b = DurableEngine::with_config(Arc::new(provider_b), config_b).await?;
    register(&mut b, attempts.clone());
    b.launch().await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        probe.get_workflow_status(&wf_id).await?.unwrap().status,
        STATUS_PENDING,
        "B's launch-recovery does not steal A's pending workflow"
    );

    // An explicit takeover of A's executor id — the operator handoff — does.
    let recovered = b.recover_pending_for(std::slice::from_ref(&exec_a)).await?;
    assert!(
        recovered.contains(&wf_id),
        "explicit recovery by executor id takes over A's workflow"
    );
    assert_eq!(
        probe.get_workflow_status(&wf_id).await?.unwrap().status,
        STATUS_SUCCESS,
        "the taken-over workflow completes"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "crashed once under A, recovered once under B"
    );

    b.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
