#![cfg(feature = "sqlite")]
//! Crash-tolerance of the scheduler at the three interesting points in a tick's
//! life: after it is persisted but before it runs, *during* the run, and after
//! the run but before the schedule is rescheduled (`last_fired_at`).
//!
//! These use `fail`'s process-global registry, so they must not overlap — each
//! takes `SERIAL` for its whole body. The `schedule_tick_*` failpoints live in
//! the schedule fire loop and are no-ops unless armed here.

use durare::{
    DurableContext, DurableEngine, Error, ListFilter, Result, ScheduleOptions, ScheduledInput,
    SqliteProvider, StateProvider, STATUS_PENDING, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Serializes the tests: `fail`'s registry is global, and every engine's fire
/// loop in this process consults the same failpoint names. An async mutex so the
/// guard can be held across the tests' awaits.
static SERIAL: Mutex<()> = Mutex::const_new(());

fn temp_db_url(tag: &str) -> (String, std::path::PathBuf) {
    let mut p = std::env::temp_dir();
    p.push(format!("durare-{tag}-{}.db", uuid::Uuid::new_v4()));
    (format!("sqlite://{}", p.display()), p)
}

async fn sched_rows(provider: &SqliteProvider) -> Result<Vec<durare::WorkflowStatus>> {
    provider
        .list_workflows(&ListFilter {
            workflow_id_prefix: vec!["sched-".to_string()],
            ..Default::default()
        })
        .await
}

/// After a tick is persisted, an abrupt failure before it runs must not lose or
/// duplicate the tick: recovery completes the orphaned PENDING row exactly once.
#[tokio::test]
async fn tick_survives_crash_before_run() -> Result<()> {
    let _serial = SERIAL.lock().await;
    static WORK: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("fp-before-run");

    let register = |engine: &mut DurableEngine| {
        engine.register(
            "job",
            |ctx: DurableContext, _at: ScheduledInput| async move {
                ctx.step("work", || async {
                    WORK.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await?;
                Ok::<_, Error>(())
            },
        );
    };

    // Persist exactly one tick, then trip the failpoint to abort the fire loop
    // before the workflow runs.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        engine
            .create_schedule("tick", "job", "* * * * * *", ScheduleOptions::new())
            .await?;
        fail::cfg("schedule_tick_after_persist", "return").expect("arm");
        engine.launch().await?;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        engine.shutdown(Duration::from_secs(1)).await?;
        fail::remove("schedule_tick_after_persist");
    }

    let provider = SqliteProvider::connect(&url).await?;
    let rows = sched_rows(&provider).await?;
    assert_eq!(rows.len(), 1, "exactly one tick persisted");
    assert_eq!(rows[0].status, STATUS_PENDING, "tick never ran");
    assert_eq!(WORK.load(Ordering::SeqCst), 0, "no work before the crash");
    let tick_id = rows[0].id.clone();

    // A fresh engine recovers the orphaned tick and runs it to completion.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        assert!(engine.recover().await? >= 1, "recovery picked up the tick");
    }
    assert_eq!(WORK.load(Ordering::SeqCst), 1, "recovered tick ran once");
    assert_eq!(
        provider
            .get_workflow_status(&tick_id)
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A crash *during* a scheduled run replays from checkpoints: a step that already
/// committed before the crash is not re-run, and the rest completes — exactly
/// once per tick, however many ticks fired.
#[tokio::test]
async fn tick_replays_after_crash_during_run() -> Result<()> {
    let _serial = SERIAL.lock().await;
    static S1: AtomicUsize = AtomicUsize::new(0);
    static S2: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("fp-during-run");

    let register = |engine: &mut DurableEngine| {
        engine.register(
            "job",
            |ctx: DurableContext, _at: ScheduledInput| async move {
                ctx.step("s1", || async {
                    S1.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await?;
                // Crash between the two steps once s1 is checkpointed.
                fail::fail_point!("scheduled_job_mid_run");
                ctx.step("s2", || async {
                    S2.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await?;
                Ok::<_, Error>(())
            },
        );
    };

    // Each fired tick runs s1, then panics (leaving the row PENDING with s1
    // checkpointed) before s2.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        engine
            .create_schedule("tick", "job", "* * * * * *", ScheduleOptions::new())
            .await?;
        fail::cfg("scheduled_job_mid_run", "panic").expect("arm");
        engine.launch().await?;
        tokio::time::sleep(Duration::from_millis(2500)).await;
        engine.shutdown(Duration::from_secs(1)).await?;
        fail::remove("scheduled_job_mid_run");
    }

    let provider = SqliteProvider::connect(&url).await?;
    let rows = sched_rows(&provider).await?;
    let n = rows.len();
    assert!(n >= 1, "at least one tick fired");
    assert!(
        rows.iter().all(|r| r.status == STATUS_PENDING),
        "all crashed mid-run"
    );
    assert_eq!(S1.load(Ordering::SeqCst), n, "s1 ran once per fired tick");
    assert_eq!(
        S2.load(Ordering::SeqCst),
        0,
        "s2 never reached before the crash"
    );

    // Recovery replays each tick: s1 from its checkpoint (not re-run), s2 to
    // completion.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register(&mut engine);
        assert!(engine.recover().await? >= 1, "recovery picked up the ticks");
    }
    assert_eq!(S1.load(Ordering::SeqCst), n, "s1 not re-run on replay");
    assert_eq!(S2.load(Ordering::SeqCst), n, "s2 ran exactly once per tick");
    let rows = sched_rows(&provider).await?;
    assert!(
        rows.iter().all(|r| r.status == STATUS_SUCCESS),
        "every tick finished after recovery"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A crash after the tick was dispatched but before `last_fired_at` is recorded
/// does not affect the run — the dispatched workflow still completes exactly
/// once; only the bookkeeping write is skipped.
#[tokio::test]
async fn tick_completes_when_crash_before_reschedule() -> Result<()> {
    let _serial = SERIAL.lock().await;
    static WORK: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = temp_db_url("fp-before-resched");

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register(
        "job",
        |ctx: DurableContext, _at: ScheduledInput| async move {
            ctx.step("work", || async {
                WORK.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            })
            .await?;
            Ok::<_, Error>(())
        },
    );
    engine
        .create_schedule("tick", "job", "* * * * * *", ScheduleOptions::new())
        .await?;

    // The fire loop dispatches one tick, then aborts before recording
    // last_fired_at. shutdown drains the dispatched run, which completes.
    fail::cfg("schedule_tick_before_reschedule", "return").expect("arm");
    engine.launch().await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    engine.shutdown(Duration::from_secs(2)).await?;
    fail::remove("schedule_tick_before_reschedule");

    assert_eq!(
        WORK.load(Ordering::SeqCst),
        1,
        "the dispatched tick ran once"
    );
    let provider = SqliteProvider::connect(&url).await?;
    let rows = sched_rows(&provider).await?;
    assert_eq!(rows.len(), 1, "exactly one tick");
    assert_eq!(rows[0].status, STATUS_SUCCESS, "tick completed");
    let schedule = engine.get_schedule("tick").await?.expect("schedule");
    assert!(
        schedule.last_fired_at.is_none(),
        "last_fired_at was not recorded across the crash"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}
