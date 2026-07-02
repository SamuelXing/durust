//! Schedule management beyond basic CRUD: `apply_schedules` (batch
//! create-or-replace), `backfill_schedule`, `trigger_schedule`, and
//! timezone-aware firing. This binary defines no `#[workflow(schedule)]`, so no
//! macro schedule pollutes the reconciler.

use chrono::{TimeZone, Utc};
use durust::{
    ApplySchedule, DurableContext, DurableEngine, Error, InMemoryProvider, Result, ScheduleFilter,
    ScheduleOptions, ScheduledInput, StateProvider, WorkflowHandle,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// `apply_schedules` creates each schedule and replaces an existing one of the
/// same name with a fresh `schedule_id`; a batch with any invalid entry is
/// rejected whole (nothing is written).
#[tokio::test]
async fn apply_creates_replaces_and_validates() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf",
        |_ctx: DurableContext, _at: ScheduledInput| async move { Ok::<_, Error>(()) },
    );

    engine
        .apply_schedules(vec![
            ApplySchedule::new("a", "wf", "* * * * * *"),
            ApplySchedule::new("b", "wf", "0 0 * * * *"),
        ])
        .await?;
    let all = engine.list_schedules(&ScheduleFilter::default()).await?;
    assert_eq!(all.len(), 2, "both schedules created");
    let id_a = engine.get_schedule("a").await?.unwrap().schedule_id;

    // Re-apply "a" with a new cron: it is replaced (new id, new spec); "b"
    // (untouched by this batch) survives.
    engine
        .apply_schedules(vec![ApplySchedule::new("a", "wf", "0 0 1 * * *")])
        .await?;
    let a = engine.get_schedule("a").await?.unwrap();
    assert_ne!(
        a.schedule_id, id_a,
        "replacement minted a fresh schedule_id"
    );
    assert_eq!(a.schedule, "0 0 1 * * *", "spec replaced");
    assert!(engine.get_schedule("b").await?.is_some(), "b untouched");

    // A batch with a bad entry rejects whole: the good entry is not written.
    let err = engine
        .apply_schedules(vec![
            ApplySchedule::new("c", "wf", "0 0 1 * * *"),
            ApplySchedule::new("d", "wf", "not a cron"),
        ])
        .await;
    assert!(err.is_err(), "invalid cron rejects the batch");
    assert!(
        engine.get_schedule("c").await?.is_none(),
        "no partial write"
    );

    // Unknown workflow and empty name are also rejected.
    assert!(engine
        .apply_schedules(vec![ApplySchedule::new("e", "missing", "* * * * * *")])
        .await
        .is_err());
    assert!(engine
        .apply_schedules(vec![ApplySchedule::new("", "wf", "* * * * * *")])
        .await
        .is_err());
    Ok(())
}

/// `backfill_schedule` fires each cron tick in a past range exactly once and
/// returns every tick id; re-running the same range does not re-run them.
#[tokio::test]
async fn backfill_fires_each_tick_once() -> Result<()> {
    static WORK: AtomicUsize = AtomicUsize::new(0);
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf",
        |_ctx: DurableContext, _at: ScheduledInput| async move {
            WORK.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        },
    );
    // Daily at noon UTC.
    engine
        .create_schedule("daily", "wf", "0 0 12 * * *", ScheduleOptions::new())
        .await?;

    // Three days → three noon ticks.
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 1, 4, 0, 0, 0).unwrap();
    let ids = engine.backfill_schedule("daily", start, end).await?;
    assert_eq!(ids.len(), 3, "one tick per day at noon");
    assert!(
        ids[0].contains("2026-01-01T12:00:00"),
        "ids carry tick time"
    );

    // Let the backfilled direct runs finish.
    for _ in 0..50 {
        if WORK.load(Ordering::SeqCst) == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(WORK.load(Ordering::SeqCst), 3, "each tick ran once");

    // Backfilling the same range again returns the ids but does not re-run.
    let again = engine.backfill_schedule("daily", start, end).await?;
    assert_eq!(again, ids, "same deterministic ids");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(WORK.load(Ordering::SeqCst), 3, "no duplicate runs");

    assert!(
        engine
            .backfill_schedule("missing", start, end)
            .await
            .is_err(),
        "unknown schedule rejected"
    );
    Ok(())
}

/// `trigger_schedule` runs the schedule's workflow once immediately and returns
/// a handle to it; the id is a distinct `-trigger-` id.
#[tokio::test]
async fn trigger_runs_once_now() -> Result<()> {
    static WORK: AtomicUsize = AtomicUsize::new(0);
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf",
        |_ctx: DurableContext, _at: ScheduledInput| async move {
            WORK.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>("done".to_string())
        },
    );
    engine
        .create_schedule("s", "wf", "0 0 12 * * *", ScheduleOptions::new())
        .await?;

    let mut handle: WorkflowHandle<String> = engine.trigger_schedule("s").await?;
    assert!(
        handle.id().starts_with("sched-s-trigger-"),
        "distinct trigger id"
    );
    assert_eq!(handle.get_result().await?, "done");
    assert_eq!(WORK.load(Ordering::SeqCst), 1, "ran exactly once");

    assert!(
        engine.trigger_schedule::<String>("missing").await.is_err(),
        "unknown schedule rejected"
    );
    Ok(())
}

/// A timezone shifts the wall-clock cron tick to a different UTC instant. Noon
/// `Asia/Tokyo` (UTC+9, no DST) is 03:00 UTC; the backfilled ids prove it.
#[tokio::test]
async fn timezone_shifts_the_fired_instant() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf",
        |_ctx: DurableContext, _at: ScheduledInput| async move { Ok::<_, Error>(()) },
    );
    engine
        .create_schedule(
            "tokyo",
            "wf",
            "0 0 12 * * *",
            ScheduleOptions::new().cron_timezone("Asia/Tokyo"),
        )
        .await?;

    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
    let ids = engine.backfill_schedule("tokyo", start, end).await?;
    assert_eq!(ids.len(), 1, "one noon-Tokyo tick on Jan 1");
    assert!(
        ids[0].contains("2026-01-01T03:00:00"),
        "noon JST is 03:00 UTC, got {}",
        ids[0]
    );

    // An invalid timezone is rejected at create time.
    assert!(
        engine
            .create_schedule(
                "bad",
                "wf",
                "0 0 12 * * *",
                ScheduleOptions::new().cron_timezone("Not/AZone"),
            )
            .await
            .is_err(),
        "invalid timezone rejected"
    );
    Ok(())
}

/// A schedule with `automatic_backfill` catches up ticks it missed while down:
/// on launch the reconciler backfills from the recorded `last_fired_at` to now,
/// so a fresh engine runs the missed ticks rather than only firing live. Without
/// the flag those ticks would be silently dropped.
#[tokio::test]
async fn automatic_backfill_catches_up_missed_ticks_on_launch() -> Result<()> {
    static WORK: AtomicUsize = AtomicUsize::new(0);
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(
        "beat",
        |_ctx: DurableContext, _at: ScheduledInput| async move {
            WORK.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        },
    );
    // Fire every second, with automatic backfill enabled.
    engine
        .create_schedule(
            "beat",
            "beat",
            "* * * * * *",
            ScheduleOptions::new().automatic_backfill(true),
        )
        .await?;
    // Simulate the schedule having last fired ~10s ago, before a restart: those
    // ~10 missed ticks must be caught up when the reconciler installs the loop.
    let ten_ago = (Utc::now() - chrono::Duration::seconds(10)).timestamp_millis();
    provider.set_schedule_last_fired("beat", ten_ago).await?;

    engine.launch().await?;

    // The reconciler backfills the missed ticks immediately (onto the internal
    // queue), so the count climbs fast. A live loop alone fires ~1/sec, so
    // reaching 5 well before ~5 seconds have passed can only be the catch-up.
    let mut caught_up = false;
    for _ in 0..150 {
        if WORK.load(Ordering::SeqCst) >= 5 {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        caught_up,
        "automatic backfill should catch up the missed ticks on launch (got {})",
        WORK.load(Ordering::SeqCst)
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
