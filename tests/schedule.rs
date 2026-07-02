//! Scheduled (cron) workflow tests. The scheduled workflow below is
//! auto-registered (via `inventory`) only in this test binary.

use durust::{
    DurableContext, DurableEngine, InMemoryProvider, ListFilter, Result, ScheduleFilter,
    ScheduledInput, StateProvider,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static RUNS: AtomicUsize = AtomicUsize::new(0);

/// Fires every second (6-field cron: sec min hour dom mon dow). Receives the
/// scheduled tick time and (unset here) context as its [`ScheduledInput`].
#[durust::workflow(schedule = "* * * * * *")]
async fn cron_tick(_ctx: DurableContext, _tick: ScheduledInput) -> Result<()> {
    RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// The cron schedule fires, and each tick runs exactly once even with two
/// executors sharing one backend (the deterministic `sched-…` id + idempotent
/// insert dedup the tick across executors).
#[tokio::test]
async fn cron_fires_once_per_tick_across_executors() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let a = DurableEngine::new(provider.clone()).await?;
    let b = DurableEngine::new(provider.clone()).await?;
    a.launch().await?;
    b.launch().await?;

    // ~2 one-second ticks.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // shutdown drains in-flight scheduled runs before we read counters.
    a.shutdown(Duration::from_secs(1)).await?;
    b.shutdown(Duration::from_secs(1)).await?;

    let rows = provider
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("sched-".to_string()),
            ..Default::default()
        })
        .await?;
    let runs = RUNS.load(Ordering::SeqCst);

    assert!(
        runs >= 1,
        "the cron schedule should have fired at least once"
    );
    assert_eq!(
        runs,
        rows.len(),
        "each tick must run exactly once across both executors"
    );
    // Every scheduled run is keyed by tick time.
    assert!(rows.iter().all(|r| r.id.starts_with("sched-cron_tick-")));

    // A `#[workflow(schedule)]` schedule is ephemeral (decorator-style): it
    // fires, but nothing is written to `workflow_schedules`. The managed/CRUD
    // view is empty; only `create_schedule` persists a row.
    assert!(
        provider
            .list_schedules(&ScheduleFilter::default())
            .await?
            .is_empty(),
        "macro schedules must not be persisted to the schedule table"
    );
    Ok(())
}

/// The registry lists both auto- and manually-registered workflows (sorted, with
/// schedules surfaced), and the scheduled-only view drops the unscheduled ones.
#[tokio::test]
async fn lists_registered_and_scheduled_workflows() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("manual_noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, durust::Error>(())
    });

    let all = engine.list_registered_workflows();
    // The manual workflow is present and unscheduled.
    assert!(all
        .iter()
        .any(|w| w.name == "manual_noop" && w.cron_schedule.is_none()));
    // The auto-registered cron workflow carries its schedule.
    let tick = all
        .iter()
        .find(|w| w.name == "cron_tick")
        .expect("cron_tick is auto-registered in this binary");
    assert_eq!(tick.cron_schedule.as_deref(), Some("* * * * * *"));
    // Sorted by name.
    let names: Vec<&str> = all.iter().map(|w| w.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    // Scheduled-only keeps cron_tick, drops manual_noop.
    let scheduled = engine.list_scheduled_workflows();
    assert!(scheduled.iter().all(|w| w.cron_schedule.is_some()));
    assert!(scheduled.iter().any(|w| w.name == "cron_tick"));
    assert!(!scheduled.iter().any(|w| w.name == "manual_noop"));
    Ok(())
}
