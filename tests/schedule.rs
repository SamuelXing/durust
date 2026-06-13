//! Scheduled (cron) workflow tests. The scheduled workflow below is
//! auto-registered (via `inventory`) only in this test binary.

use durust::{DurableContext, DurableEngine, InMemoryProvider, ListFilter, Result, StateProvider};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static RUNS: AtomicUsize = AtomicUsize::new(0);

/// Fires every second (6-field cron: sec min hour dom mon dow). Receives the
/// scheduled tick time (RFC 3339) as input.
#[durust::workflow(schedule = "* * * * * *")]
async fn cron_tick(_ctx: DurableContext, _scheduled_at: String) -> Result<()> {
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
    Ok(())
}
