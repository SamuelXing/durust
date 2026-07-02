//! Runtime schedule CRUD + firing. This binary deliberately defines no
//! `#[workflow(schedule = …)]` workflow, so the only schedules are the ones
//! these tests create — keeping firing counts isolated.

use durust::{
    DurableContext, DurableEngine, InMemoryProvider, Result, ScheduleFilter, ScheduleOptions,
    ScheduleStatus, ScheduledInput,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static CREATED_RUNS: AtomicUsize = AtomicUsize::new(0);
/// The context each fired tick received, so the test can assert the schedule's
/// configured context reaches the workflow — not just that get/list surface it.
static SEEN_CONTEXT: Mutex<Option<String>> = Mutex::new(None);

/// A schedule created at runtime is persisted, fires on its cron ticks, is
/// surfaced by get/list, stamps `last_fired_at`, and stops firing once paused.
/// The fired workflow also receives the configured context in its input.
#[tokio::test]
async fn create_schedule_fires_and_pauses() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(
        "created_tick",
        |_ctx: DurableContext, tick: ScheduledInput| async move {
            CREATED_RUNS.fetch_add(1, Ordering::SeqCst);
            *SEEN_CONTEXT.lock().unwrap() = tick.context_as::<String>().unwrap();
            Ok::<_, durust::Error>(())
        },
    );

    // Unknown workflow and bad cron are rejected.
    assert!(engine
        .create_schedule("bad", "no_such_wf", "* * * * * *", ScheduleOptions::new())
        .await
        .is_err());
    assert!(engine
        .create_schedule("bad", "created_tick", "not a cron", ScheduleOptions::new())
        .await
        .is_err());

    engine
        .create_schedule(
            "every-sec",
            "created_tick",
            "* * * * * *",
            ScheduleOptions::new().context(&"hello"),
        )
        .await?;
    // A duplicate name is rejected.
    assert!(engine
        .create_schedule(
            "every-sec",
            "created_tick",
            "* * * * * *",
            ScheduleOptions::new()
        )
        .await
        .is_err());

    // get/list surface the new schedule with its context.
    let got = engine.get_schedule("every-sec").await?.expect("exists");
    assert_eq!(got.workflow_name, "created_tick");
    assert_eq!(got.status, ScheduleStatus::Active);
    assert_eq!(got.context.as_ref().and_then(|v| v.as_str()), Some("hello"));
    assert!(got.last_fired_at.is_none());
    assert_eq!(
        engine
            .list_schedules(&ScheduleFilter::default())
            .await?
            .len(),
        1
    );

    engine.launch().await?;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    engine.pause_schedule("every-sec").await?;
    // Let the reconciler retire the firing loop, then record the run count.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let after_pause = CREATED_RUNS.load(Ordering::SeqCst);
    engine.shutdown(Duration::from_secs(1)).await?;

    assert!(after_pause >= 1, "the created schedule should have fired");
    // The configured context ("hello") was delivered to the fired workflow.
    assert_eq!(
        SEEN_CONTEXT.lock().unwrap().as_deref(),
        Some("hello"),
        "the schedule's context must reach the workflow's input"
    );

    // Paused schedule is reflected and last_fired_at was stamped.
    let paused = engine.get_schedule("every-sec").await?.expect("exists");
    assert_eq!(paused.status, ScheduleStatus::Paused);
    assert!(paused.last_fired_at.is_some(), "last_fired_at recorded");

    // Filtering by status reflects the pause.
    assert!(engine
        .list_schedules(&ScheduleFilter {
            statuses: vec![ScheduleStatus::Active],
            ..Default::default()
        })
        .await?
        .is_empty());

    // Delete removes it.
    assert!(engine.delete_schedule("every-sec").await?);
    assert!(engine.get_schedule("every-sec").await?.is_none());
    Ok(())
}

/// list_schedules honors workflow-name and name-prefix filters.
#[tokio::test]
async fn list_schedules_filters() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf_a",
        |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, durust::Error>(()) },
    );
    engine.register(
        "wf_b",
        |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, durust::Error>(()) },
    );

    engine
        .create_schedule("nightly-a", "wf_a", "0 0 0 * * *", ScheduleOptions::new())
        .await?;
    engine
        .create_schedule("nightly-b", "wf_b", "0 0 0 * * *", ScheduleOptions::new())
        .await?;
    engine
        .create_schedule("weekly-a", "wf_a", "0 0 1 * * *", ScheduleOptions::new())
        .await?;

    // By workflow name.
    let by_wf = engine
        .list_schedules(&ScheduleFilter {
            workflow_names: vec!["wf_a".to_string()],
            ..Default::default()
        })
        .await?;
    let names: Vec<&str> = by_wf.iter().map(|s| s.schedule_name.as_str()).collect();
    assert_eq!(names, vec!["nightly-a", "weekly-a"]);

    // By name prefix.
    let by_prefix = engine
        .list_schedules(&ScheduleFilter {
            name_prefixes: vec!["nightly".to_string()],
            ..Default::default()
        })
        .await?;
    let names: Vec<&str> = by_prefix.iter().map(|s| s.schedule_name.as_str()).collect();
    assert_eq!(names, vec!["nightly-a", "nightly-b"]);

    Ok(())
}

/// Resume re-activates a paused schedule (status returns to Active); pausing or
/// resuming an unknown name is a no-op that returns `false` rather than erroring.
#[tokio::test]
async fn resume_reactivates_and_unknown_name_is_a_noop() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "wf",
        |_ctx: DurableContext, _at: ScheduledInput| async move { Ok::<_, durust::Error>(()) },
    );
    engine
        .create_schedule("s", "wf", "0 0 12 * * *", ScheduleOptions::new())
        .await?;
    assert_eq!(
        engine.get_schedule("s").await?.unwrap().status,
        ScheduleStatus::Active
    );

    assert!(engine.pause_schedule("s").await?);
    assert_eq!(
        engine.get_schedule("s").await?.unwrap().status,
        ScheduleStatus::Paused
    );

    // Resume flips it back to Active.
    assert!(engine.resume_schedule("s").await?, "resume returns true");
    assert_eq!(
        engine.get_schedule("s").await?.unwrap().status,
        ScheduleStatus::Active,
        "resume re-activates the schedule"
    );

    // An unknown name is a no-op returning false (not an error).
    assert!(!engine.pause_schedule("nope").await?);
    assert!(!engine.resume_schedule("nope").await?);
    Ok(())
}
