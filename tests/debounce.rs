//! Workflow debouncing: rapid repeated triggers grouped by a key coalesce into
//! a single delayed run with the latest input.

use durust::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowHandle};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn debounce_coalesces_to_latest_input() -> Result<()> {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("notify", |_ctx: DurableContext, msg: String| async move {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(msg)
    });
    engine.launch().await?;

    let delay = Duration::from_millis(250);
    // Three rapid calls within the window: each pushes the run back and replaces
    // the input. Inline `debouncer(..)` so no engine borrow is held afterward.
    let h1: WorkflowHandle<String> = engine
        .debouncer("notify")
        .debounce("k", delay, "a".to_string())
        .await?;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let h2: WorkflowHandle<String> = engine
        .debouncer("notify")
        .debounce("k", delay, "b".to_string())
        .await?;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut h3: WorkflowHandle<String> = engine
        .debouncer("notify")
        .debounce("k", delay, "c".to_string())
        .await?;

    // Every call for the active key points at the same eventual run.
    assert_eq!(h1.id(), h2.id());
    assert_eq!(h2.id(), h3.id());

    // That single run executes with the latest input.
    let out = h3.get_result().await?;
    assert_eq!(out, "c", "the debounced run used the latest input");
    assert_eq!(RUNS.load(Ordering::SeqCst), 1, "exactly one run");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
