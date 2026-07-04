//! Workflow debouncing: rapid repeated triggers grouped by a key coalesce into
//! a single delayed run with the latest input.

use durust::{
    Client, DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowHandle,
    WorkflowOptions,
};
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

/// An out-of-process `Client` debounces the same way an engine does: it only
/// enqueues the collector and pushes inputs, while a launched engine runs the
/// coalesced target. Three rapid `Client` calls collapse to one run with the
/// latest input, all handles pointing at the same run.
#[tokio::test]
async fn debounce_from_client_coalesces_to_latest_input() -> Result<()> {
    static RUNS: AtomicUsize = AtomicUsize::new(0);
    // Engine and client share one provider; the engine runs the collector/target.
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("notify", |_ctx: DurableContext, msg: String| async move {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(msg)
    });
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let delay = Duration::from_millis(250);
    let h1: WorkflowHandle<String> = client
        .debouncer("notify")
        .debounce("k", delay, "a".to_string())
        .await?;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let h2: WorkflowHandle<String> = client
        .debouncer("notify")
        .debounce("k", delay, "b".to_string())
        .await?;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut h3: WorkflowHandle<String> = client
        .debouncer("notify")
        .debounce("k", delay, "c".to_string())
        .await?;

    assert_eq!(h1.id(), h2.id());
    assert_eq!(h2.id(), h3.id());

    let out = h3.get_result().await?;
    assert_eq!(out, "c", "the debounced run used the latest input");
    assert_eq!(RUNS.load(Ordering::SeqCst), 1, "exactly one run");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// The debounced target runs with the caller's `WorkflowOptions` — its
/// authenticated identity and application version — not just an id.
#[tokio::test]
async fn debounce_threads_target_options() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("notify", |ctx: DurableContext, msg: String| async move {
        // Echo the identity the target sees, proving auth threaded through.
        Ok::<_, Error>(format!(
            "{}:{}",
            ctx.authenticated_user().unwrap_or(""),
            msg
        ))
    });
    engine.launch().await?;

    let opts = WorkflowOptions::default()
        .authenticated_user("alice")
        .app_version("9.9.9");
    let mut h: WorkflowHandle<String> = engine
        .debouncer("notify")
        .options(opts)
        .debounce("k", Duration::from_millis(50), "x".to_string())
        .await?;
    let out = h.get_result().await?;
    assert_eq!(out, "alice:x", "the target ran under the threaded identity");

    // The target's persisted row carries the threaded version + auth.
    let status = engine
        .retrieve_workflow::<String>(h.id())
        .await?
        .get_status()
        .await?;
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(status.app_version, "9.9.9");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
