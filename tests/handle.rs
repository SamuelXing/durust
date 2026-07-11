//! `WorkflowHandle` ergonomics: await it directly (`IntoFuture`), resolve it
//! through a shared `&self` (`result`), and `clone` it to observe the same
//! workflow from another task.

use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

/// A launched in-memory engine with a `quick` workflow that returns `n + 1`.
async fn engine_with_quick() -> Result<DurableEngine> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("quick", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    engine.launch().await?;
    Ok(engine)
}

/// `handle.await` resolves to the typed output (`IntoFuture`).
#[tokio::test]
async fn handle_awaits_directly() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 1_i64, WorkflowOptions::default())
        .await?;
    let out: i64 = handle.await?;
    assert_eq!(out, 2);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// `result` takes `&self`, so it needs no `mut` binding and can be called more
/// than once — the second call falls back to the persisted status after the
/// first consumes the in-process task.
#[tokio::test]
async fn result_takes_shared_ref_and_is_reusable() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 41_i64, WorkflowOptions::default())
        .await?;
    let first: i64 = handle.result().await?;
    let second: i64 = handle.result().await?;
    assert_eq!(first, 42);
    assert_eq!(second, 42);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A handle is `Clone` and `Send`: a clone can be awaited inside a spawned task
/// (which requires the future be `Send`) and observes the same result.
#[tokio::test]
async fn handle_clones_and_is_send_across_spawn() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 10_i64, WorkflowOptions::default())
        .await?;
    let observer = handle.clone();
    let spawned = tokio::spawn(async move { observer.await });

    let here: i64 = handle.await?;
    let there: i64 = spawned.await.expect("spawned task panicked")?;
    assert_eq!(here, 11);
    assert_eq!(there, 11);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
