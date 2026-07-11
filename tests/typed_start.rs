//! Typed workflow references. `#[durare::workflow]` emits an `UpperCamelCase`
//! marker whose `WorkflowDef` impl carries the input/output types, so
//! `engine.start_with(Marker, input, opts)` is checked without a turbofish.

use durare::{
    DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowDef, WorkflowOptions,
};
use std::sync::Arc;
use std::time::Duration;

#[durare::workflow]
async fn double(_ctx: DurableContext, n: i64) -> Result<i64> {
    Ok(n * 2)
}

#[durare::workflow]
async fn greet(_ctx: DurableContext, name: String) -> Result<String> {
    Ok(format!("hello, {name}"))
}

/// The macro-emitted marker carries the registered name and drives `start_with`,
/// which fixes the input type and infers the output (no `::<In, Out>`).
#[tokio::test]
async fn start_with_runs_typed_ref() -> Result<()> {
    // The marker is `UpperCamelCase` of the fn; `NAME` is the registered name.
    assert_eq!(Double::NAME, "double");
    assert_eq!(Greet::NAME, "greet");

    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.launch().await?;

    // Input `i64` is type-checked against the marker; output `i64` is inferred.
    let handle = engine
        .start_with(Double, 21_i64, WorkflowOptions::default())
        .await?;
    let doubled: i64 = handle.await?;
    assert_eq!(doubled, 42);

    // A second marker with different I/O types resolves independently.
    let greeting: String = engine
        .start_with(Greet, "sam".to_string(), WorkflowOptions::default())
        .await?
        .await?;
    assert_eq!(greeting, "hello, sam");

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
