//! `#[durare::step]`: an async fn whose body becomes a durable checkpoint,
//! callable like any async fn — no closure, no `Box::pin`, no `Ok::<_, Error>`.

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

// A leaf step: the body is plain async, and `Ok(..)` needs no error annotation
// because the fn's `-> Result<i64>` fixes it.
#[durare::step]
async fn add_one(ctx: &DurableContext, n: i64) -> Result<i64> {
    Ok(n + 1)
}

// The registered step name can be overridden independently of the fn name.
#[durare::step(name = "shout")]
async fn to_upper(ctx: &DurableContext, s: String) -> Result<String> {
    Ok(s.to_uppercase())
}

#[durare::workflow]
async fn pipeline(ctx: DurableContext, start: i64) -> Result<String> {
    let a = add_one(&ctx, start).await?;
    let b = add_one(&ctx, a).await?;
    to_upper(&ctx, format!("n{b}")).await
}

/// The macro'd steps run and each records a checkpoint under its (overridable)
/// name, so the workflow is replay-safe without any hand-written `ctx.step`.
#[tokio::test]
async fn step_macro_checkpoints_under_its_name() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.launch().await?;

    let handle = engine
        .start_with(Pipeline, 10_i64, WorkflowOptions::default())
        .await?;
    let id = handle.id().to_string();
    let out: String = handle.await?;
    assert_eq!(out, "N12");

    let steps = engine.get_workflow_steps(&id).await?;
    assert_eq!(
        steps.iter().filter(|s| s.name == "add_one").count(),
        2,
        "both add_one calls checkpointed under the fn name"
    );
    assert_eq!(
        steps.iter().filter(|s| s.name == "shout").count(),
        1,
        "the #[step(name = ...)] override applies"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
