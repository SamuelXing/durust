//! Code-patching tests on the in-memory provider: `patch` routes new workflows
//! to new code and pre-patch workflows to old code, recording a marker step.

use durust::{
    DurableContext, DurableEngine, InMemoryProvider, Result, StateProvider, WorkflowOptions,
};
use serde_json::Value;
use std::sync::Arc;

/// A workflow reaching a patch point for the first time takes the new path and
/// records the marker as its own step.
#[tokio::test]
async fn patch_new_workflow_takes_new_path() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("wf", |ctx: DurableContext, _: ()| async move {
        ctx.patch("feature").await
    });

    let patched: bool = engine.start_typed("wf", "fresh", ()).await?;
    assert!(patched, "a brand-new workflow uses the patched code");

    // The marker was recorded as the workflow's first operation.
    let steps = engine.get_workflow_steps("fresh").await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_id, 0);
    assert_eq!(steps[0].name, "DBOS.patch-feature");
    Ok(())
}

/// A workflow that already executed past this point before the patch existed
/// (a different step occupies the slot) takes the old path.
#[tokio::test]
async fn patch_pre_patch_workflow_takes_old_path() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    // Seed a non-patch step at seq 0, as if the pre-patch code had run it there.
    provider
        .record_step_result("old", 0, "legacy_step", Value::from(1), None, None)
        .await?;

    let mut engine = DurableEngine::new(provider).await?;
    engine.register("wf", |ctx: DurableContext, _: ()| async move {
        ctx.patch("feature").await
    });

    let patched: bool = engine
        .run_workflow::<_, bool>("wf", (), WorkflowOptions::with_id("old"))
        .await?
        .get_result()
        .await?;
    assert!(
        !patched,
        "a workflow past this point pre-patch uses the old code"
    );
    Ok(())
}
