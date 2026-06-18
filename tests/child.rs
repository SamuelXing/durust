//! Child-workflow tests on the in-memory provider: a workflow starts another
//! workflow, awaits its result, and the child is linked back to its parent and
//! inherits the parent's identity.

use durust::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions, STATUS_SUCCESS,
};
use std::sync::Arc;

/// A parent workflow starts a child, awaits it, and the child row carries the
/// deterministic id, the `parent_workflow_id` link, and its own result.
#[tokio::test]
async fn child_workflow_runs_and_links_to_parent() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("triple", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * 3)
    });
    engine.register("parent", |ctx: DurableContext, n: i64| async move {
        let mut child = ctx
            .start_workflow::<_, i64>("triple", n, WorkflowOptions::default())
            .await?;
        Ok::<_, Error>(child.get_result().await? + 1)
    });

    let out: i64 = engine.start_typed("parent", "p1", 7_i64).await?;
    assert_eq!(out, 22); // (7 * 3) + 1

    // The child got the deterministic id `{parent}-{seq}` and points back home.
    let child = engine.retrieve_workflow::<i64>("p1-0").await?;
    let status = child.get_status().await?;
    assert_eq!(status.parent_workflow_id.as_deref(), Some("p1"));
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(status.output, Some(serde_json::json!(21)));
    Ok(())
}

/// A child workflow inherits the parent's authenticated identity.
#[tokio::test]
async fn child_workflow_inherits_identity() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("whoami", |ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(ctx.authenticated_user().unwrap_or("-").to_string())
    });
    engine.register("delegator", |ctx: DurableContext, _: ()| async move {
        let mut child = ctx
            .start_workflow::<_, String>("whoami", (), WorkflowOptions::default())
            .await?;
        child.get_result().await
    });

    let opts = WorkflowOptions::with_id("boss").authenticated_user("alice");
    let mut handle = engine
        .run_workflow::<_, String>("delegator", (), opts)
        .await?;
    assert_eq!(handle.get_result().await?, "alice");

    // The identity is also persisted on the child row.
    let child = engine.retrieve_workflow::<String>("boss-0").await?;
    assert_eq!(
        child.get_status().await?.authenticated_user.as_deref(),
        Some("alice")
    );
    Ok(())
}

/// `get_workflow_steps` on the in-memory backend reports steps and the child
/// invocation with names, outputs, and the child link, ordered by step id.
#[tokio::test]
async fn workflow_steps_introspection_in_memory() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("kid", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register("worker", |ctx: DurableContext, _: ()| async move {
        let v = ctx
            .step("compute", || async { Ok::<_, Error>(7_i64) })
            .await?;
        let mut child = ctx
            .start_workflow::<_, i64>("kid", v, WorkflowOptions::default())
            .await?;
        child.get_result().await
    });

    engine.start_typed::<_, i64>("worker", "w", ()).await?;

    let steps = engine.get_workflow_steps("w").await?;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].name, "compute");
    assert_eq!(steps[0].output, Some(serde_json::json!(7)));
    assert_eq!(steps[1].name, "kid");
    assert_eq!(steps[1].child_workflow_id.as_deref(), Some("w-1"));

    // The durable step records start/finish timing (start no later than finish);
    // the child-invocation marker has no step timing.
    let compute = &steps[0];
    let (start, end) = (
        compute.started_at.expect("step records started_at"),
        compute.completed_at.expect("step records completed_at"),
    );
    assert!(start <= end);
    assert!(
        steps[1].started_at.is_none() && steps[1].completed_at.is_none(),
        "a child-invocation marker carries no step timing"
    );
    Ok(())
}

/// A child can itself be routed through a queue: the parent enqueues it and a
/// dispatcher runs it, while the parent awaits the result by polling.
#[tokio::test]
async fn child_workflow_can_be_queued() -> Result<()> {
    use durust::WorkflowQueue;
    use std::time::Duration;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("square", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n * n)
    });
    engine.register("fan_out", |ctx: DurableContext, n: i64| async move {
        let opts = WorkflowOptions {
            queue: Some("kids".to_string()),
            ..Default::default()
        };
        let mut child = ctx.start_workflow::<_, i64>("square", n, opts).await?;
        child.get_result().await
    });
    engine.register_queue(
        WorkflowQueue::new("kids").base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;

    let out: i64 = engine.start_typed("fan_out", "fo1", 9_i64).await?;
    assert_eq!(out, 81);

    let child = engine.retrieve_workflow::<i64>("fo1-0").await?;
    assert_eq!(
        child.get_status().await?.queue_name.as_deref(),
        Some("kids")
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
