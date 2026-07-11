//! Durable concurrency on the in-memory provider: fan-out via `try_join!` over
//! `ctx.step` is already durable (each step checkpoints independently), and
//! `ctx.select` durably races branches and returns the first to complete.

use durust::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

/// Two steps run concurrently with `try_join!`; both are checkpointed, in poll
/// order. No special API — plain futures composition over `ctx.step`.
#[tokio::test]
async fn concurrent_steps_via_try_join() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("fanout", |ctx: DurableContext, _: ()| async move {
        let (a, b) = tokio::try_join!(
            ctx.step("a", || async { Ok::<_, Error>(10_i64) }),
            ctx.step("b", || async { Ok::<_, Error>(32_i64) }),
        )?;
        Ok::<_, Error>(a + b)
    });

    let out: i64 = engine
        .start("fanout", (), WorkflowOptions::with_id("f"))
        .await?
        .result()
        .await?;
    assert_eq!(out, 42);

    // Both branches were recorded as steps, numbered by poll order.
    let steps = engine.get_workflow_steps("f").await?;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].name, "a");
    assert_eq!(steps[1].name, "b");
    Ok(())
}

/// `select` returns the index and value of the first branch to complete and
/// records the outcome as a single `DBOS.select` step.
#[tokio::test]
async fn select_returns_first_to_complete() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("racer", |ctx: DurableContext, _: ()| async move {
        let branches: Vec<Pin<Box<dyn Future<Output = i64> + Send>>> = vec![
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                1
            }),
            Box::pin(async { 2 }),
        ];
        ctx.select(branches).await
    });

    let (index, value): (usize, i64) = engine
        .start("racer", (), WorkflowOptions::with_id("r"))
        .await?
        .result()
        .await?;
    assert_eq!((index, value), (1, 2), "the immediately-ready branch wins");

    let steps = engine.get_workflow_steps("r").await?;
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].name, "DBOS.select");
    Ok(())
}

/// Many independent workflows run concurrently and each completes with its own
/// correct result, its single step running exactly once — an isolation/scale
/// check that ids, checkpoints, and outputs never cross-contaminate under load.
#[tokio::test]
async fn many_concurrent_workflows_stay_isolated() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("square", |ctx: DurableContext, n: i64| async move {
        // A checkpointed step, so each workflow records its own operation output.
        let r = ctx
            .step("sq", move || async move { Ok::<_, Error>(n * n) })
            .await?;
        Ok::<_, Error>(r)
    });
    let engine = Arc::new(engine);

    const N: i64 = 200;
    let mut handles = Vec::new();
    for i in 0..N {
        let e = engine.clone();
        handles.push(tokio::spawn(async move {
            let out: i64 = e
                .start("square", i, WorkflowOptions::with_id(format!("wf-{i}")))
                .await?
                .result()
                .await?;
            Ok::<_, Error>((i, out))
        }));
    }
    for h in handles {
        let (i, out) = h.await.expect("workflow task panicked")?;
        assert_eq!(out, i * i, "workflow {i} returned the wrong result");
    }

    // Each workflow recorded exactly its own one step (a spot check).
    for id in ["wf-0", "wf-7", "wf-199"] {
        assert_eq!(engine.get_workflow_steps(id).await?.len(), 1);
    }
    Ok(())
}

/// `select` over no branches is a programming error, surfaced as a failure.
#[tokio::test]
async fn select_with_no_branches_errors() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("empty", |ctx: DurableContext, _: ()| async move {
        let branches: Vec<Pin<Box<dyn Future<Output = i64> + Send>>> = vec![];
        ctx.select(branches).await
    });

    let res = engine
        .start::<_, (usize, i64)>("empty", (), WorkflowOptions::with_id("e"))
        .await?
        .result()
        .await;
    assert!(res.is_err(), "select with no branches must fail");
    Ok(())
}
