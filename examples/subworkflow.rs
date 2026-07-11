//! Child workflows: fan a job out into independently-durable sub-workflows.
//!
//! `ctx.start_workflow(...)` launches another workflow from inside this one and
//! hands back a handle. Each child is a *first-class* durable workflow — its own
//! checkpoints, its own recovery, its own row — linked back to the parent and
//! given a deterministic id (`{parent}-{n}`), so a crash re-attaches to the same
//! children instead of spawning new ones. Start them without awaiting to run the
//! fan-out concurrently, then reduce over their results.
//!
//! Here a map-reduce splits a list into chunks, sums each chunk in a child, and
//! adds up the partials in the parent.
//!
//! ```text
//! cargo run --example subworkflow
//! ```

use durare::{
    DurableContext, DurableEngine, InMemoryProvider, ListFilter, Result, WorkflowOptions,
};
use std::sync::Arc;
use std::time::Duration;

#[durare::step]
async fn sum_step(ctx: &DurableContext, nums: Vec<i64>) -> Result<i64> {
    tokio::time::sleep(Duration::from_millis(60)).await; // pretend the chunk is real work
    Ok(nums.iter().sum())
}

// The child: sums one chunk. An ordinary workflow — nothing marks it as a child.
#[durare::workflow]
async fn sum_chunk(ctx: DurableContext, nums: Vec<i64>) -> Result<i64> {
    sum_step(&ctx, nums).await
}

// The parent: fan out one child per chunk, then reduce their partial sums.
#[durare::workflow]
async fn map_reduce(ctx: DurableContext, data: Vec<i64>) -> Result<i64> {
    // Map: start a child per chunk, non-blocking, so they run concurrently.
    let mut children = Vec::new();
    for chunk in data.chunks(3) {
        let child = ctx
            .start_workflow::<_, i64>("sum_chunk", chunk.to_vec(), WorkflowOptions::default())
            .await?;
        children.push(child);
    }

    // Reduce: await each child's partial sum.
    let mut total = 0;
    for child in children {
        total += child.result().await?;
    }
    Ok(total)
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    let data: Vec<i64> = (1..=10).collect();
    println!("[start] map_reduce over {data:?}");
    let total: i64 = engine
        .start_with(MapReduce, data, WorkflowOptions::with_id("mapreduce-1"))
        .await?
        .await?;
    println!("[done] total = {total}\n");
    assert_eq!(total, 55);

    // Each chunk ran as its own durable workflow, linked back to the parent.
    let mut children = engine
        .list_workflows(&ListFilter {
            workflow_id_prefix: vec!["mapreduce-1-".to_string()],
            ..Default::default()
        })
        .await?;
    children.sort_by(|a, b| a.id.cmp(&b.id));
    println!("[children] {} durable sub-workflows:", children.len());
    for c in &children {
        println!(
            "  {} -> {:?}  (parent = {})",
            c.id,
            c.output.as_ref().unwrap(),
            c.parent_workflow_id.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}
