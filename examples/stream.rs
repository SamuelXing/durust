//! Durable streams: a workflow publishes an append-only feed a reader tails live.
//!
//! Unlike an event (`set_event` — a single, last-write-wins value), a stream is
//! an ordered sequence: `ctx.write_stream(key, v)` appends, `ctx.close_stream`
//! seals it. Each append is a checkpoint, so the feed is durable and replay-safe.
//! A consumer drains it with `engine.read_stream_values(id, key)` — a live
//! `Stream` that yields values as they are written and ends when the producer
//! closes it (or goes inactive). Good for progress bars, log tails, or streaming
//! LLM tokens out of a long workflow.
//!
//! ```text
//! cargo run --example stream
//! ```

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, StreamExt, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

#[durare::workflow]
async fn crunch(ctx: DurableContext, items: i64) -> Result<i64> {
    for i in 1..=items {
        // Publish progress into the durable stream as we go.
        ctx.write_stream("progress", format!("processed {i}/{items}"))
            .await?;
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    ctx.close_stream("progress").await?;
    Ok(items)
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // Start the producer without awaiting it — it runs while we tail its stream.
    let handle = engine
        .start_with(Crunch, 5i64, WorkflowOptions::with_id("job-1"))
        .await?;

    // Tail the feed live: each value arrives as the workflow writes it.
    println!("[reader] tailing job-1/progress:");
    let mut feed = engine.read_stream_values::<String>("job-1", "progress");
    while let Some(item) = feed.next().await {
        match item {
            Ok(msg) => println!("  << {msg}"),
            Err(e) => {
                eprintln!("  stream error: {e}");
                break;
            }
        }
    }

    // The producer has closed the stream; collect its return value.
    let processed = handle.await?;
    println!("[done] workflow processed {processed} items");
    Ok(())
}
