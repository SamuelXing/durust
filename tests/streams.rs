//! Durable stream tests on the in-memory provider: a workflow writes an
//! append-only stream that an external reader drains in order, observing the
//! close (or the producer going inactive).

use durust::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

/// A producer writes three values and closes the stream; the reader drains them
/// in order and sees `closed`. Each write/close is recorded as its own step.
#[tokio::test]
async fn write_close_then_read_in_order() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("producer", |ctx: DurableContext, _: ()| async move {
        for i in 0..3_i64 {
            ctx.write_stream("nums", i).await?;
        }
        ctx.close_stream("nums").await?;
        Ok::<_, Error>(())
    });

    engine
        .run_workflow::<_, ()>("producer", (), WorkflowOptions::with_id("p"))
        .await?
        .get_result()
        .await?;

    let (values, closed): (Vec<i64>, bool) = engine.read_stream("p", "nums").await?;
    assert_eq!(values, vec![0, 1, 2]);
    assert!(closed);

    // The three writes and the close each landed as their own step.
    let steps = engine.get_workflow_steps("p").await?;
    let names: Vec<&str> = steps.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "DBOS.writeStream",
            "DBOS.writeStream",
            "DBOS.writeStream",
            "DBOS.closeStream",
        ]
    );
    Ok(())
}

/// A reader blocked on `read_stream` returns once the producer finishes, even if
/// the stream was never explicitly closed: an inactive producer can write no
/// more.
#[tokio::test]
async fn read_stops_when_producer_finishes_without_close() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("producer", |ctx: DurableContext, _: ()| async move {
        ctx.write_stream("s", "a".to_string()).await?;
        ctx.write_stream("s", "b".to_string()).await?;
        Ok::<_, Error>(())
    });

    engine
        .run_workflow::<_, ()>("producer", (), WorkflowOptions::with_id("p2"))
        .await?
        .get_result()
        .await?;

    let (values, closed): (Vec<String>, bool) = engine.read_stream("p2", "s").await?;
    assert_eq!(values, vec!["a".to_string(), "b".to_string()]);
    assert!(closed, "an inactive producer ends the stream");
    Ok(())
}

/// A snapshot read returns immediately with whatever is available from an
/// offset, without waiting for the stream to close.
#[tokio::test]
async fn snapshot_reads_available_from_offset() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("producer", |ctx: DurableContext, _: ()| async move {
        for i in 0..3_i64 {
            ctx.write_stream("nums", i).await?;
        }
        ctx.close_stream("nums").await?;
        Ok::<_, Error>(())
    });
    engine
        .run_workflow::<_, ()>("producer", (), WorkflowOptions::with_id("p3"))
        .await?
        .get_result()
        .await?;

    let (head, closed): (Vec<i64>, bool) = engine.read_stream_snapshot("p3", "nums", 0).await?;
    assert_eq!(head, vec![0, 1, 2]);
    assert!(closed);

    let (tail, closed): (Vec<i64>, bool) = engine.read_stream_snapshot("p3", "nums", 2).await?;
    assert_eq!(tail, vec![2]);
    assert!(closed);
    Ok(())
}

/// Writing to a closed stream is an error, surfaced as a workflow failure.
#[tokio::test]
async fn writing_to_closed_stream_errors() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("bad", |ctx: DurableContext, _: ()| async move {
        ctx.close_stream("s").await?;
        ctx.write_stream("s", 1_i64).await?;
        Ok::<_, Error>(())
    });

    let res = engine
        .run_workflow::<_, ()>("bad", (), WorkflowOptions::with_id("p4"))
        .await?
        .get_result()
        .await;
    assert!(res.is_err(), "writing after close must fail");
    Ok(())
}

/// `read_stream` drains values as the producer writes them: the reader starts
/// while the producer is still running and blocks until the close arrives.
#[tokio::test]
async fn read_drains_while_producer_runs() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("slow_producer", |ctx: DurableContext, _: ()| async move {
        ctx.write_stream("s", 1_i64).await?;
        ctx.sleep(Duration::from_millis(50)).await?;
        ctx.write_stream("s", 2_i64).await?;
        ctx.close_stream("s").await?;
        Ok::<_, Error>(())
    });

    // Start the producer in the background, then block draining its stream.
    let mut producer = engine
        .run_workflow::<_, ()>("slow_producer", (), WorkflowOptions::with_id("p5"))
        .await?;
    let (values, closed): (Vec<i64>, bool) = engine.read_stream("p5", "s").await?;
    assert_eq!(values, vec![1, 2]);
    assert!(closed);
    producer.get_result().await?;
    Ok(())
}
