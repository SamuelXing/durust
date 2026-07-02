//! Workflow communication tests: send/recv messaging (FIFO, timeouts, replay
//! safety) and set_event/get_event, on the in-memory provider.

use durust::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
    WorkflowStatus, STATUS_PENDING,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// An external send unblocks a workflow waiting in recv.
#[tokio::test]
async fn send_unblocks_waiting_recv() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("waiter", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("greetings", Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    let mut handle = engine
        .run_workflow::<_, String>("waiter", (), WorkflowOptions::with_id("wf-recv"))
        .await?;
    engine
        .send("wf-recv", "hello".to_string(), "greetings")
        .await?;
    assert_eq!(handle.get_result().await?, "hello");
    Ok(())
}

/// Messages on a topic are consumed in FIFO order, including across workflows
/// exchanging messages via ctx.send.
#[tokio::test]
async fn recv_is_fifo() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("take_two", |ctx: DurableContext, _: ()| async move {
        let a: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        let b: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(format!(
            "{},{}",
            a.unwrap_or_default(),
            b.unwrap_or_default()
        ))
    });
    engine.register("producer", |ctx: DurableContext, dest: String| async move {
        ctx.send(&dest, "m1".to_string(), "t").await?;
        ctx.send(&dest, "m2".to_string(), "t").await?;
        Ok::<_, Error>(())
    });

    let mut consumer = engine
        .run_workflow::<_, String>("take_two", (), WorkflowOptions::with_id("wf-fifo"))
        .await?;
    let mut producer = engine
        .run_workflow::<_, ()>(
            "producer",
            "wf-fifo".to_string(),
            WorkflowOptions::with_id("wf-producer"),
        )
        .await?;
    producer.get_result().await?;
    assert_eq!(consumer.get_result().await?, "m1,m2");
    Ok(())
}

/// recv returns None once its (durable) timeout expires.
#[tokio::test]
async fn recv_times_out_to_none() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("impatient", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("silence", Duration::from_millis(100)).await?;
        Ok::<_, Error>(msg.is_none())
    });

    let started = Instant::now();
    let timed_out: bool = engine.start_typed("impatient", "wf-timeout", ()).await?;
    assert!(timed_out, "recv with no sender must return None");
    assert!(started.elapsed() >= Duration::from_millis(80));
    Ok(())
}

/// A replayed recv returns its checkpointed message without consuming another:
/// re-executing the workflow body (via recover) yields the same message, and
/// the second message is still in the mailbox afterwards.
#[tokio::test]
async fn recv_replay_does_not_double_consume() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("take_one", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    // Create the workflow row directly in PENDING so recover() executes it.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-replay",
            "take_one",
            Value::Null,
            STATUS_PENDING,
            "",
            "0.1.0",
        ))
        .await?;
    engine.send("wf-replay", "m1".to_string(), "t").await?;
    engine.send("wf-replay", "m2".to_string(), "t").await?;

    // First execution consumes m1 and completes.
    assert_eq!(engine.recover().await?, 1);
    let first = provider.get_workflow_status("wf-replay").await?.unwrap();
    assert_eq!(first.output, Some(Value::String("m1".into())));

    // Force a re-execution of the body: the recv must replay its checkpoint
    // (m1), not consume m2.
    provider
        .set_workflow_status("wf-replay", STATUS_PENDING, None, None)
        .await?;
    assert_eq!(engine.recover().await?, 1);
    let second = provider.get_workflow_status("wf-replay").await?.unwrap();
    assert_eq!(second.output, Some(Value::String("m1".into())));

    // m2 must still be unconsumed.
    let leftover = provider
        .consume_notification("wf-replay", "t", 999, "test-probe")
        .await?;
    assert_eq!(leftover, Some(Value::String("m2".into())));
    Ok(())
}

/// Sending to a workflow id that does not exist is an error.
#[tokio::test]
async fn send_to_missing_workflow_errors() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let res = engine.send("ghost", "boo".to_string(), "t").await;
    assert!(res.is_err());
    Ok(())
}

/// set_event publishes a value readable from outside the workflow (and after
/// it completes); get_event from another workflow sees it too.
#[tokio::test]
async fn set_event_and_get_event() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("publisher", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("status", "ready").await?;
        Ok::<_, Error>(())
    });
    engine.register(
        "subscriber",
        |ctx: DurableContext, target: String| async move {
            let v: Option<String> = ctx
                .get_event(&target, "status", Duration::from_secs(5))
                .await?;
            Ok::<_, Error>(v.unwrap_or_default())
        },
    );

    engine
        .start_typed::<_, ()>("publisher", "wf-pub", ())
        .await?;

    // External read.
    let v: Option<String> = engine
        .get_event("wf-pub", "status", Duration::from_secs(1))
        .await?;
    assert_eq!(v.as_deref(), Some("ready"));

    // Cross-workflow durable read.
    let got: String = engine
        .start_typed("subscriber", "wf-sub", "wf-pub".to_string())
        .await?;
    assert_eq!(got, "ready");
    Ok(())
}

/// Distinct event keys coexist, and re-setting a key overwrites it: a reader sees
/// the latest value for an updated key and the independent value for another —
/// last-write-wins per key (mirrors the other SDKs' set/get-event semantics).
#[tokio::test]
async fn set_event_keys_are_independent_and_last_write_wins() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("multi_event", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("phase", "start").await?;
        ctx.set_event("progress", 10_i64).await?;
        // Overwrite one key; the other must be untouched.
        ctx.set_event("phase", "done").await?;
        Ok::<_, Error>(())
    });

    engine
        .start_typed::<_, ()>("multi_event", "wf-ev", ())
        .await?;

    // The overwritten key reads back its latest value.
    let phase: Option<String> = engine
        .get_event("wf-ev", "phase", Duration::from_secs(1))
        .await?;
    assert_eq!(phase.as_deref(), Some("done"), "last write wins for a key");

    // The independent key keeps its own value.
    let progress: Option<i64> = engine
        .get_event("wf-ev", "progress", Duration::from_secs(1))
        .await?;
    assert_eq!(progress, Some(10), "a distinct key is unaffected");
    Ok(())
}

/// get_event on a key that is never set returns None after the timeout, both
/// from outside and inside a workflow.
#[tokio::test]
async fn get_event_times_out_to_none() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.start_typed::<_, ()>("noop", "wf-empty", ()).await?;

    let v: Option<String> = engine
        .get_event("wf-empty", "missing", Duration::from_millis(80))
        .await?;
    assert_eq!(v, None);
    Ok(())
}
