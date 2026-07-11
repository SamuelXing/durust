//! The `DurableEngineBuilder` construction path: it builds a working engine,
//! seals registration (duplicate workflow *and* queue names — plus collisions
//! with the reserved internal names — are a build-time error), and
//! `DurableEngine::connect` scheme-dispatches a provider from a URL.

use durare::{
    DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, Result, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

/// The builder produces an engine that registers and runs a workflow, and its
/// config setters (app_version) take effect.
#[tokio::test]
async fn builder_builds_a_runnable_engine() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.app_version("9.9.9");
    b.register("add", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    let engine = b.build().await?;
    assert_eq!(engine.app_version(), "9.9.9");
    engine.launch().await?;

    let out: i64 = engine
        .start("add", 41_i64, WorkflowOptions::with_id("wf-b1"))
        .await?
        .result()
        .await?;
    assert_eq!(out, 42);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Registering the same name twice is a build-time `ConflictingRegistration`
/// (rather than a silent last-writer-wins overwrite).
#[tokio::test]
async fn builder_rejects_duplicate_names() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register("dup", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(1_i64)
    });
    b.register("dup", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(2_i64)
    });
    let Err(err) = b.build().await else {
        panic!("duplicate name must error");
    };
    assert_eq!(err.code(), ErrorCode::ConflictingRegistration);
    assert!(err.to_string().contains("dup"));
    Ok(())
}

/// A configured instance under the same workflow name but a different config
/// name is NOT a conflict (distinct registry keys); the same config twice IS.
#[tokio::test]
async fn builder_configured_instances_do_not_conflict() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register_configured("greet", "en", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>("hi".to_string())
    });
    b.register_configured("greet", "fr", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>("salut".to_string())
    });
    // Distinct config names → distinct keys → builds fine.
    let engine = b.build().await?;
    engine.shutdown(Duration::from_secs(1)).await?;

    // Same (name, config) twice → conflict.
    let provider = Arc::new(InMemoryProvider::new());
    let mut b2 = DurableEngine::builder(provider);
    b2.register_configured("greet", "en", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>("a".to_string())
    });
    b2.register_configured("greet", "en", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>("b".to_string())
    });
    let Err(err) = b2.build().await else {
        panic!("same config twice must error");
    };
    assert_eq!(err.code(), ErrorCode::ConflictingRegistration);
    Ok(())
}

/// Registering two queues under the same name is a build-time
/// `ConflictingRegistration`, not a silent last-writer-wins overwrite —
/// mirroring the guarantee already in place for workflows.
#[tokio::test]
async fn builder_rejects_duplicate_queue_names() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register_queue(WorkflowQueue::new("orders"));
    b.register_queue(WorkflowQueue::new("orders"));
    let Err(err) = b.build().await else {
        panic!("duplicate queue name must error");
    };
    assert_eq!(err.code(), ErrorCode::ConflictingRegistration);
    assert!(err.to_string().contains("orders"));
    Ok(())
}

/// A user queue that collides with the reserved internal queue name is
/// rejected — otherwise it would silently replace the queue that
/// resume/fork/debouncer route through.
#[tokio::test]
async fn builder_rejects_reserved_queue_name() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register_queue(WorkflowQueue::new("_dbos_internal_queue"));
    let Err(err) = b.build().await else {
        panic!("reserved queue name must error");
    };
    assert_eq!(err.code(), ErrorCode::ConflictingRegistration);
    Ok(())
}

/// A user workflow that collides with the reserved internal debouncer name is
/// rejected rather than silently shadowed by (or shadowing) the built-in.
#[tokio::test]
async fn builder_rejects_reserved_debouncer_workflow_name() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut b = DurableEngine::builder(provider);
    b.register(
        "_dbos_debouncer",
        |_ctx: DurableContext, _: ()| async move { Ok::<_, Error>(()) },
    );
    let Err(err) = b.build().await else {
        panic!("reserved workflow name must error");
    };
    assert_eq!(err.code(), ErrorCode::ConflictingRegistration);
    Ok(())
}

/// `connect` scheme-dispatches: `memory:` builds an in-memory engine; an
/// unknown scheme errors.
#[tokio::test]
async fn connect_dispatches_by_scheme() -> Result<()> {
    let mut b = DurableEngine::connect("memory:").await?;
    b.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    let engine = b.build().await?;
    engine.launch().await?;
    let () = engine
        .start("noop", (), WorkflowOptions::with_id("wf-c1"))
        .await?
        .result()
        .await?;
    engine.shutdown(Duration::from_secs(1)).await?;

    assert!(
        DurableEngine::connect("mysql://nope").await.is_err(),
        "an unrecognized scheme is rejected"
    );
    Ok(())
}
