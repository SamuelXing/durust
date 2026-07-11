//! Config-name / class-instance routing: several handlers registered under one
//! workflow name, disambiguated by config name, are routed to correctly on both
//! direct runs and queue dispatch, and the names persist on the row.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowHandle,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

/// Register two instances of `greet` (config `en` / `fr`); a run routes to the
/// instance named by `WorkflowOptions::config_name`, on both a direct run and a
/// queue dispatch, and the config name is persisted on the row.
#[tokio::test]
async fn config_name_routes_to_matching_instance() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register_configured(
        "greet",
        "en",
        |_ctx: DurableContext, who: String| async move { Ok::<_, Error>(format!("Hello, {who}")) },
    );
    engine.register_configured("greet", "fr", |_ctx: DurableContext, who: String| async move {
        Ok::<_, Error>(format!("Bonjour, {who}"))
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    // Direct runs (the inline dispatch path).
    let en: String = engine
        .start::<_, String>(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::default().config_name("en"),
        )
        .await?
        .result()
        .await?;
    let fr: String = engine
        .start::<_, String>(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::default().config_name("fr"),
        )
        .await?
        .result()
        .await?;
    assert_eq!(en, "Hello, Sam");
    assert_eq!(fr, "Bonjour, Sam");

    // Queue dispatch (a claiming dispatcher must route by the persisted config).
    let qen: WorkflowHandle<String> = engine
        .start(
            "greet",
            "Q".to_string(),
            WorkflowOptions::default()
                .config_name("en")
                .class_name("Greeter")
                .queue("q"),
        )
        .await?;
    let qfr: WorkflowHandle<String> = engine
        .start(
            "greet",
            "Q".to_string(),
            WorkflowOptions::default().config_name("fr").queue("q"),
        )
        .await?;
    assert_eq!(qen.result().await?, "Hello, Q");
    assert_eq!(qfr.result().await?, "Bonjour, Q");

    // The config/class names round-trip on the persisted row.
    let status = engine
        .retrieve_workflow::<String>(qen.id())
        .await?
        .get_status()
        .await?;
    assert_eq!(status.config_name.as_deref(), Some("en"));
    assert_eq!(status.class_name.as_deref(), Some("Greeter"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A run whose `config_name` matches no registered instance fails with
/// `UnknownWorkflow` — it does not silently fall back to the plain-name handler.
#[tokio::test]
async fn unknown_config_name_is_an_error() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register_configured(
        "greet",
        "en",
        |_ctx: DurableContext, who: String| async move { Ok::<_, Error>(format!("Hello, {who}")) },
    );
    engine.launch().await?;

    let res: Result<WorkflowHandle<String>> = engine
        .start(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::default().config_name("de"),
        )
        .await;
    assert!(
        matches!(res, Err(Error::UnknownWorkflow(_))),
        "an unregistered config name is not routed to the plain-name handler"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A plain (unconfigured) registration is keyed by name alone, so existing
/// no-config workflows keep working alongside configured instances of other
/// names.
#[tokio::test]
async fn plain_registration_unaffected_by_config_routing() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("plain", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    engine.launch().await?;

    let out: i64 = engine
        .start::<_, i64>("plain", 41_i64, WorkflowOptions::default())
        .await?
        .result()
        .await?;
    assert_eq!(out, 42);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A fork of a configured run copies `config_name`/`class_name`, so the
/// claiming dispatcher routes the fork to the same instance instead of failing
/// with `UnknownWorkflow`.
#[tokio::test]
async fn fork_of_configured_run_routes_to_same_instance() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register_configured(
        "greet",
        "en",
        |_ctx: DurableContext, who: String| async move { Ok::<_, Error>(format!("Hello, {who}")) },
    );
    engine.register_configured("greet", "fr", |_ctx: DurableContext, who: String| async move {
        Ok::<_, Error>(format!("Bonjour, {who}"))
    });
    engine.launch().await?;

    let fr: String = engine
        .start::<_, String>(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::with_id("wf-fr")
                .config_name("fr")
                .class_name("Greeter"),
        )
        .await?
        .result()
        .await?;
    assert_eq!(fr, "Bonjour, Sam");

    let forked = engine
        .fork_workflow::<String>("wf-fr", 0, WorkflowOptions::with_id("wf-fr-fork"))
        .await?;
    assert_eq!(forked.result().await?, "Bonjour, Sam");

    let status = engine
        .retrieve_workflow::<String>("wf-fr-fork")
        .await?
        .get_status()
        .await?;
    assert_eq!(status.config_name.as_deref(), Some("fr"));
    assert_eq!(status.class_name.as_deref(), Some("Greeter"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
