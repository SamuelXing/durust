//! Config-name / class-instance routing: several handlers registered under one
//! workflow name, disambiguated by config name, are routed to correctly on both
//! direct runs and queue dispatch, and the names persist on the row.

use durust::{
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
        .run_workflow::<_, String>(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::default().config_name("en"),
        )
        .await?
        .get_result()
        .await?;
    let fr: String = engine
        .run_workflow::<_, String>(
            "greet",
            "Sam".to_string(),
            WorkflowOptions::default().config_name("fr"),
        )
        .await?
        .get_result()
        .await?;
    assert_eq!(en, "Hello, Sam");
    assert_eq!(fr, "Bonjour, Sam");

    // Queue dispatch (a claiming dispatcher must route by the persisted config).
    let mut qen: WorkflowHandle<String> = engine
        .enqueue(
            "q",
            "greet",
            "Q".to_string(),
            WorkflowOptions::default()
                .config_name("en")
                .class_name("Greeter"),
        )
        .await?;
    let mut qfr: WorkflowHandle<String> = engine
        .enqueue(
            "q",
            "greet",
            "Q".to_string(),
            WorkflowOptions::default().config_name("fr"),
        )
        .await?;
    assert_eq!(qen.get_result().await?, "Hello, Q");
    assert_eq!(qfr.get_result().await?, "Bonjour, Q");

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
        .run_workflow(
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
        .run_workflow::<_, i64>("plain", 41_i64, WorkflowOptions::default())
        .await?
        .get_result()
        .await?;
    assert_eq!(out, 42);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
