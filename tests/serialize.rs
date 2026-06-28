//! Serialization-format parity: values written under one format are decoded
//! correctly by a provider configured for another (dispatch-on-read), which is
//! the basis for cross-language interop via `portable_json`.

use durust::{
    DurableContext, DurableEngine, Error, Result, Serializer, SqliteProvider, WorkflowOptions,
};
use std::sync::Arc;

fn temp_db_url(tag: &str) -> (String, std::path::PathBuf) {
    let mut p = std::env::temp_dir();
    p.push(format!("durust-ser-{tag}-{}.db", uuid::Uuid::new_v4()));
    (format!("sqlite://{}", p.display()), p)
}

async fn engine_with(url: &str, fmt: Serializer) -> Result<DurableEngine> {
    let provider = SqliteProvider::connect(url).await?.with_serializer(fmt);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    // A workflow whose input flows through a step into the output, so input,
    // step output, and workflow output all exercise the serializer.
    engine.register("greet", |ctx: DurableContext, name: String| async move {
        let msg = ctx
            .step("build", || async { Ok::<_, Error>(format!("hi {name}")) })
            .await?;
        Ok::<_, Error>(msg)
    });
    Ok(engine)
}

/// A workflow written by a `portable_json` provider is fully readable by a
/// default (`DBOS_JSON`) provider, and vice versa — proving decode dispatches on
/// the stored format, not the reader's configured one.
async fn cross_format(writer: Serializer, reader: Serializer, tag: &str) -> Result<()> {
    let (url, path) = temp_db_url(tag);

    // Writer process: run to completion under `writer`'s format.
    {
        let engine = engine_with(&url, writer).await?;
        let out: String = engine
            .run_workflow::<_, String>(
                "greet",
                "ada".to_string(),
                WorkflowOptions::with_id("wf-ser"),
            )
            .await?
            .get_result()
            .await?;
        assert_eq!(out, "hi ada");
    }

    // Reader process: a fresh engine whose provider encodes in a *different*
    // format must still decode the persisted input, step output, and result.
    {
        let engine = engine_with(&url, reader).await?;
        let mut handle = engine.retrieve_workflow::<String>("wf-ser").await?;
        let status = handle.get_status().await?;
        assert_eq!(status.input, serde_json::json!("ada"));
        assert_eq!(status.output, Some(serde_json::json!("hi ada")));
        assert_eq!(handle.get_result().await?, "hi ada");
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// A workflow that fails under `portable_json` stores its error as the
/// cross-language envelope `{"name","message"}`, so a reader recovers both the
/// human message and the structured error — the shape any SDK can parse. The
/// structured `error_info` can only be present if the envelope (not a bare
/// string) was actually persisted.
#[tokio::test]
async fn portable_error_is_stored_as_envelope() -> Result<()> {
    let (url, path) = temp_db_url("err-p");
    let provider = SqliteProvider::connect(&url)
        .await?
        .with_serializer(Serializer::Portable);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("boom", |_ctx: DurableContext, _: ()| async move {
        Err::<(), _>(Error::app("kaboom"))
    });

    let outcome = engine
        .run_workflow::<_, ()>("boom", (), WorkflowOptions::with_id("wf-err"))
        .await?
        .get_result()
        .await;
    assert!(outcome.is_err());

    let handle = engine.retrieve_workflow::<()>("wf-err").await?;
    let status = handle.get_status().await?;
    assert_eq!(status.status, "ERROR");
    assert_eq!(status.error.as_deref(), Some("kaboom"));
    let info = status
        .error_info
        .expect("a portable error carries structured info");
    assert_eq!(info.name, "Portable Error");
    assert_eq!(info.message, "kaboom");
    assert!(info.code.is_none() && info.data.is_none());

    let _ = std::fs::remove_file(path);
    Ok(())
}

/// In the default (`DBOS_JSON`) format the error stays a bare string with no
/// structured envelope — unchanged from before the portable envelope landed.
#[tokio::test]
async fn default_error_stays_bare() -> Result<()> {
    let (url, path) = temp_db_url("err-d");
    let provider = SqliteProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("boom", |_ctx: DurableContext, _: ()| async move {
        Err::<(), _>(Error::app("kaboom"))
    });

    let outcome = engine
        .run_workflow::<_, ()>("boom", (), WorkflowOptions::with_id("wf-err"))
        .await?
        .get_result()
        .await;
    assert!(outcome.is_err());

    let handle = engine.retrieve_workflow::<()>("wf-err").await?;
    let status = handle.get_status().await?;
    assert_eq!(status.error.as_deref(), Some("kaboom"));
    assert!(status.error_info.is_none());

    let _ = std::fs::remove_file(path);
    Ok(())
}

#[tokio::test]
async fn portable_written_default_read() -> Result<()> {
    cross_format(Serializer::Portable, Serializer::Json, "p2d").await
}

#[tokio::test]
async fn default_written_portable_read() -> Result<()> {
    cross_format(Serializer::Json, Serializer::Portable, "d2p").await
}
