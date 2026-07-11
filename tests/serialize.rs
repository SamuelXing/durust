//! Serialization-format parity: values written under one format are decoded
//! correctly by a provider configured for another (dispatch-on-read), which is
//! the basis for cross-language interop via `portable_json`.

use durust::{
    DurableContext, DurableEngine, Error, PortableWorkflowError, Result, Serializer,
    SqliteProvider, WorkflowOptions,
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
            .start::<_, String>(
                "greet",
                "ada".to_string(),
                WorkflowOptions::with_id("wf-ser"),
            )
            .await?
            .result()
            .await?;
        assert_eq!(out, "hi ada");
    }

    // Reader process: a fresh engine whose provider encodes in a *different*
    // format must still decode the persisted input, step output, and result.
    {
        let engine = engine_with(&url, reader).await?;
        let handle = engine.retrieve_workflow::<String>("wf-ser").await?;
        let status = handle.get_status().await?;
        assert_eq!(status.input, serde_json::json!("ada"));
        assert_eq!(status.output, Some(serde_json::json!("hi ada")));
        assert_eq!(handle.result().await?, "hi ada");
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
        .start::<_, ()>("boom", (), WorkflowOptions::with_id("wf-err"))
        .await?
        .result()
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

/// A workflow that raises a structured `Error::Portable` under portable mode
/// stores its full type name + code + data, and a separate reader recovers them
/// — both as the structured `error_info` and as a reconstructed `Error::Portable`
/// from `result`, the cross-language structured-error round-trip.
#[tokio::test]
async fn portable_typed_error_round_trips() -> Result<()> {
    let (url, path) = temp_db_url("err-typed");

    // Writer: a workflow fails with a typed, cross-language error.
    {
        let provider = SqliteProvider::connect(&url)
            .await?
            .with_serializer(Serializer::Portable);
        let mut engine = DurableEngine::new(Arc::new(provider)).await?;
        engine.register("validate", |_ctx: DurableContext, _: ()| async move {
            Err::<(), _>(Error::Portable(PortableWorkflowError {
                name: "ValidationError".to_string(),
                message: "bad email".to_string(),
                code: Some(serde_json::json!(400)),
                data: Some(serde_json::json!({"field": "email"})),
            }))
        });
        let outcome = engine
            .start::<_, ()>("validate", (), WorkflowOptions::with_id("wf-typed"))
            .await?
            .result()
            .await;
        // The owning caller gets the typed error straight back.
        assert!(matches!(outcome, Err(Error::Portable(ref pe)) if pe.name == "ValidationError"));
    }

    // Reader: a fresh engine recovers the structured error from storage.
    {
        let provider = SqliteProvider::connect(&url)
            .await?
            .with_serializer(Serializer::Portable);
        let engine = DurableEngine::new(Arc::new(provider)).await?;
        let handle = engine.retrieve_workflow::<()>("wf-typed").await?;

        let status = handle.get_status().await?;
        let info = status
            .error_info
            .expect("structured error survives storage");
        assert_eq!(info.name, "ValidationError");
        assert_eq!(info.code, Some(serde_json::json!(400)));
        assert_eq!(info.data, Some(serde_json::json!({"field": "email"})));

        // result reconstructs the typed error for the observer.
        let handle = handle;
        match handle.result().await {
            Err(Error::Portable(pe)) => {
                assert_eq!(pe.name, "ValidationError");
                assert_eq!(pe.message, "bad email");
                assert_eq!(pe.code, Some(serde_json::json!(400)));
            }
            other => panic!("expected a reconstructed portable error, got {other:?}"),
        }
    }

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
        .start::<_, ()>("boom", (), WorkflowOptions::with_id("wf-err"))
        .await?
        .result()
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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct Row {
    id: i64,
    tags: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct Report {
    title: String,
    rows: Vec<Row>,
    meta: std::collections::HashMap<String, i64>,
    note: Option<String>,
}

fn sample_report() -> Report {
    let mut meta = std::collections::HashMap::new();
    meta.insert("total".to_string(), 3);
    meta.insert("ok".to_string(), 2);
    Report {
        title: "quarterly".to_string(),
        rows: vec![
            Row {
                id: 1,
                tags: vec!["a".to_string(), "b".to_string()],
            },
            Row {
                id: 2,
                tags: vec![],
            },
        ],
        meta,
        note: None,
    }
}

/// A deeply-nested value (struct with vectors, a map, an option, and nested
/// structs) survives real TEXT (de)serialization on SQLite intact — not just an
/// in-memory clone: the step serializes it into its checkpoint and the workflow
/// output, and it reconstructs to the exact structure on read.
///
/// Re-submitting the same id returns the stored output without running the body
/// again (OAOO completion), so the builder step stays at a single execution. That
/// is start-time dedup, not mid-flight step-checkpoint replay — the body is not
/// re-executed here.
#[tokio::test]
async fn nested_value_round_trips_through_step_checkpoint() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static BUILDS: AtomicUsize = AtomicUsize::new(0);

    let (url, path) = temp_db_url("nested");
    let provider = SqliteProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("report", |ctx: DurableContext, _: ()| async move {
        let r = ctx
            .step("build", || async {
                BUILDS.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(sample_report())
            })
            .await?;
        Ok::<_, Error>(r)
    });

    // First execution builds and checkpoints the nested value.
    let a: Report = engine
        .start("report", (), WorkflowOptions::with_id("wf-nested"))
        .await?
        .result()
        .await?;
    assert_eq!(a, sample_report());

    // Re-submitting the same id returns the stored output, deserialized back to
    // the identical structure, without re-running the body.
    let b: Report = engine
        .start("report", (), WorkflowOptions::with_id("wf-nested"))
        .await?
        .result()
        .await?;
    assert_eq!(b, sample_report());
    assert_eq!(
        BUILDS.load(Ordering::SeqCst),
        1,
        "the step ran once; the resubmit returns the stored output, not a re-run"
    );

    drop(engine);
    let _ = std::fs::remove_file(path);
    Ok(())
}
