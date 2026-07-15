#![cfg(all(feature = "postgres", feature = "sqlite"))]
//! End-to-end cross-SDK conformance: durare runs a workflow another SDK wrote.
//!
//! Mirrors `dbos-transact-py`'s `test_interop_direct_insert` (and Go's
//! `TestPortableInterop/DirectDBInsert*`): portable-format rows are inserted into
//! the `dbos` schema via **raw SQL** — exactly as a Python/Go/TypeScript/Java
//! producer would write them — then durare's engine claims the `ENQUEUED`
//! workflow, runs it (reading the portable input, publishing an event, writing a
//! stream, consuming a portable message), and writes its result. We assert the
//! stored output/event/stream are **byte-identical** to the shared golden
//! strings, so this exercises the whole engine, not just the serializer.
//!
//! durare workflows take a single typed input, so a cross-language workflow uses
//! a single-struct argument: the foreign producer writes it as `positionalArgs[0]`
//! (durare reads only the first positional arg of a portable input envelope).

mod common;

use durare::{
    DurableContext, DurableEngine, Error, PostgresProvider, Result, Serializer, SqliteProvider,
    WorkflowQueue,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

// Golden portable strings (byte-identical across dbos-transact-{py,go,ts,java}).
const GOLDEN_OUTPUT_JSON: &str = r#"{"echo_text":"hello-interop","echo_num":42,"echo_dt":"2025-06-15T10:30:00.000Z","items_count":3,"meta_keys":["key1","key2","nested"],"flag":true,"empty":null,"received":{"sender":"test","payload":[1,2,3]}}"#;
const GOLDEN_EVENT_JSON: &str = r#"{"text":"hello-interop","num":42,"flag":true}"#;
const GOLDEN_STREAM_JSON: &str = r#"{"item":"hello-interop"}"#;
const GOLDEN_MESSAGE_JSON: &str = r#"{"sender":"test","payload":[1,2,3]}"#;

/// The single-struct input a foreign producer stores as `positionalArgs[0]`.
#[derive(Deserialize)]
struct CanonicalArgs {
    text: String,
    num: i64,
    dt: String,
    items: Vec<String>,
    meta: serde_json::Map<String, Value>,
    flag: bool,
    #[serde(default)]
    empty: Option<Value>,
}

/// Output whose field order matches the cross-language golden.
#[derive(Serialize, Deserialize)]
struct CanonicalOut {
    echo_text: String,
    echo_num: i64,
    echo_dt: String,
    items_count: i64,
    meta_keys: Vec<String>,
    flag: bool,
    empty: Option<Value>,
    received: Value,
}

/// The durare analog of the SDKs' `canonicalWorkflow`: publish an event, write a
/// stream, consume a message, and echo everything back.
#[durare::workflow(name = "canonicalWorkflow")]
async fn canonical_workflow(ctx: DurableContext, args: CanonicalArgs) -> Result<CanonicalOut> {
    ctx.set_event(
        "interop_status",
        json!({"text": args.text, "num": args.num, "flag": args.flag}),
    )
    .await?;
    ctx.write_stream("interop_stream", json!({"item": args.text}))
        .await?;
    let received: Value = ctx
        .recv("interop_topic", Duration::from_secs(5))
        .await?
        .expect("the pre-inserted portable message");

    let mut meta_keys: Vec<String> = args.meta.keys().cloned().collect();
    meta_keys.sort();
    Ok(CanonicalOut {
        echo_text: args.text.clone(),
        echo_num: args.num,
        echo_dt: args.dt,
        items_count: args.items.len() as i64,
        meta_keys,
        flag: args.flag,
        empty: args.empty,
        received,
    })
}

/// The foreign-written portable input envelope: one positional arg (the struct),
/// its object keys in the same order the golden output echoes them.
fn foreign_input_json() -> String {
    r#"{"positionalArgs":[{"text":"hello-interop","num":42,"dt":"2025-06-15T10:30:00.000Z","items":["alpha","beta","gamma"],"meta":{"key1":"value1","key2":99,"nested":{"deep":true}},"flag":true,"empty":null}]}"#.to_string()
}

#[tokio::test]
async fn durare_runs_a_foreign_written_portable_workflow() -> Result<()> {
    let mut path = std::env::temp_dir();
    path.push(format!("durare-interop-db-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let wf_id = uuid::Uuid::new_v4().to_string();

    // A portable-mode engine (the cross-language configuration). `new` migrates
    // the `dbos` schema onto the file.
    let provider = SqliteProvider::connect(&url)
        .await?
        .with_serializer(Serializer::Portable);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register_queue(WorkflowQueue::new("interopq"));

    // A second connection to the same file, standing in for the *foreign* SDK:
    // write the portable rows with raw SQL, exactly as it would.
    let foreign = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "INSERT INTO workflow_status
            (workflow_uuid, name, inputs, status, executor_id, application_version,
             queue_name, priority, serialization, created_at, updated_at)
         VALUES (?, ?, ?, ?, '', '', ?, 0, 'portable_json', ?, ?)",
    )
    .bind(&wf_id)
    .bind("canonicalWorkflow")
    .bind(foreign_input_json())
    .bind("ENQUEUED")
    .bind("interopq")
    .bind(now)
    .bind(now)
    .execute(&foreign)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO notifications
            (message_uuid, destination_uuid, topic, message, serialization, created_at_epoch_ms)
         VALUES (?, ?, 'interop_topic', ?, 'portable_json', ?)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(&wf_id)
    .bind(GOLDEN_MESSAGE_JSON)
    .bind(now)
    .execute(&foreign)
    .await
    .unwrap();

    // durare claims the foreign ENQUEUED workflow and runs it to completion.
    engine.launch().await?;
    let out: CanonicalOut = engine
        .retrieve_workflow::<CanonicalOut>(&wf_id)
        .await?
        .result()
        .await?;
    assert_eq!(
        out.received,
        json!({"sender": "test", "payload": [1, 2, 3]})
    );
    assert_eq!(out.meta_keys, vec!["key1", "key2", "nested"]);

    // The persisted portable records must be byte-identical to the goldens every
    // other SDK produces.
    let output: String =
        sqlx::query_scalar("SELECT output FROM workflow_status WHERE workflow_uuid = ?")
            .bind(&wf_id)
            .fetch_one(&foreign)
            .await
            .unwrap();
    assert_eq!(output, GOLDEN_OUTPUT_JSON);

    let event: String = sqlx::query_scalar(
        "SELECT value FROM workflow_events WHERE workflow_uuid = ? AND key = 'interop_status'",
    )
    .bind(&wf_id)
    .fetch_one(&foreign)
    .await
    .unwrap();
    assert_eq!(event, GOLDEN_EVENT_JSON);

    let stream: String = sqlx::query_scalar(
        "SELECT value FROM streams WHERE workflow_uuid = ? AND key = 'interop_stream'",
    )
    .bind(&wf_id)
    .fetch_one(&foreign)
    .await
    .unwrap();
    assert_eq!(stream, GOLDEN_STREAM_JSON);

    engine.shutdown(Duration::from_secs(1)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// The same conformance scenario on Postgres — the production, multi-executor
/// backend where cross-SDK interop actually happens. Runs only when
/// `DATABASE_URL` is set (as in CI); a no-op otherwise, like durare's other
/// Postgres tests.
#[tokio::test]
async fn durare_runs_a_foreign_written_portable_workflow_pg() -> Result<()> {
    let Ok(base_url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping pg interop test: DATABASE_URL not set");
        return Ok(());
    };
    // The foreign row below carries `application_version = ''` — claimable only
    // by the engine running the *latest registered* version. The version
    // registry is durable and shared, and every test binary registers its own
    // default version (a hash of its own executable) at launch — so a sibling
    // binary whose registration is fresher would gate this engine off its own
    // row forever. A private database per run makes that interference
    // impossible by construction.
    let (admin, url, dbname) = common::hermetic_pg_db(&base_url, "durare_interop").await;
    let wf_id = uuid::Uuid::new_v4().to_string();

    let provider = PostgresProvider::connect(&url)
        .await?
        .with_serializer(Serializer::Portable);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register_queue(WorkflowQueue::new("interopq"));

    // A separate connection standing in for the foreign SDK's raw writes. Tables
    // live in the `dbos` schema; a fresh pool has no search_path, so qualify them.
    let foreign = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "INSERT INTO dbos.workflow_status
            (workflow_uuid, name, inputs, status, executor_id, application_version,
             queue_name, priority, serialization, created_at, updated_at)
         VALUES ($1, $2, $3, $4, '', '', $5, 0, 'portable_json', $6, $7)",
    )
    .bind(&wf_id)
    .bind("canonicalWorkflow")
    .bind(foreign_input_json())
    .bind("ENQUEUED")
    .bind("interopq")
    .bind(now)
    .bind(now)
    .execute(&foreign)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO dbos.notifications
            (message_uuid, destination_uuid, topic, message, serialization, created_at_epoch_ms)
         VALUES ($1, $2, 'interop_topic', $3, 'portable_json', $4)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(&wf_id)
    .bind(GOLDEN_MESSAGE_JSON)
    .bind(now)
    .execute(&foreign)
    .await
    .unwrap();

    engine.launch().await?;
    let out: CanonicalOut = engine
        .retrieve_workflow::<CanonicalOut>(&wf_id)
        .await?
        .result()
        .await?;
    assert_eq!(
        out.received,
        json!({"sender": "test", "payload": [1, 2, 3]})
    );

    let output: String =
        sqlx::query_scalar("SELECT output FROM dbos.workflow_status WHERE workflow_uuid = $1")
            .bind(&wf_id)
            .fetch_one(&foreign)
            .await
            .unwrap();
    assert_eq!(output, GOLDEN_OUTPUT_JSON);

    let event: String = sqlx::query_scalar(
        "SELECT value FROM dbos.workflow_events WHERE workflow_uuid = $1 AND key = 'interop_status'",
    )
    .bind(&wf_id)
    .fetch_one(&foreign)
    .await
    .unwrap();
    assert_eq!(event, GOLDEN_EVENT_JSON);

    let stream: String = sqlx::query_scalar(
        "SELECT value FROM dbos.streams WHERE workflow_uuid = $1 AND key = 'interop_stream'",
    )
    .bind(&wf_id)
    .fetch_one(&foreign)
    .await
    .unwrap();
    assert_eq!(stream, GOLDEN_STREAM_JSON);

    engine.shutdown(Duration::from_secs(1)).await?;
    foreign.close().await;
    drop(engine);
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}

/// durare reads a workflow another SDK ran and *failed*: the portable error
/// envelope another SDK wrote in the `error` column surfaces as structured
/// `error_info`, and `result()` reconstructs the typed [`Error::Portable`] a
/// caller can match on. (Errors are part of the cross-SDK portable contract —
/// unlike the `DBOS.sleep` wake-instant checkpoint, whose format the SDKs do not
/// agree on: Go and durare store an RFC3339 timestamp, Python an epoch-seconds
/// float, so mid-sleep recovery is not a cross-SDK operation.)
#[tokio::test]
async fn durare_reads_a_foreign_failed_workflow() -> Result<()> {
    let mut path = std::env::temp_dir();
    path.push(format!("durare-interop-err-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let wf_id = uuid::Uuid::new_v4().to_string();

    let provider = SqliteProvider::connect(&url)
        .await?
        .with_serializer(Serializer::Portable);
    let engine = DurableEngine::new(Arc::new(provider)).await?;

    // A foreign SDK's failed workflow: terminal ERROR with a portable error
    // envelope in the `error` column. No engine run — durare just reads it.
    let foreign = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let envelope = r#"{"name":"InsufficientFunds","message":"balance too low","code":402,"data":{"available":10}}"#;
    sqlx::query(
        "INSERT INTO workflow_status
            (workflow_uuid, name, inputs, status, error, executor_id, application_version,
             serialization, created_at, updated_at)
         VALUES (?, ?, ?, 'ERROR', ?, '', '', 'portable_json', ?, ?)",
    )
    .bind(&wf_id)
    .bind("chargeCard")
    .bind(r#"{"positionalArgs":[]}"#)
    .bind(envelope)
    .bind(now)
    .bind(now)
    .execute(&foreign)
    .await
    .unwrap();

    // The structured error the foreign SDK wrote surfaces intact.
    let status = engine
        .retrieve_workflow::<()>(&wf_id)
        .await?
        .get_status()
        .await?;
    assert_eq!(status.status, "ERROR");
    assert_eq!(status.error.as_deref(), Some("balance too low"));
    let info = status.error_info.expect("structured portable error");
    assert_eq!(info.name, "InsufficientFunds");
    assert_eq!(info.code, Some(json!(402)));
    assert_eq!(info.data, Some(json!({"available": 10})));

    // And `result()` reconstructs the typed error.
    match engine.retrieve_workflow::<()>(&wf_id).await?.result().await {
        Err(Error::Portable(pe)) => {
            assert_eq!(pe.name, "InsufficientFunds");
            assert_eq!(pe.code, Some(json!(402)));
        }
        other => panic!("expected Error::Portable, got {other:?}"),
    }

    let _ = std::fs::remove_file(&path);
    Ok(())
}
