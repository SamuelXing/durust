//! Conductor client (part 1): connection lifecycle + the executor-lifecycle
//! handlers. A local websocket server stands in for the cloud conductor: it
//! pushes requests and asserts on the client's responses.

use durust::{Conductor, ConductorConfig, DurableEngine, InMemoryProvider, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Send a request and read back the client's response as JSON, skipping any
/// ping/pong control frames.
async fn exchange<S>(ws: &mut WebSocketStream<S>, req: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(Message::Text(req.to_string())).await.unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            _ => continue,
        }
    }
}

#[tokio::test]
async fn conductor_answers_lifecycle_messages() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // The fake conductor: accept one connection and drive the exchanges.
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let info = exchange(&mut ws, json!({"type":"executor_info","request_id":"r1"})).await;
        let recovery = exchange(
            &mut ws,
            json!({"type":"recovery","request_id":"r2","executor_ids":["other"]}),
        )
        .await;
        let exist = exchange(
            &mut ws,
            json!({"type":"exist_pending_workflows","request_id":"r3",
                   "executor_id":"someone","application_version":"9.9.9"}),
        )
        .await;
        let unknown = exchange(&mut ws, json!({"type":"made_up","request_id":"r4"})).await;

        (info, recovery, exist, unknown)
    });

    let engine = Arc::new(
        DurableEngine::new_with_version(Arc::new(InMemoryProvider::new()), "1.2.3").await?,
    );
    engine.launch().await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "test-key".into(),
            app_name: "test-app".into(),
            executor_metadata: Some(json!({"region": "us-east"})),
        },
    )?;

    let (info, recovery, exist, unknown) = server.await.unwrap();

    // executor_info echoes our identity and advertises the Rust client.
    assert_eq!(info["type"], "executor_info");
    assert_eq!(info["request_id"], "r1");
    assert_eq!(info["application_version"], "1.2.3");
    assert_eq!(info["language"], "rust");
    assert_eq!(info["executor_id"], engine.executor_id());
    assert_eq!(info["executor_metadata"]["region"], "us-east");
    assert!(info.get("dbos_version").is_some());

    // recovery for an unknown executor succeeds (nothing to recover).
    assert_eq!(recovery["type"], "recovery");
    assert_eq!(recovery["request_id"], "r2");
    assert_eq!(recovery["success"], true);
    assert!(recovery.get("error_message").is_none());

    // No pending workflows exist for a stranger executor/version.
    assert_eq!(exist["type"], "exist_pending_workflows");
    assert_eq!(exist["request_id"], "r3");
    assert_eq!(exist["exist"], false);

    // An unhandled type still gets a well-formed error response.
    assert_eq!(unknown["type"], "made_up");
    assert_eq!(unknown["request_id"], "r4");
    assert_eq!(unknown["error_message"], "Unknown message type");

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_requires_api_key_and_url() -> Result<()> {
    let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);

    let no_key = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: "ws://localhost:1".into(),
            api_key: String::new(),
            app_name: "app".into(),
            executor_metadata: None,
        },
    );
    assert!(no_key.is_err(), "missing API key is rejected");

    let no_url = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: String::new(),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
        },
    );
    assert!(no_url.is_err(), "missing URL is rejected");
    Ok(())
}
