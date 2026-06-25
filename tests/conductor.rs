//! Conductor client (part 1): connection lifecycle + the executor-lifecycle
//! handlers. A local websocket server stands in for the cloud conductor: it
//! pushes requests and asserts on the client's responses.

use durust::{
    Conductor, ConductorConfig, DurableContext, DurableEngine, Error, InMemoryProvider, Result,
    WorkflowOptions,
};
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

#[tokio::test]
async fn conductor_handles_workflow_management() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let list = exchange(
            &mut ws,
            json!({"type":"list_workflows","request_id":"l","body":{}}),
        )
        .await;
        let get = exchange(
            &mut ws,
            json!({"type":"get_workflow","request_id":"g","workflow_id":"wf-1",
                   "load_input":true,"load_output":true}),
        )
        .await;
        let steps = exchange(
            &mut ws,
            json!({"type":"list_steps","request_id":"s","workflow_id":"wf-1"}),
        )
        .await;
        let fork = exchange(
            &mut ws,
            json!({"type":"fork_workflow","request_id":"f",
                   "body":{"workflow_id":"wf-1","start_step":0,"new_workflow_id":"forked-1"}}),
        )
        .await;
        let cancel = exchange(
            &mut ws,
            json!({"type":"cancel","request_id":"c","workflow_id":"wf-1"}),
        )
        .await;
        let delete = exchange(
            &mut ws,
            json!({"type":"delete","request_id":"d","workflow_ids":["forked-1"]}),
        )
        .await;
        (list, get, steps, fork, cancel, delete)
    });

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("work", |ctx: DurableContext, msg: String| async move {
        let r = ctx
            .step("s1", || async { Ok::<_, Error>(format!("{msg}!")) })
            .await?;
        Ok::<_, Error>(r)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;
    let mut h = engine
        .run_workflow::<_, String>("work", "hi".to_string(), WorkflowOptions::with_id("wf-1"))
        .await?;
    assert_eq!(h.get_result().await?, "hi!");

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
        },
    )?;

    let (list, get, steps, fork, cancel, delete) = server.await.unwrap();

    // list_workflows -> output array carrying the conductor wire shape.
    assert_eq!(list["type"], "list_workflows");
    let rows = list["output"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["WorkflowUUID"], "wf-1");
    assert_eq!(rows[0]["Status"], "SUCCESS");
    assert_eq!(rows[0]["WasForkedFrom"], false);
    assert_eq!(rows[0]["Priority"], "0"); // priority always present, stringified

    // get_workflow -> single body with the output loaded.
    assert_eq!(get["output"]["WorkflowUUID"], "wf-1");
    assert_eq!(get["output"]["Output"], "\"hi!\"");

    // list_steps -> the one recorded step.
    let step_list = steps["output"].as_array().unwrap();
    assert!(step_list.iter().any(|s| s["function_name"] == "s1"));

    // fork -> the new id we asked for.
    assert_eq!(fork["new_workflow_id"], "forked-1");

    // cancel / delete -> success.
    assert_eq!(cancel["success"], true);
    assert_eq!(delete["success"], true);

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_closes_gracefully_on_shutdown() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // Server: complete one exchange, signal readiness, then read until it sees
    // the client's Close frame (the graceful closing handshake).
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let _ = exchange(&mut ws, json!({"type":"executor_info","request_id":"r1"})).await;
        ready_tx.send(()).unwrap();
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_))) => return true,
                Some(Ok(_)) => continue, // skip pings / other frames
                Some(Err(_)) | None => return false,
            }
        }
    });

    let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);
    engine.launch().await?;
    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
        },
    )?;

    // Only shut down once the client is connected and has answered a request.
    ready_rx.await.unwrap();
    conductor.shutdown(Duration::from_secs(2)).await?;

    assert!(
        server.await.unwrap(),
        "server observed the client's Close frame"
    );
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
