//! Admin HTTP server: end-to-end over a real socket. A tiny raw-HTTP/1.1 client
//! (Connection: close) keeps the test dependency-free.

use durust::{
    AdminServer, DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Send one request and read the whole response (status, body).
async fn http(port: u16, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).to_string();
    let status: u16 = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let resp_body = text
        .split_once("\r\n\r\n")
        .map(|x| x.1)
        .unwrap_or("")
        .to_string();
    (status, resp_body)
}

#[tokio::test]
async fn admin_server_endpoints() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("echo", |_ctx: DurableContext, msg: String| async move {
        Ok::<_, Error>(msg)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;

    // Produce one finished workflow under a known id.
    let h = engine
        .start::<_, String>("echo", "hi".to_string(), WorkflowOptions::with_id("wf-1"))
        .await?;
    assert_eq!(h.result().await?, "hi");

    let admin = AdminServer::start(engine.clone(), 0).await?;
    let port = admin.port();
    assert_ne!(port, 0, "an ephemeral port was assigned");

    // Health.
    let (status, body) = http(port, "GET", "/dbos-healthz", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("healthy"), "health body: {body}");

    // Conductor handshake placeholder.
    let (status, body) = http(port, "GET", "/conductor", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("true"), "conductor body: {body}");

    // Queue metadata (the always-on internal queue is registered but hidden from
    // the public listing, so the array is empty until a user queue is added).
    let (status, body) = http(port, "GET", "/dbos-workflow-queues-metadata", None).await;
    assert_eq!(status, 200);
    assert!(body.starts_with('['), "queue metadata is an array: {body}");

    // List workflows (no body) returns our finished run.
    let (status, body) = http(port, "POST", "/workflows", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("wf-1"), "list body: {body}");
    assert!(body.contains("SUCCESS"), "list body: {body}");

    // List with a filter body that matches nothing.
    let (status, body) = http(
        port,
        "POST",
        "/workflows",
        Some(r#"{"workflow_name":"does-not-exist"}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.trim(), "[]", "filtered list empty: {body}");

    // Single workflow.
    let (status, body) = http(port, "GET", "/workflows/wf-1", None).await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"WorkflowUUID\":\"wf-1\""),
        "get body: {body}"
    );

    // Missing workflow → 404.
    let (status, _) = http(port, "GET", "/workflows/nope", None).await;
    assert_eq!(status, 404);

    // Steps for the workflow.
    let (status, body) = http(port, "GET", "/workflows/wf-1/steps", None).await;
    assert_eq!(status, 200);
    assert!(body.starts_with('['), "steps is an array: {body}");

    // Cancel is idempotent on a finished workflow → 204.
    let (status, _) = http(port, "POST", "/workflows/wf-1/cancel", None).await;
    assert_eq!(status, 204);

    // Recovery for an unknown executor recovers nothing → empty array.
    let (status, body) = http(
        port,
        "POST",
        "/dbos-workflow-recovery",
        Some(r#"["other"]"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body.trim(), "[]", "recovery body: {body}");

    // Global timeout with a past cutoff cancels nothing pending → 204.
    let (status, _) = http(
        port,
        "POST",
        "/dbos-global-timeout",
        Some(r#"{"cutoff_epoch_timestamp_ms":1}"#),
    )
    .await;
    assert_eq!(status, 204);

    // Garbage collect is a reserved no-op → 204.
    let (status, _) = http(port, "POST", "/dbos-garbage-collect", Some("{}")).await;
    assert_eq!(status, 204);

    // Deactivate stops dispatch and reports it on the engine.
    assert!(!engine.is_deactivated());
    let (status, body) = http(port, "GET", "/deactivate", None).await;
    assert_eq!(status, 200);
    assert_eq!(body.trim(), "deactivated");
    assert!(engine.is_deactivated(), "engine marked deactivated");

    admin.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn admin_fork_endpoint_returns_new_id() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("echo", |_ctx: DurableContext, msg: String| async move {
        Ok::<_, Error>(msg)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;

    let h = engine
        .start::<_, String>("echo", "hi".to_string(), WorkflowOptions::with_id("orig"))
        .await?;
    h.result().await?;

    let admin = AdminServer::start(engine.clone(), 0).await?;
    let port = admin.port();

    let (status, body) = http(
        port,
        "POST",
        "/workflows/orig/fork",
        Some(r#"{"start_step":0,"new_workflow_id":"forked-1"}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"workflow_id\":\"forked-1\""),
        "fork body: {body}"
    );

    admin.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
