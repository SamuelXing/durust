//! Conductor client — the connection to the DBOS cloud control plane.
//!
//! A long-lived websocket *client* that dials the conductor service, then
//! answers the requests it pushes (executor info, recovery, workflow and
//! schedule management, …). The connection self-heals: it pings to keep the
//! link alive and reconnects with exponential backoff after a drop.
//!
//! This is **part 1**: connection lifecycle + the executor-lifecycle handlers
//! (`executor_info`, `recovery`, `exist_pending_workflows`). Every other
//! message type is answered with a well-formed "unknown message type" error
//! until its handler lands, so the link stays healthy as coverage grows.
//!
//! Opt-in, like the admin server:
//! ```no_run
//! # use durust::{DurableEngine, InMemoryProvider, Conductor, ConductorConfig};
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # async fn run() -> durust::Result<()> {
//! let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);
//! engine.launch().await?;
//! let conductor = Conductor::start(engine.clone(), ConductorConfig {
//!     url: "wss://conductor.dbos.dev".into(),
//!     api_key: std::env::var("DBOS_CONDUCTOR_KEY").unwrap(),
//!     app_name: "my-app".into(),
//!     executor_metadata: None,
//! })?;
//! // ... runs in the background ...
//! conductor.shutdown(Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! ```

use crate::engine::DurableEngine;
use crate::error::{Error, Result};
use crate::provider::{ListFilter, STATUS_PENDING};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

/// The DBOS version this client advertises to the conductor.
const DBOS_VERSION: &str = env!("CARGO_PKG_VERSION");

const PING_INTERVAL: Duration = Duration::from_secs(20);
// Slightly above the server's executor-ping wait; a healthy link receives a
// pong (or any frame) well within this, so a lapse means the link is dead.
const PING_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_RECONNECT_WAIT: Duration = Duration::from_secs(1);
const MAX_RECONNECT_WAIT: Duration = Duration::from_secs(30);

/// Configuration for [`Conductor::start`].
pub struct ConductorConfig {
    /// Base conductor URL, e.g. `wss://conductor.dbos.dev` (the
    /// `/websocket/{app}/{key}` path is appended automatically).
    pub url: String,
    /// API key for this application.
    pub api_key: String,
    /// Application name.
    pub app_name: String,
    /// Optional free-form metadata reported in `executor_info` responses.
    pub executor_metadata: Option<Value>,
}

/// A running conductor connection. Call [`shutdown`](Self::shutdown) to stop it.
pub struct Conductor {
    token: CancellationToken,
    task: JoinHandle<()>,
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

impl Conductor {
    /// Validate the config, then launch the connection loop on a background
    /// task. Returns immediately; the connection is established (and re-
    /// established) in the background.
    pub fn start(engine: Arc<DurableEngine>, config: ConductorConfig) -> Result<Conductor> {
        if config.api_key.is_empty() {
            return Err(Error::app("conductor API key is required"));
        }
        if config.url.is_empty() {
            return Err(Error::app("conductor URL is required"));
        }
        let ws_url = format!(
            "{}/websocket/{}/{}",
            config.url.trim_end_matches('/'),
            config.app_name,
            config.api_key
        );
        let token = CancellationToken::new();
        let task = tokio::spawn(connection_loop(engine, config, ws_url, token.clone()));
        Ok(Conductor { token, task })
    }

    /// Signal the connection loop to stop and wait up to `timeout` for it.
    pub async fn shutdown(mut self, timeout: Duration) -> Result<()> {
        self.token.cancel();
        let _ = tokio::time::timeout(timeout, &mut self.task).await;
        Ok(())
    }
}

/// Reconnect loop: connect, serve until the link drops or we are told to stop,
/// and back off (with jitter) between failed connection attempts.
async fn connection_loop(
    engine: Arc<DurableEngine>,
    config: ConductorConfig,
    ws_url: String,
    token: CancellationToken,
) {
    let mut backoff = INITIAL_RECONNECT_WAIT;
    loop {
        if token.is_cancelled() {
            return;
        }
        match connect_async(ws_url.as_str()).await {
            Ok((mut ws, _resp)) => {
                tracing::info!("connected to DBOS conductor");
                backoff = INITIAL_RECONNECT_WAIT;
                let stopped = serve(&mut ws, &engine, &config, &token).await;
                if stopped {
                    return;
                }
                tracing::debug!("conductor link dropped; reconnecting");
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to connect to conductor");
                // Exponential backoff with jitter, interruptible by shutdown.
                let wait = jittered(backoff, &ws_url);
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
                backoff = (backoff * 2).min(MAX_RECONNECT_WAIT);
            }
        }
    }
}

/// Serve one connection until it drops or shutdown is signalled. Returns `true`
/// if we are stopping (do not reconnect), `false` if the link merely dropped.
async fn serve(
    ws: &mut WsStream,
    engine: &Arc<DurableEngine>,
    config: &ConductorConfig,
    token: &CancellationToken,
) -> bool {
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                let _ = ws.send(Message::Close(None)).await;
                return true;
            }
            _ = ping.tick() => {
                if ws.send(Message::Ping(Vec::new())).await.is_err() {
                    return false; // write failed -> reconnect
                }
            }
            // Any received frame (text, pong, …) resets this deadline by virtue
            // of wrapping each read; a lapse means the link is dead.
            res = tokio::time::timeout(PING_TIMEOUT, ws.next()) => {
                match res {
                    Err(_elapsed) => return false,        // read deadline -> reconnect
                    Ok(None) => return false,             // stream closed
                    Ok(Some(Err(_))) => return false,     // read error
                    Ok(Some(Ok(msg))) => match msg {
                        Message::Text(text) => {
                            if let Err(e) = handle_message(engine, config, ws, &text).await {
                                tracing::error!(error = %e, "failed to handle conductor message");
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => return false,
                        _ => {} // pong / binary: just activity
                    },
                }
            }
        }
    }
}

/// The base fields every conductor message carries.
#[derive(Deserialize)]
struct BaseMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(default)]
    request_id: String,
}

#[derive(Deserialize)]
struct RecoveryRequest {
    #[serde(default)]
    executor_ids: Vec<String>,
}

#[derive(Deserialize)]
struct ExistPendingRequest {
    #[serde(default)]
    executor_id: String,
    #[serde(default)]
    application_version: String,
}

/// Parse the message type and dispatch to the matching handler. Unhandled types
/// get a well-formed error response so the conductor link stays healthy.
async fn handle_message(
    engine: &Arc<DurableEngine>,
    config: &ConductorConfig,
    ws: &mut WsStream,
    text: &str,
) -> Result<()> {
    let base: BaseMessage = serde_json::from_str(text)?;
    let rid = base.request_id.as_str();
    match base.msg_type.as_str() {
        "executor_info" => {
            let mut resp = base_response("executor_info", rid, None);
            resp.insert("executor_id".into(), json!(engine.executor_id()));
            resp.insert("application_version".into(), json!(engine.app_version()));
            if let Some(h) = hostname() {
                resp.insert("hostname".into(), json!(h));
            }
            resp.insert("dbos_version".into(), json!(DBOS_VERSION));
            resp.insert("language".into(), json!("rust"));
            if let Some(meta) = &config.executor_metadata {
                resp.insert("executor_metadata".into(), meta.clone());
            }
            send(ws, resp).await
        }
        "recovery" => {
            let req: RecoveryRequest = serde_json::from_str(text)?;
            let err = engine
                .recover_pending_for(&req.executor_ids)
                .await
                .err()
                .map(|e| format!("failed to recover pending workflows: {e}"));
            let mut resp = base_response("recovery", rid, err.clone());
            resp.insert("success".into(), json!(err.is_none()));
            send(ws, resp).await
        }
        "exist_pending_workflows" => {
            let req: ExistPendingRequest = serde_json::from_str(text)?;
            let filter = ListFilter {
                status: vec![STATUS_PENDING.to_string()],
                executor_ids: vec![req.executor_id],
                app_version: Some(req.application_version),
                limit: Some(1),
                ..Default::default()
            };
            let (exist, err) = match engine.list_workflows(&filter).await {
                Ok(rows) => (!rows.is_empty(), None),
                Err(e) => (
                    false,
                    Some(format!("failed to check for pending workflows: {e}")),
                ),
            };
            let mut resp = base_response("exist_pending_workflows", rid, err);
            resp.insert("exist".into(), json!(exist));
            send(ws, resp).await
        }
        other => {
            tracing::warn!(msg_type = other, "unknown conductor message type");
            let resp = base_response(other, rid, Some("Unknown message type".to_string()));
            send(ws, resp).await
        }
    }
}

/// Build a response object with the shared `type`/`request_id`/`error_message`
/// fields (the error is omitted when `None`, matching the wire `omitempty`).
fn base_response(msg_type: &str, request_id: &str, error: Option<String>) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("type".into(), json!(msg_type));
    m.insert("request_id".into(), json!(request_id));
    if let Some(e) = error {
        m.insert("error_message".into(), json!(e));
    }
    m
}

/// Serialize and send a JSON response as a text frame.
async fn send(ws: &mut WsStream, resp: Map<String, Value>) -> Result<()> {
    let text = serde_json::to_string(&Value::Object(resp))?;
    ws.send(Message::Text(text))
        .await
        .map_err(|e| Error::app(format!("failed to send conductor response: {e}")))
}

/// Best-effort hostname for `executor_info` (the field is optional on the wire).
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty())
}

/// Reconnect wait with ±50% jitter to avoid a thundering herd. Deterministic per
/// URL (no `Math.random` equivalent needed) — enough spread across executors,
/// which dial distinct URLs.
fn jittered(base: Duration, seed: &str) -> Duration {
    let h = seed
        .bytes()
        .fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64));
    let frac = (h % 1000) as f64 / 1000.0; // 0.0..1.0
    let factor = 0.5 + frac; // 0.5..1.5
    base.mul_f64(factor)
}
