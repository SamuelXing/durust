//! Conductor client — the connection to the DBOS cloud control plane.
//!
//! A long-lived websocket *client* that dials the conductor service, then
//! answers the requests it pushes (executor info, recovery, workflow and
//! schedule management, …). The connection self-heals: it pings to keep the
//! link alive and reconnects with exponential backoff after a drop.
//!
//! Implemented so far: the executor-lifecycle handlers (`executor_info`,
//! `recovery`, `exist_pending_workflows`), workflow management/queries
//! (`cancel`, `resume`, `delete`, `fork_workflow`, `list_workflows`,
//! `list_queued_workflows`, `get_workflow`, `list_steps`), and schedule
//! management (`list_schedules`, `get_schedule`, `pause_schedule`,
//! `resume_schedule`, `backfill_schedule`, `trigger_schedule`), and the
//! registry/analytics messages (`list_queues`, `get_queue`,
//! `list_application_versions`, `set_latest_application_version`,
//! `get_workflow_aggregates`, `get_step_aggregates`), and the per-workflow
//! observability reads (`get_workflow_events`, `get_workflow_notifications`,
//! `get_workflow_streams`), and the ops messages (`get_metrics`, `retention`),
//! the portable transfer messages (`export_workflow`, `import_workflow`), and the
//! `alert` message (delivered to an optional user-registered handler). Every
//! other message type is answered with a well-formed "unknown message type"
//! error until its handler lands, so the link stays healthy as coverage grows.
//!
//! Opt-in, like the admin server:
//! ```no_run
//! # use durare::{DurableEngine, InMemoryProvider, Conductor, ConductorConfig};
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # async fn run() -> durare::Result<()> {
//! let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);
//! engine.launch().await?;
//! let conductor = Conductor::start(engine.clone(), ConductorConfig {
//!     url: String::new(), // empty = the hosted DBOS conductor
//!     api_key: std::env::var("DBOS_CONDUCTOR_KEY").unwrap(),
//!     app_name: "my-app".into(),
//!     executor_metadata: None,
//!     alert_handler: None,
//! })?;
//! // ... runs in the background ...
//! conductor.shutdown(Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! ```

use crate::engine::{DurableEngine, WorkflowOptions};
use crate::error::{Error, Result};
use crate::provider::{
    ExportedWorkflow, ListFilter, StepAggregate, StepAggregateQuery, StepInfo, VersionInfo,
    WorkflowAggregate, WorkflowAggregateQuery, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED,
    STATUS_PENDING,
};
use crate::queue::WorkflowQueue;
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
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
// How long to drive the closing handshake (drain the peer's Close echo) before
// giving up and dropping the connection.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Handler invoked when the conductor delivers an `alert` message: receives the
/// alert's `name`, `message`, and `metadata`. Registered via
/// [`ConductorConfig::alert_handler`]; a panic inside it is caught and reported
/// back to the conductor as a failure rather than tearing down the connection.
pub type AlertHandler = Arc<dyn Fn(&str, &str, &HashMap<String, String>) + Send + Sync>;

/// Configuration for [`Conductor::start`].
pub struct ConductorConfig {
    /// Base conductor URL. Leave **empty** for the hosted DBOS conductor,
    /// `wss://cloud.dbos.dev/conductor/v1alpha1` (the domain part honors the
    /// `DBOS_DOMAIN` env var, like the other DBOS SDKs). The
    /// `/websocket/{app}/{key}` path is appended automatically either way.
    pub url: String,
    /// API key for this application. A secret: it is embedded in the websocket
    /// URL per the conductor protocol, and durare never logs it — connection
    /// failures log only the transport error. `ConductorConfig` deliberately
    /// implements no `Debug`, so a config cannot leak through `{:?}` formatting:
    ///
    /// ```compile_fail
    /// fn debug_it(c: &durare::ConductorConfig) -> String {
    ///     format!("{c:?}")
    /// }
    /// ```
    pub api_key: String,
    /// Application name.
    pub app_name: String,
    /// Optional free-form metadata reported in `executor_info` responses.
    pub executor_metadata: Option<Value>,
    /// Optional handler for `alert` messages pushed by the conductor. When
    /// absent, alerts are logged and acknowledged as success.
    pub alert_handler: Option<AlertHandler>,
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
        let ws_url = websocket_url(&config);
        let token = CancellationToken::new();
        let task = tokio::spawn(connection_loop(engine, config, ws_url, token.clone()));
        Ok(Conductor { token, task })
    }

    /// Signal the connection loop to stop and wait up to `timeout` for it to
    /// finish its graceful close. If the grace period elapses (e.g. a wedged
    /// write), the background task is aborted so it cannot linger detached —
    /// dropping a `JoinHandle` alone does not stop a task.
    pub async fn shutdown(self, timeout: Duration) -> Result<()> {
        self.token.cancel();
        let abort = self.task.abort_handle();
        if tokio::time::timeout(timeout, self.task).await.is_err() {
            tracing::warn!("conductor did not stop within timeout; aborting");
            abort.abort();
        }
        Ok(())
    }
}

/// The full websocket URL: the configured base — or, when empty, the hosted
/// DBOS conductor built from `DBOS_DOMAIN` (default `cloud.dbos.dev`), the
/// same rule as the Go and Python SDKs — with `/websocket/{app}/{key}`
/// appended.
fn websocket_url(config: &ConductorConfig) -> String {
    let base = if config.url.is_empty() {
        // A set-but-empty DBOS_DOMAIN counts as unset (Go's rule; Python
        // would build a hostless URL here) — an empty domain is always a
        // misconfiguration, never a target.
        let domain = std::env::var("DBOS_DOMAIN")
            .ok()
            .filter(|d| !d.is_empty())
            .unwrap_or_else(|| "cloud.dbos.dev".to_string());
        format!("wss://{domain}/conductor/v1alpha1")
    } else {
        config.url.trim_end_matches('/').to_string()
    };
    format!("{}/websocket/{}/{}", base, config.app_name, config.api_key)
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
///
/// Cancel safety: only the *waiting* futures sit in the `select!` head
/// (cancellation, the ping tick, the read deadline). All message processing
/// runs in a branch **body**, which `select!` never interrupts — so an in-flight
/// `handle_message` (DB work + its response write) always completes before
/// cancellation is observed on the next turn. A request is therefore never torn
/// mid-processing, and an inbound frame is only consumed when its read branch
/// actually wins (so the select race never drops a message). On cancellation we
/// run the closing handshake via [`close_gracefully`] rather than dropping.
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
                close_gracefully(ws).await;
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

/// Complete the WebSocket closing handshake: send a Close frame (flushing any
/// final response already in the sink), then drain the peer's remaining frames
/// until it echoes Close — bounded by [`CLOSE_DRAIN_TIMEOUT`]. This releases the
/// connection cleanly instead of resetting it. Best-effort: a dead peer just
/// makes the drain return early.
async fn close_gracefully(ws: &mut WsStream) {
    if ws.close(None).await.is_err() {
        return; // peer already gone; nothing to drain
    }
    let drain = async { while let Some(Ok(_)) = ws.next().await {} };
    let _ = tokio::time::timeout(CLOSE_DRAIN_TIMEOUT, drain).await;
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
    #[serde(default, deserialize_with = "null_default")]
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
                app_version: vec![req.application_version],
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
        "cancel" => handle_ids(engine, ws, rid, text, IdsAction::Cancel).await,
        "resume" => handle_ids(engine, ws, rid, text, IdsAction::Resume).await,
        "delete" => handle_ids(engine, ws, rid, text, IdsAction::Delete).await,
        "fork_workflow" => handle_fork(engine, ws, rid, text).await,
        "list_workflows" => handle_list(engine, ws, rid, text, false).await,
        "list_queued_workflows" => handle_list(engine, ws, rid, text, true).await,
        "get_workflow" => handle_get_workflow(engine, ws, rid, text).await,
        "list_steps" => handle_list_steps(engine, ws, rid, text).await,
        "list_schedules" => handle_list_schedules(engine, ws, rid, text).await,
        "get_schedule" => handle_get_schedule(engine, ws, rid, text).await,
        "pause_schedule" => handle_schedule_toggle(engine, ws, rid, text, true).await,
        "resume_schedule" => handle_schedule_toggle(engine, ws, rid, text, false).await,
        "backfill_schedule" => handle_backfill_schedule(engine, ws, rid, text).await,
        "trigger_schedule" => handle_trigger_schedule(engine, ws, rid, text).await,
        "list_queues" => handle_list_queues(engine, ws, rid).await,
        "get_queue" => handle_get_queue(engine, ws, rid, text).await,
        "list_application_versions" => handle_list_versions(engine, ws, rid).await,
        "set_latest_application_version" => handle_set_latest_version(engine, ws, rid, text).await,
        "get_workflow_aggregates" => handle_workflow_aggregates(engine, ws, rid, text).await,
        "get_step_aggregates" => handle_step_aggregates(engine, ws, rid, text).await,
        "get_workflow_events" => handle_get_events(engine, ws, rid, text).await,
        "get_workflow_notifications" => handle_get_notifications(engine, ws, rid, text).await,
        "get_workflow_streams" => handle_get_streams(engine, ws, rid, text).await,
        "get_metrics" => handle_get_metrics(engine, ws, rid, text).await,
        "retention" => handle_retention(engine, ws, rid, text).await,
        "export_workflow" => handle_export_workflow(engine, ws, rid, text).await,
        "import_workflow" => handle_import_workflow(engine, ws, rid, text).await,
        "alert" => handle_alert(config, ws, rid, text).await,
        other => {
            tracing::warn!(msg_type = other, "unknown conductor message type");
            let resp = base_response(other, rid, Some("Unknown message type".to_string()));
            send(ws, resp).await
        }
    }
}

/// Bulk id-based management messages, all shaped `{workflow_id?, workflow_ids?}`
/// → `{success}`.
enum IdsAction {
    Cancel,
    Resume,
    Delete,
}

#[derive(Deserialize)]
struct IdsRequest {
    #[serde(default)]
    workflow_id: String,
    #[serde(default, deserialize_with = "null_default")]
    workflow_ids: Vec<String>,
    #[serde(default)]
    delete_children: bool,
    /// Resume only: re-enqueue onto this named queue instead of the internal
    /// one, so the resumed run competes under that queue's limits.
    #[serde(default)]
    queue_name: Option<String>,
}

async fn handle_ids(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
    action: IdsAction,
) -> Result<()> {
    let req: IdsRequest = serde_json::from_str(text)?;
    let mut ids = req.workflow_ids;
    if ids.is_empty() && !req.workflow_id.is_empty() {
        ids = vec![req.workflow_id];
    }
    let (type_str, result) = match action {
        IdsAction::Cancel => ("cancel", engine.cancel_workflows(&ids).await),
        IdsAction::Resume => (
            "resume",
            match req.queue_name.as_deref() {
                Some(queue) => engine
                    .resume_workflows_on::<Value>(&ids, queue)
                    .await
                    .map(|_| ()),
                None => engine.resume_workflows::<Value>(&ids).await.map(|_| ()),
            },
        ),
        IdsAction::Delete => (
            "delete",
            engine.delete_workflows(&ids, req.delete_children).await,
        ),
    };
    let err = result
        .err()
        .map(|e| format!("failed to {type_str} workflows: {e}"));
    let mut resp = base_response(type_str, rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ForkRequest {
    #[serde(default)]
    body: ForkBody,
}

#[derive(Default, Deserialize)]
struct ForkBody {
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    start_step: i64,
    application_version: Option<String>,
    new_workflow_id: Option<String>,
    queue_name: Option<String>,
    queue_partition_key: Option<String>,
}

async fn handle_fork(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ForkRequest = serde_json::from_str(text)?;
    let body = req.body;
    // Validate the step index to avoid an out-of-range cast.
    if body.start_step < 0 || body.start_step > (i32::MAX as i64) / 2 {
        let resp = base_response("fork_workflow", rid, Some("invalid start_step".into()));
        return send(ws, resp).await;
    }
    let mut opts = WorkflowOptions::default();
    if let Some(id) = body.new_workflow_id {
        opts.workflow_id = Some(id);
    }
    if let Some(v) = body.application_version {
        opts.app_version = Some(v);
    }
    if let Some(q) = body.queue_name {
        opts.queue = Some(q);
    }
    if let Some(k) = body.queue_partition_key {
        opts.partition_key = Some(k);
    }
    let (new_id, err) = match engine
        .fork_workflow::<Value>(&body.workflow_id, body.start_step as i32, opts)
        .await
    {
        Ok(h) => (Some(h.id().to_string()), None),
        Err(e) => (None, Some(format!("failed to fork workflow: {e}"))),
    };
    let mut resp = base_response("fork_workflow", rid, err);
    if let Some(id) = new_id {
        resp.insert("new_workflow_id".into(), json!(id));
    }
    send(ws, resp).await
}

async fn handle_list(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
    queued: bool,
) -> Result<()> {
    let req: ListRequest = serde_json::from_str(text)?;
    let filter = req.body.to_filter(queued);
    let type_str = if queued {
        "list_queued_workflows"
    } else {
        "list_workflows"
    };
    let (output, err) = match engine.list_workflows(&filter).await {
        Ok(rows) => (
            rows.iter().map(format_list_workflow).collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (vec![], Some(format!("failed to list workflows: {e}"))),
    };
    let mut resp = base_response(type_str, rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct GetWorkflowRequest {
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    load_input: bool,
    #[serde(default)]
    load_output: bool,
}

async fn handle_get_workflow(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: GetWorkflowRequest = serde_json::from_str(text)?;
    let filter = ListFilter {
        workflow_ids: vec![req.workflow_id],
        load_input: req.load_input,
        load_output: req.load_output,
        ..Default::default()
    };
    let (output, err) = match engine.list_workflows(&filter).await {
        Ok(rows) => (rows.first().map(format_list_workflow), None),
        Err(e) => (None, Some(format!("failed to get workflow: {e}"))),
    };
    let mut resp = base_response("get_workflow", rid, err);
    if let Some(o) = output {
        resp.insert("output".into(), o);
    }
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ListStepsRequest {
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    load_output: bool,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn handle_list_steps(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ListStepsRequest = serde_json::from_str(text)?;
    let (output, err) = match engine.get_workflow_steps(&req.workflow_id).await {
        Ok(mut steps) => {
            // The engine returns all steps; apply the request's window in memory.
            let off = req.offset.unwrap_or(0).max(0) as usize;
            if off >= steps.len() {
                steps.clear();
            } else {
                steps.drain(0..off);
            }
            if let Some(lim) = req.limit {
                if lim >= 0 {
                    steps.truncate(lim as usize);
                }
            }
            if !req.load_output {
                for s in &mut steps {
                    s.output = None;
                }
            }
            (
                Some(steps.iter().map(format_step).collect::<Vec<_>>()),
                None,
            )
        }
        Err(e) => (None, Some(format!("failed to list workflow steps: {e}"))),
    };
    let mut resp = base_response("list_steps", rid, err);
    if let Some(o) = output {
        resp.insert("output".into(), json!(o));
    }
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ListSchedulesRequest {
    #[serde(default)]
    body: ListSchedulesBody,
}

#[derive(Default, Deserialize)]
struct ListSchedulesBody {
    #[serde(default)]
    status: StringOrList,
    #[serde(default)]
    workflow_name: StringOrList,
    #[serde(default)]
    schedule_name_prefix: StringOrList,
    load_context: Option<bool>,
}

async fn handle_list_schedules(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ListSchedulesRequest = serde_json::from_str(text)?;
    let load_context = req.body.load_context.unwrap_or(true);
    let filter = ScheduleFilter {
        statuses: req
            .body
            .status
            .vec()
            .iter()
            .map(|s| ScheduleStatus::parse(s))
            .collect(),
        workflow_names: req.body.workflow_name.vec(),
        name_prefixes: req.body.schedule_name_prefix.vec(),
    };
    let (output, err) = match engine.list_schedules(&filter).await {
        Ok(rows) => (
            rows.iter()
                .map(|s| format_schedule(s, load_context))
                .collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (vec![], Some(format!("failed to list schedules: {e}"))),
    };
    let mut resp = base_response("list_schedules", rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct GetScheduleRequest {
    #[serde(default)]
    schedule_name: String,
    load_context: Option<bool>,
}

async fn handle_get_schedule(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: GetScheduleRequest = serde_json::from_str(text)?;
    let load_context = req.load_context.unwrap_or(true);
    let (output, err) = match engine.get_schedule(&req.schedule_name).await {
        Ok(s) => (s.map(|s| format_schedule(&s, load_context)), None),
        Err(e) => (
            None,
            Some(format!(
                "failed to get schedule '{}': {e}",
                req.schedule_name
            )),
        ),
    };
    let mut resp = base_response("get_schedule", rid, err);
    // `output` is non-omitempty on the wire: null when the schedule is absent.
    resp.insert("output".into(), output.unwrap_or(Value::Null));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ScheduleNameRequest {
    #[serde(default)]
    schedule_name: String,
}

async fn handle_schedule_toggle(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
    pause: bool,
) -> Result<()> {
    let req: ScheduleNameRequest = serde_json::from_str(text)?;
    let (type_str, verb, result) = if pause {
        (
            "pause_schedule",
            "pause",
            engine.pause_schedule(&req.schedule_name).await,
        )
    } else {
        (
            "resume_schedule",
            "resume",
            engine.resume_schedule(&req.schedule_name).await,
        )
    };
    let err = result
        .err()
        .map(|e| format!("failed to {verb} schedule '{}': {e}", req.schedule_name));
    let mut resp = base_response(type_str, rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct BackfillRequest {
    #[serde(default)]
    schedule_name: String,
    #[serde(default)]
    start: String,
    #[serde(default)]
    end: String,
}

async fn handle_backfill_schedule(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: BackfillRequest = serde_json::from_str(text)?;
    let parse = |s: &str| DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc));
    let (ids, err) = match (parse(&req.start), parse(&req.end)) {
        (Err(e), _) => (
            vec![],
            Some(format!("failed to parse start time '{}': {e}", req.start)),
        ),
        (_, Err(e)) => (
            vec![],
            Some(format!("failed to parse end time '{}': {e}", req.end)),
        ),
        (Ok(start), Ok(end)) => match engine
            .backfill_schedule(&req.schedule_name, start, end)
            .await
        {
            Ok(ids) => (ids, None),
            Err(e) => (
                vec![],
                Some(format!(
                    "failed to backfill schedule '{}': {e}",
                    req.schedule_name
                )),
            ),
        },
    };
    let mut resp = base_response("backfill_schedule", rid, err);
    resp.insert("workflow_ids".into(), json!(ids));
    send(ws, resp).await
}

async fn handle_trigger_schedule(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ScheduleNameRequest = serde_json::from_str(text)?;
    let (id, err) = match engine.trigger_schedule::<Value>(&req.schedule_name).await {
        Ok(h) => (Some(h.id().to_string()), None),
        Err(e) => (
            None,
            Some(format!(
                "failed to trigger schedule '{}': {e}",
                req.schedule_name
            )),
        ),
    };
    let mut resp = base_response("trigger_schedule", rid, err);
    // `workflow_id` is non-omitempty on the wire: null on failure.
    resp.insert(
        "workflow_id".into(),
        id.map(Value::String).unwrap_or(Value::Null),
    );
    send(ws, resp).await
}

/// Render a [`WorkflowSchedule`] in the conductor's schedule shape. Nullable
/// fields are emitted as `null` (not omitted) to match the wire `*string` tags.
fn format_schedule(s: &WorkflowSchedule, load_context: bool) -> Value {
    let context = if load_context {
        s.context
            .as_ref()
            .map(|c| Value::String(c.to_string()))
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    json!({
        "schedule_id": s.schedule_id,
        "schedule_name": s.schedule_name,
        "workflow_name": s.workflow_name,
        "workflow_class_name": Value::Null, // no class concept in this SDK
        "schedule": s.schedule,
        "status": s.status.as_str(),
        "context": context,
        "last_fired_at": s.last_fired_at
            .map(|t| Value::String(t.to_rfc3339_opts(SecondsFormat::Nanos, true)))
            .unwrap_or(Value::Null),
        "automatic_backfill": s.automatic_backfill,
        "cron_timezone": s.cron_timezone.as_ref().map(|t| json!(t)).unwrap_or(Value::Null),
        "queue_name": s.queue_name.as_ref().map(|q| json!(q)).unwrap_or(Value::Null),
    })
}

async fn handle_list_queues(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
) -> Result<()> {
    // Read the database-backed registry (fleet-wide), not just this process's
    // in-memory queues — so the conductor sees every executor's queues.
    let (output, err) = match engine.list_queues().await {
        Ok(qs) => (qs.iter().map(format_queue).collect::<Vec<_>>(), None),
        Err(e) => (vec![], Some(format!("failed to list queues: {e}"))),
    };
    let mut resp = base_response("list_queues", rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct GetQueueRequest {
    #[serde(default)]
    name: String,
}

async fn handle_get_queue(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: GetQueueRequest = serde_json::from_str(text)?;
    let (output, err) = match engine.list_queues().await {
        Ok(qs) => (
            qs.iter()
                .find(|q| q.name == req.name)
                .map(format_queue)
                .unwrap_or(Value::Null),
            None,
        ),
        Err(e) => (Value::Null, Some(format!("failed to get queue: {e}"))),
    };
    let mut resp = base_response("get_queue", rid, err);
    resp.insert("output".into(), output);
    send(ws, resp).await
}

async fn handle_list_versions(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
) -> Result<()> {
    let (output, err) = match engine.list_application_versions().await {
        Ok(vs) => (vs.iter().map(format_version).collect::<Vec<_>>(), None),
        Err(e) => (
            vec![],
            Some(format!("failed to list application versions: {e}")),
        ),
    };
    let mut resp = base_response("list_application_versions", rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct SetLatestVersionRequest {
    #[serde(default)]
    version_name: String,
}

async fn handle_set_latest_version(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: SetLatestVersionRequest = serde_json::from_str(text)?;
    let err = engine
        .set_latest_application_version(&req.version_name)
        .await
        .err()
        .map(|e| {
            format!(
                "failed to set latest application version '{}': {e}",
                req.version_name
            )
        });
    let mut resp = base_response("set_latest_application_version", rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct WorkflowAggregatesRequest {
    #[serde(default)]
    body: WorkflowAggregatesBody,
}

#[derive(Default, Deserialize)]
struct WorkflowAggregatesBody {
    #[serde(default)]
    group_by_status: bool,
    #[serde(default)]
    group_by_name: bool,
    #[serde(default)]
    group_by_queue_name: bool,
    #[serde(default)]
    group_by_executor_id: bool,
    #[serde(default)]
    group_by_application_version: bool,
    #[serde(default)]
    select_count: bool,
    #[serde(default)]
    select_min_created_at: bool,
    #[serde(default)]
    select_max_queue_wait_ms: bool,
    #[serde(default)]
    select_max_total_latency_ms: bool,
    time_bucket_size_ms: Option<i64>,
    #[serde(default)]
    status: StringOrList,
    #[serde(default)]
    name: StringOrList,
    #[serde(default)]
    app_version: StringOrList,
    #[serde(default)]
    executor_id: StringOrList,
    #[serde(default)]
    queue_name: StringOrList,
    #[serde(default)]
    workflow_id_prefix: StringOrList,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    completed_after: Option<DateTime<Utc>>,
    completed_before: Option<DateTime<Utc>>,
    dequeued_after: Option<DateTime<Utc>>,
    dequeued_before: Option<DateTime<Utc>>,
}

async fn handle_workflow_aggregates(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: WorkflowAggregatesRequest = serde_json::from_str(text)?;
    let b = req.body;
    let mut query = WorkflowAggregateQuery {
        by_status: b.group_by_status,
        by_name: b.group_by_name,
        by_queue_name: b.group_by_queue_name,
        by_executor_id: b.group_by_executor_id,
        by_app_version: b.group_by_application_version,
        select_count: b.select_count,
        select_min_created_at: b.select_min_created_at,
        select_max_queue_wait_ms: b.select_max_queue_wait_ms,
        select_max_total_latency_ms: b.select_max_total_latency_ms,
        time_bucket_ms: b.time_bucket_size_ms,
        status: b.status.vec(),
        name: b.name.vec(),
        app_version: b.app_version.vec(),
        executor_ids: b.executor_id.vec(),
        queue_names: b.queue_name.vec(),
        workflow_id_prefix: b.workflow_id_prefix.first(),
        start_time_ms: b.start_time.map(|t| t.timestamp_millis()),
        end_time_ms: b.end_time.map(|t| t.timestamp_millis()),
        completed_after_ms: b.completed_after.map(|t| t.timestamp_millis()),
        completed_before_ms: b.completed_before.map(|t| t.timestamp_millis()),
        dequeued_after_ms: b.dequeued_after.map(|t| t.timestamp_millis()),
        dequeued_before_ms: b.dequeued_before.map(|t| t.timestamp_millis()),
        limit: None,
    };
    // Backwards compat: a count-only request omits every select_* flag; default
    // to count so it returns counts rather than being rejected (matches Go/Python).
    if query.no_select() {
        query.select_count = true;
    }
    let (output, err) = match engine.get_workflow_aggregates(&query).await {
        Ok(rows) => (
            rows.iter()
                .map(format_workflow_aggregate)
                .collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (
            vec![],
            Some(format!("failed to get workflow aggregates: {e}")),
        ),
    };
    let mut resp = base_response("get_workflow_aggregates", rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct StepAggregatesRequest {
    #[serde(default)]
    body: StepAggregatesBody,
}

#[derive(Default, Deserialize)]
struct StepAggregatesBody {
    #[serde(default)]
    group_by_function_name: bool,
    #[serde(default)]
    group_by_status: bool,
    #[serde(default)]
    select_count: bool,
    #[serde(default)]
    select_max_duration_ms: bool,
    time_bucket_size_ms: Option<i64>,
    #[serde(default)]
    status: StringOrList,
    #[serde(default)]
    function_name: StringOrList,
    #[serde(default)]
    workflow_id_prefix: StringOrList,
    completed_after: Option<DateTime<Utc>>,
    completed_before: Option<DateTime<Utc>>,
}

async fn handle_step_aggregates(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: StepAggregatesRequest = serde_json::from_str(text)?;
    let b = req.body;
    let mut query = StepAggregateQuery {
        by_function_name: b.group_by_function_name,
        by_status: b.group_by_status,
        select_count: b.select_count,
        select_max_duration_ms: b.select_max_duration_ms,
        time_bucket_ms: b.time_bucket_size_ms,
        status: b.status.vec(),
        function_name: b.function_name.vec(),
        workflow_id_prefix: b.workflow_id_prefix.first(),
        completed_after_ms: b.completed_after.map(|t| t.timestamp_millis()),
        completed_before_ms: b.completed_before.map(|t| t.timestamp_millis()),
        limit: None,
    };
    // Backwards compat: a count-only request omits every select_* flag; default
    // to count so it returns counts rather than being rejected (matches Go/Python).
    if query.no_select() {
        query.select_count = true;
    }
    let (output, err) = match engine.get_step_aggregates(&query).await {
        Ok(rows) => (
            rows.iter().map(format_step_aggregate).collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (vec![], Some(format!("failed to get step aggregates: {e}"))),
    };
    let mut resp = base_response("get_step_aggregates", rid, err);
    resp.insert("output".into(), json!(output));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct WorkflowIdRequest {
    #[serde(default)]
    workflow_id: String,
}

async fn handle_get_events(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: WorkflowIdRequest = serde_json::from_str(text)?;
    let (events, err) = match engine.list_workflow_events(&req.workflow_id).await {
        Ok(evs) => (
            evs.iter()
                .map(|(k, v)| json!({ "key": k, "value": v.to_string() }))
                .collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (vec![], Some(format!("failed to get workflow events: {e}"))),
    };
    let mut resp = base_response("get_workflow_events", rid, err);
    resp.insert("events".into(), json!(events));
    send(ws, resp).await
}

async fn handle_get_notifications(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: WorkflowIdRequest = serde_json::from_str(text)?;
    let (notifications, err) = match engine.list_workflow_notifications(&req.workflow_id).await {
        Ok(rows) => (
            rows.iter()
                .map(|n| {
                    json!({
                        "topic": n.topic,
                        "message": n.message.to_string(),
                        "created_at_epoch_ms": n.created_at_ms,
                        "consumed": n.consumed,
                    })
                })
                .collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (
            vec![],
            Some(format!("failed to get workflow notifications: {e}")),
        ),
    };
    let mut resp = base_response("get_workflow_notifications", rid, err);
    resp.insert("notifications".into(), json!(notifications));
    send(ws, resp).await
}

async fn handle_get_streams(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: WorkflowIdRequest = serde_json::from_str(text)?;
    let (streams, err) = match engine.list_workflow_streams(&req.workflow_id).await {
        Ok(rows) => (
            rows.iter()
                .map(|(key, vals)| {
                    json!({
                        "key": key,
                        "values": vals.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>(),
            None,
        ),
        Err(e) => (vec![], Some(format!("failed to get workflow streams: {e}"))),
    };
    let mut resp = base_response("get_workflow_streams", rid, err);
    resp.insert("streams".into(), json!(streams));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct GetMetricsRequest {
    #[serde(default)]
    start_time: String,
    #[serde(default)]
    end_time: String,
    #[serde(default)]
    metric_class: String,
}

async fn handle_get_metrics(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: GetMetricsRequest = serde_json::from_str(text)?;
    // The only metric class the conductor asks for; mirror Go's rejection.
    if req.metric_class != "workflow_step_count" {
        let mut resp = base_response(
            "get_metrics",
            rid,
            Some(format!("Unexpected metric class: {}", req.metric_class)),
        );
        resp.insert("metrics".into(), Value::Null);
        return send(ws, resp).await;
    }
    let parse = |s: &str| DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc));
    let (start, end) = match (parse(&req.start_time), parse(&req.end_time)) {
        (Ok(s), Ok(e)) => (s.timestamp_millis(), e.timestamp_millis()),
        (Err(e), _) => {
            let mut resp =
                base_response("get_metrics", rid, Some(format!("invalid start_time: {e}")));
            resp.insert("metrics".into(), Value::Null);
            return send(ws, resp).await;
        }
        (_, Err(e)) => {
            let mut resp =
                base_response("get_metrics", rid, Some(format!("invalid end_time: {e}")));
            resp.insert("metrics".into(), Value::Null);
            return send(ws, resp).await;
        }
    };

    // workflow_count grouped by name, step_count grouped by function name —
    // both over the [start, end) window (workflows by created_at, steps by
    // completed_at), matching the reference metric queries.
    let wq = WorkflowAggregateQuery {
        by_name: true,
        select_count: true,
        start_time_ms: Some(start),
        end_time_ms: Some(end),
        ..Default::default()
    };
    let sq = StepAggregateQuery {
        by_function_name: true,
        select_count: true,
        completed_after_ms: Some(start),
        completed_before_ms: Some(end),
        ..Default::default()
    };

    let mut metrics: Vec<Value> = Vec::new();
    let mut err = None;
    match engine.get_workflow_aggregates(&wq).await {
        Ok(rows) => {
            for r in rows {
                if let (Some(Some(name)), Some(count)) = (r.group.get("name"), r.count) {
                    metrics.push(metric("workflow_count", name, count as f64));
                }
            }
        }
        Err(e) => err = Some(format!("Exception encountered when getting metrics: {e}")),
    }
    if err.is_none() {
        match engine.get_step_aggregates(&sq).await {
            Ok(rows) => {
                for r in rows {
                    if let (Some(Some(name)), Some(count)) = (r.group.get("function_name"), r.count)
                    {
                        metrics.push(metric("step_count", name, count as f64));
                    }
                }
            }
            Err(e) => err = Some(format!("Exception encountered when getting metrics: {e}")),
        }
    }

    let mut resp = base_response("get_metrics", rid, err.clone());
    resp.insert(
        "metrics".into(),
        if err.is_some() {
            Value::Null
        } else {
            json!(metrics)
        },
    );
    send(ws, resp).await
}

/// One `metricData` entry.
fn metric(metric_type: &str, name: &str, value: f64) -> Value {
    json!({ "metric_name": name, "metric_type": metric_type, "value": value })
}

#[derive(Deserialize)]
struct RetentionRequest {
    #[serde(default)]
    body: RetentionBody,
}

#[derive(Default, Deserialize)]
struct RetentionBody {
    gc_cutoff_epoch_ms: Option<i64>,
    gc_rows_threshold: Option<i64>,
    timeout_cutoff_epoch_ms: Option<i64>,
}

async fn handle_retention(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: RetentionRequest = serde_json::from_str(text)?;
    // The retention policy has two independent halves, applied in order:
    // garbage-collect old history first, then time out anything still pending
    // before the timeout cutoff — the second only runs if the first succeeded.
    let mut err = None;
    if req.body.gc_cutoff_epoch_ms.is_some() || req.body.gc_rows_threshold.is_some() {
        err = engine
            .garbage_collect(req.body.gc_cutoff_epoch_ms, req.body.gc_rows_threshold)
            .await
            .err()
            .map(|e| format!("failed to garbage collect workflows: {e}"));
    }
    if err.is_none() {
        if let Some(cutoff) = req.body.timeout_cutoff_epoch_ms {
            err = engine
                .cancel_all_before(cutoff)
                .await
                .err()
                .map(|e| format!("failed to timeout workflows: {e}"));
        }
    }
    let mut resp = base_response("retention", rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ExportWorkflowRequest {
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    export_children: bool,
}

/// Export a workflow (and optionally its children) and reply with the portable
/// payload as gzipped, base64-encoded JSON under `serialized_workflow` (omitted
/// on failure). The encoding matches the other SDKs so the payload is portable.
async fn handle_export_workflow(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ExportWorkflowRequest = serde_json::from_str(text)?;
    let (serialized, err) = match engine
        .export_workflow(&req.workflow_id, req.export_children)
        .await
    {
        Ok(exported) => match encode_export(&exported) {
            Ok(s) => (Some(s), None),
            Err(e) => (
                None,
                Some(format!("failed to serialize exported workflow: {e}")),
            ),
        },
        Err(e) => (
            None,
            Some(format!(
                "Exception encountered when exporting workflow {}: {e}",
                req.workflow_id
            )),
        ),
    };
    let mut resp = base_response("export_workflow", rid, err);
    if let Some(s) = serialized {
        resp.insert("serialized_workflow".into(), json!(s));
    }
    send(ws, resp).await
}

#[derive(Deserialize)]
struct ImportWorkflowRequest {
    #[serde(default)]
    serialized_workflow: String,
}

/// Decode a `serialized_workflow` payload (base64 → gunzip → JSON) and import the
/// workflows it carries, replying with `{success}`.
async fn handle_import_workflow(
    engine: &Arc<DurableEngine>,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: ImportWorkflowRequest = serde_json::from_str(text)?;
    let err = match decode_export(&req.serialized_workflow) {
        Ok(workflows) => engine
            .import_workflow(&workflows)
            .await
            .err()
            .map(|e| format!("Exception encountered when importing workflow: {e}")),
        Err(e) => Some(e),
    };
    let mut resp = base_response("import_workflow", rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

/// Serialize exported workflows to the portable wire form: JSON → gzip → base64.
fn encode_export(exported: &[ExportedWorkflow]) -> Result<String> {
    let json = serde_json::to_vec(exported)?;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(&json)
        .map_err(|e| Error::app(format!("failed to gzip exported workflow: {e}")))?;
    let bytes = gz
        .finish()
        .map_err(|e| Error::app(format!("failed to finish gzip: {e}")))?;
    Ok(STANDARD.encode(bytes))
}

/// Reverse [`encode_export`]: base64 → gunzip → JSON. Errors are returned as the
/// conductor-facing message strings (mirroring each failure stage).
fn decode_export(serialized: &str) -> std::result::Result<Vec<ExportedWorkflow>, String> {
    let compressed = STANDARD
        .decode(serialized)
        .map_err(|e| format!("Failed to base64 decode serialized workflow: {e}"))?;
    let mut json = Vec::new();
    GzDecoder::new(&compressed[..])
        .read_to_end(&mut json)
        .map_err(|e| format!("Failed to decompress workflow data: {e}"))?;
    serde_json::from_slice(&json).map_err(|e| format!("Failed to unmarshal workflow data: {e}"))
}

#[derive(Deserialize)]
struct AlertRequest {
    #[serde(default)]
    name: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

/// Deliver an `alert` to the registered handler, if any. The handler is a
/// user callback, so we run it inside [`catch_unwind`](std::panic::catch_unwind):
/// a panic is reported back as a failure (mirroring the conductor's contract)
/// instead of unwinding through the connection task. With no handler registered,
/// the alert is logged and acknowledged as success.
async fn handle_alert(
    config: &ConductorConfig,
    ws: &mut WsStream,
    rid: &str,
    text: &str,
) -> Result<()> {
    let req: AlertRequest = serde_json::from_str(text)?;
    let err = match &config.alert_handler {
        Some(handler) => {
            let handler = handler.clone();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                handler(&req.name, &req.message, &req.metadata);
            }))
            .err()
            .map(|p| format!("panic in alert handler: {}", panic_detail(p.as_ref())))
        }
        None => {
            tracing::info!(
                name = %req.name,
                message = %req.message,
                "alert received (no handler registered)"
            );
            None
        }
    };
    let mut resp = base_response("alert", rid, err.clone());
    resp.insert("success".into(), json!(err.is_none()));
    send(ws, resp).await
}

/// Best-effort string for a caught panic payload (`&str` or `String`).
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown".to_string()
    }
}

/// Render a [`WorkflowQueue`] in the conductor's queue shape (snake_case;
/// nullable fields emitted as `null`).
fn format_queue(q: &WorkflowQueue) -> Value {
    json!({
        "name": q.name,
        "concurrency": q.global_concurrency,
        "worker_concurrency": q.worker_concurrency,
        "rate_limit_max": q.rate_limit.as_ref().map(|r| r.limit),
        "rate_limit_period_sec": q.rate_limit.as_ref().map(|r| r.period.as_secs_f64()),
        "priority_enabled": q.priority_enabled,
        "partition_queue": q.partitioned,
        "polling_interval_sec": q.base_polling_interval.as_secs_f64(),
    })
}

/// Render a [`VersionInfo`] in the conductor's application-version shape
/// (epoch-ms integers).
fn format_version(v: &VersionInfo) -> Value {
    json!({
        "version_id": v.version_id,
        "version_name": v.version_name,
        "version_timestamp": v.version_timestamp.timestamp_millis(),
        "created_at": v.created_at.timestamp_millis(),
    })
}

/// Render a [`WorkflowAggregate`]. Each aggregate is emitted only when the query
/// selected it; unselected aggregates are `null` (matching the other SDKs).
fn format_workflow_aggregate(a: &WorkflowAggregate) -> Value {
    json!({
        "group": a.group,
        "count": a.count,
        "min_created_at": a.min_created_at,
        "max_queue_wait_ms": a.max_queue_wait_ms,
        "max_total_latency_ms": a.max_total_latency_ms,
    })
}

/// Render a [`StepAggregate`] (matches the wire shape one-to-one).
fn format_step_aggregate(a: &StepAggregate) -> Value {
    json!({
        "group": a.group,
        "count": a.count,
        "max_duration_ms": a.max_duration_ms,
    })
}

/// Deserialize an explicit JSON `null` as the type's default. The conductor
/// service marshals absent lists as `null` (a Go nil slice), which
/// `#[serde(default)]` alone does not cover — it only handles a *missing*
/// field, and a bare `Vec<String>` rejects `null` outright. serde has no
/// built-in for this; this helper is the canonical pattern its maintainers
/// recommend (serde-rs/serde#1098) — the packaged equivalent
/// (`serde_with::DefaultOnNull`) is not worth a new dependency here.
fn null_default<'de, D, T>(d: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// A conductor filter field that accepts either a single string or an array of
/// strings (the wire `StringOrList`).
#[derive(Default)]
struct StringOrList(Vec<String>);

impl<'de> Deserialize<'de> for StringOrList {
    fn deserialize<D>(d: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(String),
            Many(Vec<String>),
        }
        Ok(match Option::<OneOrMany>::deserialize(d)? {
            None => StringOrList(Vec::new()),
            Some(OneOrMany::One(s)) => StringOrList(vec![s]),
            Some(OneOrMany::Many(v)) => StringOrList(v),
        })
    }
}

impl StringOrList {
    /// First value, for the single-valued list filters this SDK supports.
    fn first(&self) -> Option<String> {
        self.0.first().cloned()
    }
    fn vec(&self) -> Vec<String> {
        self.0.clone()
    }
}

/// `{ "body": { …filters… } }` for `list_workflows` / `list_queued_workflows`.
#[derive(Deserialize)]
struct ListRequest {
    #[serde(default)]
    body: ListBody,
}

#[derive(Default, Deserialize)]
struct ListBody {
    #[serde(default, deserialize_with = "null_default")]
    workflow_uuids: Vec<String>,
    #[serde(default)]
    workflow_name: StringOrList,
    #[serde(default)]
    status: StringOrList,
    #[serde(default)]
    application_version: StringOrList,
    #[serde(default)]
    executor_id: StringOrList,
    #[serde(default)]
    authenticated_user: StringOrList,
    #[serde(default)]
    forked_from: StringOrList,
    #[serde(default)]
    parent_workflow_id: StringOrList,
    #[serde(default)]
    queue_name: StringOrList,
    #[serde(default)]
    workflow_id_prefix: StringOrList,
    has_parent: Option<bool>,
    was_forked_from: Option<bool>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    completed_after: Option<DateTime<Utc>>,
    completed_before: Option<DateTime<Utc>>,
    dequeued_after: Option<DateTime<Utc>>,
    dequeued_before: Option<DateTime<Utc>>,
    limit: Option<i64>,
    offset: Option<i64>,
    #[serde(default)]
    sort_desc: bool,
    #[serde(default)]
    load_input: bool,
    #[serde(default)]
    load_output: bool,
    #[serde(default)]
    queues_only: bool,
}

impl ListBody {
    fn to_filter(&self, force_queued: bool) -> ListFilter {
        let mut f = ListFilter {
            workflow_ids: self.workflow_uuids.clone(),
            name: self.workflow_name.vec(),
            status: self.status.vec(),
            app_version: self.application_version.vec(),
            executor_ids: self.executor_id.vec(),
            authenticated_users: self.authenticated_user.vec(),
            forked_from: self.forked_from.vec(),
            parent_workflow_ids: self.parent_workflow_id.vec(),
            was_forked_from: self.was_forked_from,
            queue_name: self.queue_name.vec(),
            workflow_id_prefix: self.workflow_id_prefix.vec(),
            has_parent: self.has_parent,
            start_time_ms: self.start_time.map(|t| t.timestamp_millis()),
            end_time_ms: self.end_time.map(|t| t.timestamp_millis()),
            completed_after_ms: self.completed_after.map(|t| t.timestamp_millis()),
            completed_before_ms: self.completed_before.map(|t| t.timestamp_millis()),
            dequeued_after_ms: self.dequeued_after.map(|t| t.timestamp_millis()),
            dequeued_before_ms: self.dequeued_before.map(|t| t.timestamp_millis()),
            limit: self.limit,
            offset: self.offset,
            sort_desc: self.sort_desc,
            load_input: self.load_input,
            load_output: self.load_output,
            queues_only: self.queues_only,
        };
        if force_queued {
            f.queues_only = true;
            f.load_output = false; // queued listings never carry output
            if f.status.is_empty() {
                f.status = vec![
                    STATUS_PENDING.to_string(),
                    STATUS_ENQUEUED.to_string(),
                    STATUS_DELAYED.to_string(),
                ];
            }
        }
        f
    }
}

/// Render an epoch-ms instant as the stringified-millis the conductor expects.
fn epoch_ms_str(ms: i64) -> Value {
    Value::String(ms.to_string())
}

/// A stored payload's compact JSON string, if present and non-null.
fn payload(v: Option<&Value>) -> Option<String> {
    match v {
        Some(v) if !v.is_null() => Some(v.to_string()),
        _ => None,
    }
}

/// Render a [`WorkflowStatus`] in the conductor's list/get response shape — the
/// PascalCase keys with stringified-epoch-ms times, every field `omitempty`.
fn format_list_workflow(ws: &WorkflowStatus) -> Value {
    let mut m = Map::new();
    m.insert("WorkflowUUID".into(), json!(ws.id));
    if !ws.status.is_empty() {
        m.insert("Status".into(), json!(ws.status));
    }
    if !ws.name.is_empty() {
        m.insert("WorkflowName".into(), json!(ws.name));
    }
    if let Some(u) = &ws.authenticated_user {
        m.insert("AuthenticatedUser".into(), json!(u));
    }
    if let Some(r) = &ws.assumed_role {
        m.insert("AssumedRole".into(), json!(r));
    }
    if !ws.authenticated_roles.is_empty() {
        let roles = serde_json::to_string(&ws.authenticated_roles).unwrap_or_default();
        m.insert("AuthenticatedRoles".into(), json!(roles));
    }
    if let Some(i) = payload(Some(&ws.input)) {
        m.insert("Input".into(), json!(i));
    }
    if let Some(o) = payload(ws.output.as_ref()) {
        m.insert("Output".into(), json!(o));
    }
    if let Some(e) = &ws.error {
        m.insert("Error".into(), json!(e));
    }
    m.insert(
        "CreatedAt".into(),
        epoch_ms_str(ws.created_at.timestamp_millis()),
    );
    m.insert(
        "UpdatedAt".into(),
        epoch_ms_str(ws.updated_at.timestamp_millis()),
    );
    if let Some(q) = &ws.queue_name {
        m.insert("QueueName".into(), json!(q));
    }
    if let Some(k) = &ws.queue_partition_key {
        m.insert("QueuePartitionKey".into(), json!(k));
    }
    if let Some(d) = &ws.dedup_id {
        m.insert("DeduplicationID".into(), json!(d));
    }
    m.insert("Priority".into(), json!(ws.priority.to_string()));
    if !ws.app_version.is_empty() {
        m.insert("ApplicationVersion".into(), json!(ws.app_version));
    }
    if !ws.executor_id.is_empty() {
        m.insert("ExecutorID".into(), json!(ws.executor_id));
    }
    if let Some(t) = ws.timeout_ms {
        if t > 0 {
            m.insert("WorkflowTimeoutMS".into(), epoch_ms_str(t));
        }
    }
    if let Some(d) = ws.deadline_ms {
        m.insert("WorkflowDeadlineEpochMS".into(), epoch_ms_str(d));
    }
    if let Some(f) = &ws.forked_from {
        m.insert("ForkedFrom".into(), json!(f));
    }
    m.insert("WasForkedFrom".into(), json!(ws.forked_from.is_some()));
    if let Some(p) = &ws.parent_workflow_id {
        m.insert("ParentWorkflowID".into(), json!(p));
    }
    // A dequeued workflow (PENDING with a start time) reports when it started.
    if ws.status == STATUS_PENDING {
        if let Some(s) = ws.started_at_ms {
            m.insert("DequeuedAt".into(), epoch_ms_str(s));
        }
    }
    if let Some(d) = ws.delay_until_ms {
        m.insert("DelayUntilEpochMS".into(), epoch_ms_str(d));
    }
    if let Some(c) = ws.completed_at_ms {
        m.insert("CompletedAt".into(), epoch_ms_str(c));
    }
    Value::Object(m)
}

/// Render a [`StepInfo`] in the conductor's step response shape (snake_case).
fn format_step(s: &StepInfo) -> Value {
    let mut m = Map::new();
    m.insert("function_id".into(), json!(s.step_id));
    m.insert("function_name".into(), json!(s.name));
    if let Some(o) = payload(s.output.as_ref()) {
        m.insert("output".into(), json!(o));
    }
    if let Some(e) = &s.error {
        m.insert("error".into(), json!(e));
    }
    if let Some(c) = &s.child_workflow_id {
        m.insert("child_workflow_id".into(), json!(c));
    }
    if let Some(t) = s.started_at {
        m.insert(
            "started_at_epoch_ms".into(),
            epoch_ms_str(t.timestamp_millis()),
        );
    }
    if let Some(t) = s.completed_at {
        m.insert(
            "completed_at_epoch_ms".into(),
            epoch_ms_str(t.timestamp_millis()),
        );
    }
    Value::Object(m)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// One test fn covers both the default and the `DBOS_DOMAIN` override:
    /// they mutate process env, so they must not run as parallel tests.
    #[test]
    fn websocket_url_defaults_to_the_hosted_conductor() {
        let config = |url: &str| ConductorConfig {
            url: url.into(),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        };

        std::env::remove_var("DBOS_DOMAIN");
        assert_eq!(
            websocket_url(&config("")),
            "wss://cloud.dbos.dev/conductor/v1alpha1/websocket/app/k"
        );

        std::env::set_var("DBOS_DOMAIN", "example.test");
        assert_eq!(
            websocket_url(&config("")),
            "wss://example.test/conductor/v1alpha1/websocket/app/k"
        );

        // Set-but-empty counts as unset (Go's rule), not a hostless URL.
        std::env::set_var("DBOS_DOMAIN", "");
        assert_eq!(
            websocket_url(&config("")),
            "wss://cloud.dbos.dev/conductor/v1alpha1/websocket/app/k"
        );
        std::env::remove_var("DBOS_DOMAIN");

        // An explicit base is used verbatim, trailing slash trimmed.
        assert_eq!(
            websocket_url(&config("ws://127.0.0.1:9000/")),
            "ws://127.0.0.1:9000/websocket/app/k"
        );
    }
}
