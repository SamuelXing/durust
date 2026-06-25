//! Conductor client — the connection to the DBOS cloud control plane.
//!
//! A long-lived websocket *client* that dials the conductor service, then
//! answers the requests it pushes (executor info, recovery, workflow and
//! schedule management, …). The connection self-heals: it pings to keep the
//! link alive and reconnects with exponential backoff after a drop.
//!
//! Implemented so far: the executor-lifecycle handlers (`executor_info`,
//! `recovery`, `exist_pending_workflows`) and workflow management/queries
//! (`cancel`, `resume`, `delete`, `fork_workflow`, `list_workflows`,
//! `list_queued_workflows`, `get_workflow`, `list_steps`). Every other message
//! type is answered with a well-formed "unknown message type" error until its
//! handler lands, so the link stays healthy as coverage grows.
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

use crate::engine::{DurableEngine, WorkflowOptions};
use crate::error::{Error, Result};
use crate::provider::{
    ListFilter, StepInfo, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_PENDING,
};
use chrono::{DateTime, Utc};
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
// How long to drive the closing handshake (drain the peer's Close echo) before
// giving up and dropping the connection.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

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
        "cancel" => handle_ids(engine, ws, rid, text, IdsAction::Cancel).await,
        "resume" => handle_ids(engine, ws, rid, text, IdsAction::Resume).await,
        "delete" => handle_ids(engine, ws, rid, text, IdsAction::Delete).await,
        "fork_workflow" => handle_fork(engine, ws, rid, text).await,
        "list_workflows" => handle_list(engine, ws, rid, text, false).await,
        "list_queued_workflows" => handle_list(engine, ws, rid, text, true).await,
        "get_workflow" => handle_get_workflow(engine, ws, rid, text).await,
        "list_steps" => handle_list_steps(engine, ws, rid, text).await,
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
    #[serde(default)]
    workflow_ids: Vec<String>,
    #[serde(default)]
    delete_children: bool,
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
            engine.resume_workflows::<Value>(&ids).await.map(|_| ()),
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
    #[serde(default)]
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
    forked_from: StringOrList,
    #[serde(default)]
    queue_name: StringOrList,
    #[serde(default)]
    workflow_id_prefix: StringOrList,
    has_parent: Option<bool>,
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
            name: self.workflow_name.first(),
            status: self.status.vec(),
            app_version: self.application_version.first(),
            executor_ids: self.executor_id.vec(),
            forked_from: self.forked_from.first(),
            queue_name: self.queue_name.first(),
            workflow_id_prefix: self.workflow_id_prefix.first(),
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
