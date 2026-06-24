//! Admin HTTP control surface.
//!
//! An opt-in HTTP server exposing health, recovery, and workflow-management
//! endpoints over a running [`DurableEngine`]. The route set and JSON shapes
//! match the other DBOS SDKs so the same tooling (the DBOS console, the
//! conductor, `curl` health probes) works against a Rust process.
//!
//! ```no_run
//! # use durust::{DurableEngine, InMemoryProvider, AdminServer};
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # async fn run() -> durust::Result<()> {
//! let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);
//! engine.launch().await?;
//! let admin = AdminServer::start(engine.clone(), 3001).await?; // 0 = ephemeral port
//! // ... serve requests ...
//! admin.shutdown(Duration::from_secs(5)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! The endpoint set (verbatim paths, for cross-SDK tooling):
//! - `GET  /dbos-healthz` — liveness
//! - `GET  /deactivate` — stop claiming new work, keep serving
//! - `GET  /conductor` — conductor handshake placeholder
//! - `GET  /dbos-workflow-queues-metadata` — registered-queue config
//! - `POST /dbos-workflow-recovery` — recover a set of executors' pending work
//! - `POST /dbos-global-timeout` — cancel everything created before a cutoff
//! - `POST /dbos-garbage-collect` — reserved (no-op, matching the reference)
//! - `POST /workflows` — list workflows (JSON filter body)
//! - `POST /queues` — list queued workflows
//! - `GET  /workflows/{id}` — one workflow
//! - `GET  /workflows/{id}/steps` — its steps
//! - `POST /workflows/{id}/{cancel,resume,fork}` — management

use crate::engine::{DurableEngine, WorkflowOptions};
use crate::error::{Error, Result};
use crate::provider::{
    ListFilter, StepInfo, WorkflowStatus, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_PENDING,
};
use crate::queue::WorkflowQueue;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A running admin HTTP server. Drop or [`shutdown`](Self::shutdown) to stop it.
pub struct AdminServer {
    port: u16,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl AdminServer {
    /// Bind and start the admin server on `port` (use `0` for an OS-assigned
    /// ephemeral port — read it back with [`port`](Self::port)). The server runs
    /// on its own task until [`shutdown`](Self::shutdown) is called.
    pub async fn start(engine: Arc<DurableEngine>, port: u16) -> Result<AdminServer> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .map_err(|e| Error::app(format!("admin server bind failed: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| Error::app(format!("admin server addr failed: {e}")))?
            .port();
        let app = router(engine);
        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let shutdown = async {
                let _ = rx.await;
            };
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
            {
                tracing::error!(error = %e, "admin server error");
            }
        });
        tracing::info!(port, "admin server started");
        Ok(AdminServer {
            port,
            shutdown: Some(tx),
            task,
        })
    }

    /// The port the server is actually listening on (resolved even when started
    /// with port `0`).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Signal a graceful shutdown and wait up to `timeout` for the server task to
    /// finish.
    pub async fn shutdown(mut self, timeout: Duration) -> Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = tokio::time::timeout(timeout, &mut self.task).await;
        Ok(())
    }
}

/// Build the admin router over a shared engine.
fn router(engine: Arc<DurableEngine>) -> Router {
    Router::new()
        .route("/dbos-healthz", get(health))
        .route("/deactivate", get(deactivate))
        .route("/conductor", get(conductor))
        .route("/dbos-workflow-queues-metadata", get(queues_metadata))
        .route("/dbos-workflow-recovery", post(workflow_recovery))
        .route("/dbos-global-timeout", post(global_timeout))
        .route("/dbos-garbage-collect", post(garbage_collect))
        .route("/workflows", post(list_workflows))
        .route("/queues", post(list_queued_workflows))
        .route("/workflows/:id", get(get_workflow))
        .route("/workflows/:id/steps", get(workflow_steps))
        .route("/workflows/:id/cancel", post(cancel_workflow))
        .route("/workflows/:id/resume", post(resume_workflow))
        .route("/workflows/:id/fork", post(fork_workflow))
        .with_state(engine)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

type ApiResult = std::result::Result<axum::response::Response, (StatusCode, String)>;

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "healthy" }))
}

async fn conductor() -> impl IntoResponse {
    Json(json!({ "status": true }))
}

async fn deactivate(State(engine): State<Arc<DurableEngine>>) -> impl IntoResponse {
    engine.deactivate();
    (StatusCode::OK, "deactivated")
}

async fn queues_metadata(State(engine): State<Arc<DurableEngine>>) -> impl IntoResponse {
    let meta: Vec<Value> = engine
        .list_registered_queues()
        .iter()
        .map(to_queue_metadata)
        .collect();
    Json(meta)
}

async fn workflow_recovery(State(engine): State<Arc<DurableEngine>>, body: Bytes) -> ApiResult {
    let executor_ids: Vec<String> = serde_json::from_slice(&body)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid JSON body".to_string()))?;
    let ids = engine
        .recover_pending_for(&executor_ids)
        .await
        .map_err(internal)?;
    Ok(Json(ids).into_response())
}

#[derive(Deserialize)]
struct GlobalTimeoutRequest {
    cutoff_epoch_timestamp_ms: i64,
}

async fn global_timeout(State(engine): State<Arc<DurableEngine>>, body: Bytes) -> ApiResult {
    let req: GlobalTimeoutRequest = serde_json::from_slice(&body)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid JSON body".to_string()))?;
    engine
        .cancel_all_before(req.cutoff_epoch_timestamp_ms)
        .await
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Default, Deserialize)]
struct GarbageCollectRequest {
    #[allow(dead_code)]
    cutoff_epoch_timestamp_ms: Option<i64>,
    #[allow(dead_code)]
    rows_threshold: Option<i64>,
}

async fn garbage_collect(body: Bytes) -> ApiResult {
    // Reserved: parse the body for shape-compatibility, but garbage collection
    // is not yet implemented (matching the reference SDK's no-op endpoint).
    let _req: GarbageCollectRequest = parse_optional(&body)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn list_workflows(State(engine): State<Arc<DurableEngine>>, body: Bytes) -> ApiResult {
    let req: ListWorkflowsRequest = parse_optional(&body)?;
    let workflows = engine
        .list_workflows(&req.to_filter())
        .await
        .map_err(internal)?;
    let out: Vec<Value> = workflows.iter().map(to_list_workflow_response).collect();
    Ok(Json(out).into_response())
}

async fn list_queued_workflows(State(engine): State<Arc<DurableEngine>>, body: Bytes) -> ApiResult {
    let req: ListWorkflowsRequest = parse_optional(&body)?;
    let mut filter = req.to_filter();
    if filter.status.is_empty() {
        filter.status = vec![
            STATUS_ENQUEUED.to_string(),
            STATUS_PENDING.to_string(),
            STATUS_DELAYED.to_string(),
        ];
    }
    filter.queues_only = true;
    let workflows = engine.list_workflows(&filter).await.map_err(internal)?;
    let out: Vec<Value> = workflows.iter().map(to_list_workflow_response).collect();
    Ok(Json(out).into_response())
}

async fn get_workflow(
    State(engine): State<Arc<DurableEngine>>,
    Path(id): Path<String>,
) -> ApiResult {
    let filter = ListFilter {
        workflow_ids: vec![id],
        ..Default::default()
    };
    let workflows = engine.list_workflows(&filter).await.map_err(internal)?;
    match workflows.first() {
        Some(ws) => Ok(Json(to_list_workflow_response(ws)).into_response()),
        None => Err((StatusCode::NOT_FOUND, "Workflow not found".to_string())),
    }
}

async fn workflow_steps(
    State(engine): State<Arc<DurableEngine>>,
    Path(id): Path<String>,
) -> ApiResult {
    let steps = engine.get_workflow_steps(&id).await.map_err(internal)?;
    let out: Vec<Value> = steps.iter().map(to_step_response).collect();
    Ok(Json(out).into_response())
}

async fn cancel_workflow(
    State(engine): State<Arc<DurableEngine>>,
    Path(id): Path<String>,
) -> ApiResult {
    engine.cancel_workflow(&id).await.map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn resume_workflow(
    State(engine): State<Arc<DurableEngine>>,
    Path(id): Path<String>,
) -> ApiResult {
    engine
        .resume_workflow::<Value>(&id)
        .await
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Default, Deserialize)]
struct ForkRequest {
    start_step: Option<i32>,
    new_workflow_id: Option<String>,
    application_version: Option<String>,
}

async fn fork_workflow(
    State(engine): State<Arc<DurableEngine>>,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult {
    let req: ForkRequest = parse_optional(&body)?;
    let mut opts = match req.new_workflow_id {
        Some(new_id) => WorkflowOptions::with_id(new_id),
        None => WorkflowOptions::default(),
    };
    if let Some(v) = req.application_version {
        opts = opts.app_version(v);
    }
    let handle = engine
        .fork_workflow::<Value>(&id, req.start_step.unwrap_or(0), opts)
        .await
        .map_err(internal)?;
    Ok(Json(json!({ "workflow_id": handle.id() })).into_response())
}

// ---------------------------------------------------------------------------
// Request parsing
// ---------------------------------------------------------------------------

/// Parse a JSON request body, tolerating an empty body as the type's default
/// (matching the reference servers, which only decode when `Content-Length > 0`).
fn parse_optional<T: serde::de::DeserializeOwned + Default>(
    body: &Bytes,
) -> std::result::Result<T, (StatusCode, String)> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON input: {e}")))
}

/// The list-workflows filter body shared by `POST /workflows` and `POST /queues`.
/// Field names are the cross-SDK wire names (snake_case).
#[derive(Default, Deserialize)]
struct ListWorkflowsRequest {
    workflow_uuids: Option<Vec<String>>,
    // `authenticated_user` is accepted for wire-compatibility but not yet a
    // supported list filter (no provider column filter); ignored for now.
    #[allow(dead_code)]
    authenticated_user: Option<String>,
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    end_time: Option<chrono::DateTime<chrono::Utc>>,
    status: Option<String>,
    application_version: Option<String>,
    workflow_name: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    sort_desc: Option<bool>,
    workflow_id_prefix: Option<String>,
    load_input: Option<bool>,
    load_output: Option<bool>,
    queue_name: Option<String>,
}

impl ListWorkflowsRequest {
    fn to_filter(&self) -> ListFilter {
        let mut f = ListFilter {
            workflow_ids: self.workflow_uuids.clone().unwrap_or_default(),
            workflow_id_prefix: self.workflow_id_prefix.clone(),
            name: self.workflow_name.clone(),
            app_version: self.application_version.clone(),
            queue_name: self.queue_name.clone(),
            start_time_ms: self.start_time.map(|t| t.timestamp_millis()),
            end_time_ms: self.end_time.map(|t| t.timestamp_millis()),
            limit: self.limit,
            offset: self.offset,
            sort_desc: self.sort_desc.unwrap_or(false),
            ..Default::default()
        };
        if let Some(s) = &self.status {
            if !s.is_empty() {
                f.status = vec![s.clone()];
            }
        }
        if let Some(li) = self.load_input {
            f.load_input = li;
        }
        if let Some(lo) = self.load_output {
            f.load_output = lo;
        }
        f
    }
}

// ---------------------------------------------------------------------------
// Response shaping (cross-SDK JSON — keys are wire identifiers, kept verbatim)
// ---------------------------------------------------------------------------

/// Render an `Option<i64>` epoch-ms as the stringified-millis the console
/// expects, or JSON `null`.
fn epoch_ms(ms: Option<i64>) -> Value {
    match ms {
        Some(m) => Value::String(m.to_string()),
        None => Value::Null,
    }
}

/// A stored payload becomes its compact JSON string (the console reads a JSON
/// string), or `""` when absent/null.
fn payload_str(v: Option<&Value>) -> String {
    match v {
        Some(v) if !v.is_null() => v.to_string(),
        _ => String::new(),
    }
}

fn to_list_workflow_response(ws: &WorkflowStatus) -> Value {
    json!({
        "WorkflowUUID": ws.id,
        "Status": ws.status,
        "WorkflowName": ws.name,
        "AuthenticatedUser": ws.authenticated_user,
        "AssumedRole": ws.assumed_role,
        "AuthenticatedRoles": ws.authenticated_roles,
        "Output": payload_str(ws.output.as_ref()),
        "Input": payload_str(Some(&ws.input)),
        "ExecutorID": ws.executor_id,
        "ApplicationVersion": ws.app_version,
        "ApplicationID": "",
        "Attempts": ws.recovery_attempts,
        "QueueName": ws.queue_name,
        "Timeout": ws.timeout_ms,
        "DeduplicationID": ws.dedup_id,
        "Priority": ws.priority,
        "QueuePartitionKey": ws.queue_partition_key,
        "CreatedAt": epoch_ms(Some(ws.created_at.timestamp_millis())),
        "UpdatedAt": epoch_ms(Some(ws.updated_at.timestamp_millis())),
        "WorkflowDeadlineEpochMS": epoch_ms(ws.deadline_ms),
        "StartedAt": epoch_ms(ws.started_at_ms),
    })
}

fn to_step_response(step: &StepInfo) -> Value {
    json!({
        "function_id": step.step_id,
        "function_name": step.name,
        "child_workflow_id": step.child_workflow_id,
        "output": payload_str(step.output.as_ref()),
        "error": step.error,
        "started_at_epoch_ms": step.started_at.map(|t| t.timestamp_millis()),
        "completed_at_epoch_ms": step.completed_at.map(|t| t.timestamp_millis()),
    })
}

fn to_queue_metadata(q: &WorkflowQueue) -> Value {
    json!({
        "name": q.name,
        "workerConcurrency": q.worker_concurrency,
        "concurrency": q.global_concurrency,
        "priorityEnabled": q.priority_enabled,
        "rateLimit": q.rate_limit.as_ref().map(|r| json!({
            "Limit": r.limit,
            "Period": r.period.as_nanos() as i64,
        })),
        "maxTasksPerIteration": q.max_tasks_per_iteration,
        "partitionQueue": q.partitioned,
    })
}

fn internal(e: Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}
