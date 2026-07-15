//! Conductor client (part 1): connection lifecycle + the executor-lifecycle
//! handlers. A local websocket server stands in for the cloud conductor: it
//! pushes requests and asserts on the client's responses.
#![cfg(feature = "conductor")]

use durare::{
    AlertHandler, Conductor, ConductorConfig, DurableContext, DurableEngine, Error,
    InMemoryProvider, Result, ScheduleOptions, WorkflowOptions, WorkflowQueue,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
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
            alert_handler: None,
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
async fn conductor_requires_api_key_and_defaults_url() -> Result<()> {
    let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);

    let no_key = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: "ws://localhost:1".into(),
            api_key: String::new(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    );
    assert!(no_key.is_err(), "missing API key is rejected");

    // An empty URL is accepted: it defaults to the hosted DBOS conductor.
    // Point the domain at a closed local port so the background dial fails
    // fast instead of contacting the real endpoint. Safe to mutate here: no
    // other test in this binary reads DBOS_DOMAIN (they pass explicit URLs),
    // and the URL is resolved inside `start`, so the variable can be reset
    // immediately — before any fallible await could skip the cleanup.
    std::env::set_var("DBOS_DOMAIN", "127.0.0.1:1");
    let defaulted = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: String::new(),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    );
    std::env::remove_var("DBOS_DOMAIN");
    let defaulted = defaulted.expect("empty URL defaults instead of erroring");
    defaulted.shutdown(Duration::from_secs(2)).await?;
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
                   "body":{"workflow_id":"wf-1","start_step":0,"new_workflow_id":"forked-1",
                           "queue_name":"cond-fork-q","queue_partition_key":"p-1"}}),
        )
        .await;
        let fork_row = exchange(
            &mut ws,
            json!({"type":"get_workflow","request_id":"fg","workflow_id":"forked-1"}),
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
        (list, get, steps, fork, fork_row, cancel, delete)
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
    let h = engine
        .start::<_, String>("work", "hi".to_string(), WorkflowOptions::with_id("wf-1"))
        .await?;
    assert_eq!(h.result().await?, "hi!");

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (list, get, steps, fork, fork_row, cancel, delete) = server.await.unwrap();

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

    // fork -> the new id we asked for, enqueued on the requested queue (no
    // dispatcher listens on it, so the fork sits ENQUEUED with the partition
    // key persisted — proving queue routing reached the row).
    assert_eq!(fork["new_workflow_id"], "forked-1");
    assert_eq!(fork_row["output"]["WorkflowUUID"], "forked-1");
    assert_eq!(fork_row["output"]["Status"], "ENQUEUED");
    assert_eq!(fork_row["output"]["QueueName"], "cond-fork-q");

    // cancel / delete -> success.
    assert_eq!(cancel["success"], true);
    assert_eq!(delete["success"], true);

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A conductor `resume` carrying `queue_name` re-enqueues the resumed workflow
/// onto that named queue (the reference conductor's resume queue option). No
/// dispatcher listens on the queue, so the row sits `ENQUEUED` there — proving
/// the queue option reached the row through the conductor path.
#[tokio::test]
async fn conductor_resume_routes_to_named_queue() -> Result<()> {
    use durare::{StateProvider, WorkflowStatus, STATUS_PENDING};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let resume = exchange(
            &mut ws,
            json!({"type":"resume","request_id":"r","workflow_ids":["wf-r"],
                   "queue_name":"cond-resume-q"}),
        )
        .await;
        let row = exchange(
            &mut ws,
            json!({"type":"get_workflow","request_id":"rg","workflow_id":"wf-r"}),
        )
        .await;
        (resume, row)
    });

    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("bounce", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;

    // Seed a cancelled (resumable) run. cond-resume-q has no dispatcher, so a
    // resume onto it leaves the row ENQUEUED there for us to observe.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-r",
            "bounce",
            json!(41),
            STATUS_PENDING,
            "",
            engine.app_version(),
        ))
        .await?;
    engine.cancel_workflow("wf-r").await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (resume, row) = server.await.unwrap();
    assert_eq!(resume["success"], true);
    assert_eq!(row["output"]["WorkflowUUID"], "wf-r");
    assert_eq!(row["output"]["Status"], "ENQUEUED");
    assert_eq!(row["output"]["QueueName"], "cond-resume-q");

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_handles_schedule_management() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let list = exchange(
            &mut ws,
            json!({"type":"list_schedules","request_id":"l","body":{}}),
        )
        .await;
        let get = exchange(
            &mut ws,
            json!({"type":"get_schedule","request_id":"g","schedule_name":"s1"}),
        )
        .await;
        let pause = exchange(
            &mut ws,
            json!({"type":"pause_schedule","request_id":"p","schedule_name":"s1"}),
        )
        .await;
        let resume = exchange(
            &mut ws,
            json!({"type":"resume_schedule","request_id":"r","schedule_name":"s1"}),
        )
        .await;
        let trigger = exchange(
            &mut ws,
            json!({"type":"trigger_schedule","request_id":"t","schedule_name":"s1"}),
        )
        .await;
        let backfill = exchange(
            &mut ws,
            json!({"type":"backfill_schedule","request_id":"b","schedule_name":"s1",
                   "start":"2022-06-01T00:00:00Z","end":"2024-06-01T00:00:00Z"}),
        )
        .await;
        let missing = exchange(
            &mut ws,
            json!({"type":"get_schedule","request_id":"m","schedule_name":"nope"}),
        )
        .await;
        (list, get, pause, resume, trigger, backfill, missing)
    });

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("sched_wf", |_ctx: DurableContext, ts: String| async move {
        Ok::<_, Error>(ts)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;
    // Cron fires Jan 1 00:00:00 each year — never during the test, but the
    // backfill window below spans two such instants.
    engine
        .create_schedule(
            "s1",
            "sched_wf",
            "0 0 0 1 1 *",
            ScheduleOptions::new().context(&json!({"k": "v"})),
        )
        .await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (list, get, pause, resume, trigger, backfill, missing) = server.await.unwrap();

    // list_schedules -> our schedule in the conductor wire shape.
    let rows = list["output"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["schedule_name"], "s1");
    assert_eq!(rows[0]["workflow_name"], "sched_wf");
    assert_eq!(rows[0]["status"], "ACTIVE");
    assert_eq!(rows[0]["context"], "{\"k\":\"v\"}"); // load_context defaults true
    assert!(rows[0]["workflow_class_name"].is_null()); // null, not omitted

    // get_schedule -> single body.
    assert_eq!(get["output"]["schedule_name"], "s1");

    // pause / resume -> success.
    assert_eq!(pause["success"], true);
    assert_eq!(resume["success"], true);

    // trigger -> a workflow id for the one-off run.
    assert!(trigger["workflow_id"].is_string());

    // backfill -> the Jan-1 ticks inside the window.
    let ids = backfill["workflow_ids"].as_array().unwrap();
    assert!(!ids.is_empty(), "backfill fired at least one tick");

    // get_schedule for an unknown name -> output null.
    assert!(missing["output"].is_null());

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_handles_registry_and_aggregates() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let queues = exchange(&mut ws, json!({"type":"list_queues","request_id":"q"})).await;
        let queue = exchange(
            &mut ws,
            json!({"type":"get_queue","request_id":"gq","name":"myq"}),
        )
        .await;
        let versions = exchange(
            &mut ws,
            json!({"type":"list_application_versions","request_id":"v"}),
        )
        .await;
        let set_latest = exchange(
            &mut ws,
            json!({"type":"set_latest_application_version","request_id":"sv","version_name":"2.0.0"}),
        )
        .await;
        let wagg = exchange(
            &mut ws,
            json!({"type":"get_workflow_aggregates","request_id":"wa",
                   "body":{"group_by_status":true,"select_count":true,
                           "select_min_created_at":true,
                           "select_max_queue_wait_ms":true,
                           "select_max_total_latency_ms":true}}),
        )
        .await;
        let sagg = exchange(
            &mut ws,
            json!({"type":"get_step_aggregates","request_id":"sa",
                   "body":{"group_by_function_name":true,"select_count":true}}),
        )
        .await;
        // Count-only requests omit every select_* flag; the handler must default
        // to count (Go/Python parity) rather than rejecting the query.
        let wagg_count_only = exchange(
            &mut ws,
            json!({"type":"get_workflow_aggregates","request_id":"wac",
                   "body":{"group_by_status":true}}),
        )
        .await;
        let sagg_count_only = exchange(
            &mut ws,
            json!({"type":"get_step_aggregates","request_id":"sac",
                   "body":{"group_by_function_name":true}}),
        )
        .await;
        (
            queues,
            queue,
            versions,
            set_latest,
            wagg,
            sagg,
            wagg_count_only,
            sagg_count_only,
        )
    });

    let mut engine =
        DurableEngine::new_with_version(Arc::new(InMemoryProvider::new()), "2.0.0").await?;
    engine.register("work", |ctx: DurableContext, msg: String| async move {
        let r = ctx
            .step("s1", || async { Ok::<_, Error>(format!("{msg}!")) })
            .await?;
        Ok::<_, Error>(r)
    });
    engine.register_queue(WorkflowQueue::new("myq").worker_concurrency(3));
    let engine = Arc::new(engine);
    engine.launch().await?; // registers application version 2.0.0
    let h = engine
        .start::<_, String>("work", "hi".to_string(), WorkflowOptions::with_id("wf-1"))
        .await?;
    h.result().await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (queues, queue, versions, set_latest, wagg, sagg, wagg_count_only, sagg_count_only) =
        server.await.unwrap();

    // list_queues -> our registered queue (the internal queue is hidden).
    let qs = queues["output"].as_array().unwrap();
    let myq = qs.iter().find(|q| q["name"] == "myq").expect("myq listed");
    assert_eq!(myq["worker_concurrency"], 3);
    assert!(myq["concurrency"].is_null()); // null, not omitted
    assert!(myq["polling_interval_sec"].is_number());

    // get_queue -> the same queue.
    assert_eq!(queue["output"]["name"], "myq");

    // list_application_versions -> our launched version present.
    let vs = versions["output"].as_array().unwrap();
    assert!(vs.iter().any(|v| v["version_name"] == "2.0.0"));
    assert!(vs[0]["version_timestamp"].is_number());

    // set_latest -> success.
    assert_eq!(set_latest["success"], true);

    // workflow aggregates grouped by status -> a SUCCESS group of count 1.
    let wrows = wagg["output"].as_array().unwrap();
    let success = wrows
        .iter()
        .find(|r| r["group"]["status"] == "SUCCESS")
        .expect("a SUCCESS group");
    assert_eq!(success["count"], 1);
    // Selected latency aggregates are now computed. A direct run starts the
    // instant it is created, so queue-wait is exactly 0 (not null, and never
    // negative — started_at is derived from created_at, so it can't precede it).
    assert!(success["min_created_at"].is_number());
    assert!(success["max_total_latency_ms"].as_i64().unwrap() >= 0);
    assert_eq!(
        success["max_queue_wait_ms"].as_i64().unwrap(),
        0,
        "a direct run has zero queue wait"
    );

    // step aggregates grouped by function name -> our step 's1'.
    let srows = sagg["output"].as_array().unwrap();
    assert!(srows.iter().any(|r| r["group"]["function_name"] == "s1"));

    // Count-only requests (no select_* flags) default to count, not an error.
    assert!(wagg_count_only["error_message"].is_null());
    let wco = wagg_count_only["output"].as_array().unwrap();
    let wco_success = wco
        .iter()
        .find(|r| r["group"]["status"] == "SUCCESS")
        .expect("a SUCCESS group");
    assert_eq!(wco_success["count"], 1);
    assert!(wco_success["min_created_at"].is_null()); // unselected -> null
    assert!(sagg_count_only["error_message"].is_null());
    let sco = sagg_count_only["output"].as_array().unwrap();
    let s1 = sco
        .iter()
        .find(|r| r["group"]["function_name"] == "s1")
        .expect("an s1 group");
    assert_eq!(s1["count"], 1);

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_handles_events_and_streams() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let events = exchange(
            &mut ws,
            json!({"type":"get_workflow_events","request_id":"e","workflow_id":"p-1"}),
        )
        .await;
        let streams = exchange(
            &mut ws,
            json!({"type":"get_workflow_streams","request_id":"st","workflow_id":"p-1"}),
        )
        .await;
        let notifs = exchange(
            &mut ws,
            json!({"type":"get_workflow_notifications","request_id":"n","workflow_id":"p-1"}),
        )
        .await;
        (events, streams, notifs)
    });

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("producer", |ctx: DurableContext, _: String| async move {
        ctx.set_event("status", "done").await?;
        ctx.write_stream("log", "line1").await?;
        ctx.write_stream("log", "line2").await?;
        ctx.close_stream("log").await?;
        Ok::<_, Error>("ok".to_string())
    });
    let engine = Arc::new(engine);
    engine.launch().await?;
    let h = engine
        .start::<_, String>("producer", String::new(), WorkflowOptions::with_id("p-1"))
        .await?;
    h.result().await?;
    // A notification delivered to the workflow's mailbox.
    engine.send("p-1", "hello", "greetings").await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (events, streams, notifs) = server.await.unwrap();

    // events -> {key, value} with the value JSON-stringified.
    let evs = events["events"].as_array().unwrap();
    let status = evs
        .iter()
        .find(|e| e["key"] == "status")
        .expect("status event");
    assert_eq!(status["value"], "\"done\"");

    // streams -> values grouped by key, in write order, sentinel excluded.
    let sts = streams["streams"].as_array().unwrap();
    let log = sts.iter().find(|s| s["key"] == "log").expect("log stream");
    assert_eq!(
        log["values"].as_array().unwrap(),
        &vec![json!("\"line1\""), json!("\"line2\"")]
    );

    // notifications -> the delivered message, with its topic and consumed flag.
    let ns = notifs["notifications"].as_array().unwrap();
    assert_eq!(ns.len(), 1);
    assert_eq!(ns[0]["topic"], "greetings");
    assert_eq!(ns[0]["message"], "\"hello\"");
    assert_eq!(ns[0]["consumed"], false);
    assert!(ns[0]["created_at_epoch_ms"].is_number());

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_handles_metrics_and_retention() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let metrics = exchange(
            &mut ws,
            json!({"type":"get_metrics","request_id":"m",
                   "start_time":"2000-01-01T00:00:00Z","end_time":"2100-01-01T00:00:00Z",
                   "metric_class":"workflow_step_count"}),
        )
        .await;
        let bad_class = exchange(
            &mut ws,
            json!({"type":"get_metrics","request_id":"mb",
                   "start_time":"2000-01-01T00:00:00Z","end_time":"2100-01-01T00:00:00Z",
                   "metric_class":"nonsense"}),
        )
        .await;
        // Cutoff far in the future -> cancels everything still pending.
        let retention = exchange(
            &mut ws,
            json!({"type":"retention","request_id":"rt",
                   "body":{"timeout_cutoff_epoch_ms":9999999999999i64}}),
        )
        .await;
        (metrics, bad_class, retention)
    });

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("work", |ctx: DurableContext, msg: String| async move {
        let r = ctx
            .step("s1", || async { Ok::<_, Error>(format!("{msg}!")) })
            .await?;
        Ok::<_, Error>(r)
    });
    engine.register_queue(WorkflowQueue::new("q"));
    let engine = Arc::new(engine);
    engine.launch().await?;

    // One completed workflow (with a step) ...
    let h = engine
        .start::<_, String>("work", "hi".to_string(), WorkflowOptions::with_id("wf-1"))
        .await?;
    h.result().await?;
    // ... and one long-delayed workflow that stays DELAYED (cancellable).
    let _delayed = engine
        .start::<_, String>(
            "work",
            "later".to_string(),
            WorkflowOptions {
                workflow_id: Some("delayed-1".into()),
                queue: Some("q".into()),
                delay: Some(Duration::from_secs(3600)),
                ..Default::default()
            },
        )
        .await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (metrics, bad_class, retention) = server.await.unwrap();

    // metrics -> workflow_count for "work" and step_count for "s1".
    let ms = metrics["metrics"].as_array().unwrap();
    let wf_metric = ms
        .iter()
        .find(|m| m["metric_type"] == "workflow_count" && m["metric_name"] == "work")
        .expect("workflow_count for 'work'");
    assert!(wf_metric["value"].as_f64().unwrap() >= 1.0);
    assert!(ms
        .iter()
        .any(|m| m["metric_type"] == "step_count" && m["metric_name"] == "s1"));

    // unexpected metric class -> error + null metrics.
    assert!(bad_class["error_message"]
        .as_str()
        .unwrap()
        .contains("Unexpected metric class"));
    assert!(bad_class["metrics"].is_null());

    // retention timeout -> success, and the delayed workflow is now cancelled.
    assert_eq!(retention["success"], true);
    let status = engine.retrieve_workflow::<String>("delayed-1").await?;
    assert_eq!(status.get_status().await?.status, "CANCELLED");

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// export_workflow ships a workflow's durable state as a gzipped/base64 payload;
/// import_workflow restores it. The round trip goes entirely over the conductor
/// link: export, delete via the link, then re-import and verify the state is back.
#[tokio::test]
async fn conductor_exports_and_imports_workflow() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        let exported = exchange(
            &mut ws,
            json!({"type":"export_workflow","request_id":"ex","workflow_id":"p-1"}),
        )
        .await;
        // Delete the workflow over the link so import has to recreate it.
        let deleted = exchange(
            &mut ws,
            json!({"type":"delete","request_id":"del","workflow_id":"p-1"}),
        )
        .await;
        let serialized = exported["serialized_workflow"]
            .as_str()
            .unwrap()
            .to_string();
        let imported = exchange(
            &mut ws,
            json!({"type":"import_workflow","request_id":"imp",
                   "serialized_workflow": serialized}),
        )
        .await;
        // A missing workflow reports an error and ships no payload.
        let missing = exchange(
            &mut ws,
            json!({"type":"export_workflow","request_id":"miss","workflow_id":"ghost"}),
        )
        .await;
        // A malformed payload fails cleanly with success=false.
        let bad = exchange(
            &mut ws,
            json!({"type":"import_workflow","request_id":"bad",
                   "serialized_workflow":"not valid base64!!"}),
        )
        .await;
        (exported, deleted, imported, missing, bad)
    });

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("producer", |ctx: DurableContext, n: i64| async move {
        let doubled = ctx
            .step("double", || async { Ok::<_, Error>(n * 2) })
            .await?;
        ctx.set_event("status", "done").await?;
        ctx.write_stream("log", "line1").await?;
        ctx.write_stream("log", "line2").await?;
        ctx.close_stream("log").await?;
        Ok::<_, Error>(doubled)
    });
    let engine = Arc::new(engine);
    engine.launch().await?;
    engine
        .start::<_, i64>("producer", 21_i64, WorkflowOptions::with_id("p-1"))
        .await?
        .result()
        .await?;

    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "k".into(),
            app_name: "app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (exported, deleted, imported, missing, bad) = server.await.unwrap();

    // export -> a non-empty serialized payload, no error.
    assert_eq!(exported["type"], "export_workflow");
    assert!(exported.get("error_message").is_none());
    assert!(!exported["serialized_workflow"].as_str().unwrap().is_empty());

    assert_eq!(deleted["success"], true);
    assert_eq!(imported["type"], "import_workflow");
    assert_eq!(imported["success"], true);
    assert!(imported.get("error_message").is_none());

    // export of an unknown workflow -> error, no payload.
    assert!(missing["error_message"].is_string());
    assert!(missing.get("serialized_workflow").is_none());

    // import of a malformed payload -> failure with a base64 error.
    assert_eq!(bad["success"], false);
    assert!(bad["error_message"].as_str().unwrap().contains("base64"));

    // The re-imported workflow's durable state is intact.
    let rows = engine
        .list_workflows(&durare::ListFilter {
            workflow_ids: vec!["p-1".to_string()],
            load_output: true,
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, durare::STATUS_SUCCESS);
    assert_eq!(rows[0].output, Some(json!(42)));

    let steps = engine.get_workflow_steps("p-1").await?;
    let double = steps.iter().find(|s| s.name == "double").expect("step");
    assert_eq!(double.output, Some(json!(42)));

    let events = engine.list_workflow_events("p-1").await?;
    assert_eq!(events, vec![("status".to_string(), json!("done"))]);

    let streams = engine.list_workflow_streams("p-1").await?;
    assert_eq!(
        streams,
        vec![("log".to_string(), vec![json!("line1"), json!("line2")])]
    );

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_handles_alert() -> Result<()> {
    use std::sync::Mutex;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let ok = exchange(
            &mut ws,
            json!({"type":"alert","request_id":"a1","name":"deploy",
                   "message":"new version live","metadata":{"env":"prod"}}),
        )
        .await;
        let boom = exchange(
            &mut ws,
            json!({"type":"alert","request_id":"a2","name":"boom",
                   "message":"trigger panic","metadata":{}}),
        )
        .await;
        (ok, boom)
    });

    // The handler records each alert it receives, and panics on "boom".
    type AlertLog = Arc<Mutex<Vec<(String, String, HashMap<String, String>)>>>;
    let seen: AlertLog = Arc::new(Mutex::new(Vec::new()));
    let recorder = seen.clone();
    let handler: AlertHandler = Arc::new(move |name: &str, message: &str, meta| {
        if name == "boom" {
            panic!("handler exploded");
        }
        recorder
            .lock()
            .unwrap()
            .push((name.to_string(), message.to_string(), meta.clone()));
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
            alert_handler: Some(handler),
        },
    )?;

    let (ok, boom) = server.await.unwrap();

    // A normal alert acks success and reaches the handler verbatim.
    assert_eq!(ok["type"], "alert");
    assert_eq!(ok["request_id"], "a1");
    assert_eq!(ok["success"], true);
    assert!(ok.get("error_message").is_none());
    let recorded = seen.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0, "deploy");
    assert_eq!(recorded[0].1, "new version live");
    assert_eq!(recorded[0].2.get("env").map(String::as_str), Some("prod"));

    // A panicking handler is caught and reported as a failure (the link lives on).
    assert_eq!(boom["success"], false);
    assert!(boom["error_message"]
        .as_str()
        .unwrap()
        .contains("panic in alert handler"));

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn conductor_acks_alert_without_handler() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        exchange(
            &mut ws,
            json!({"type":"alert","request_id":"a","name":"info",
                   "message":"hi","metadata":{}}),
        )
        .await
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
            alert_handler: None,
        },
    )?;

    // With no handler registered, the alert is still acknowledged as success.
    let resp = server.await.unwrap();
    assert_eq!(resp["type"], "alert");
    assert_eq!(resp["request_id"], "a");
    assert_eq!(resp["success"], true);
    assert!(resp.get("error_message").is_none());

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
            alert_handler: None,
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

/// The conductor service (Go) marshals absent lists as explicit JSON `null` —
/// the console's very first `list_workflows` arrives as
/// `{"body":{"workflow_uuids":null,…}}`. Every list-typed request field must
/// treat that as empty rather than failing deserialization (which surfaced as
/// `failed to handle conductor message error=serialization error: invalid
/// type: null, expected a sequence` against the live console).
#[tokio::test]
async fn conductor_tolerates_explicit_null_list_fields() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();

        // The console's opening query, verbatim shape.
        let list = exchange(
            &mut ws,
            json!({"type":"list_workflows","request_id":"n1",
                   "body":{"workflow_uuids":null,"workflow_name":null,
                           "status":null,"limit":50,"sort_desc":true}}),
        )
        .await;
        let recovery = exchange(
            &mut ws,
            json!({"type":"recovery","request_id":"n2","executor_ids":null}),
        )
        .await;
        let cancel = exchange(
            &mut ws,
            json!({"type":"cancel","request_id":"n3",
                   "workflow_id":"","workflow_ids":null}),
        )
        .await;

        (list, recovery, cancel)
    });

    let engine = Arc::new(DurableEngine::new(Arc::new(InMemoryProvider::new())).await?);
    engine.launch().await?;
    let conductor = Conductor::start(
        engine.clone(),
        ConductorConfig {
            url: format!("ws://127.0.0.1:{port}"),
            api_key: "test-key".into(),
            app_name: "test-app".into(),
            executor_metadata: None,
            alert_handler: None,
        },
    )?;

    let (list, recovery, cancel) = server.await.unwrap();

    assert_eq!(list["request_id"], "n1");
    assert!(
        list.get("error_message").is_none(),
        "null list filters must not error: {list}"
    );
    assert!(
        list["output"].is_array(),
        "a real listing came back: {list}"
    );

    assert_eq!(recovery["request_id"], "n2");
    assert_eq!(recovery["success"], true, "null executor_ids: {recovery}");

    assert_eq!(cancel["request_id"], "n3");
    assert_eq!(cancel["success"], true, "null workflow_ids: {cancel}");

    conductor.shutdown(Duration::from_secs(2)).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
