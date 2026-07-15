//! The tracing-span contract: every workflow and step execution emits a span
//! carrying the `dbos.*` attributes, parented workflow → step and parent →
//! child, with the outcome recorded at completion and replayed steps marked.
//!
//! Each test installs a thread-local capture subscriber; on the default
//! current-thread test runtime every engine task polls on this thread, so the
//! capture sees all spans without touching global state.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepOptions, WorkflowOptions,
    WorkflowQueue,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One captured span: its metadata name, resolved parent, and every field
/// value recorded at creation or later, stringified.
#[derive(Debug, Clone)]
struct SpanRec {
    id: u64,
    parent: Option<u64>,
    name: String,
    fields: HashMap<String, String>,
}

#[derive(Default, Clone)]
struct Capture(Arc<Mutex<Vec<SpanRec>>>);

impl Capture {
    fn spans(&self) -> Vec<SpanRec> {
        self.0.lock().unwrap().clone()
    }

    /// The captured spans with metadata name `name`, in creation order.
    fn named(&self, name: &str) -> Vec<SpanRec> {
        self.spans()
            .into_iter()
            .filter(|s| s.name == name)
            .collect()
    }
}

struct FieldSink<'a>(&'a mut HashMap<String, String>);

impl Visit for FieldSink<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let parent = if let Some(p) = attrs.parent() {
            Some(p.into_u64())
        } else if attrs.is_contextual() {
            ctx.current_span().id().map(|i| i.into_u64())
        } else {
            None
        };
        let mut fields = HashMap::new();
        attrs.record(&mut FieldSink(&mut fields));
        self.0.lock().unwrap().push(SpanRec {
            id: id.into_u64(),
            parent,
            name: attrs.metadata().name().to_string(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut spans = self.0.lock().unwrap();
        if let Some(rec) = spans.iter_mut().find(|s| s.id == id.into_u64()) {
            values.record(&mut FieldSink(&mut rec.fields));
        }
    }
}

/// Install a capture for this test's thread; keep the guard alive for the
/// test's whole body.
fn capture() -> (Capture, tracing::subscriber::DefaultGuard) {
    let cap = Capture::default();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(cap.clone()));
    (cap, guard)
}

fn field<'r>(rec: &'r SpanRec, name: &str) -> &'r str {
    rec.fields.get(name).unwrap_or_else(|| {
        panic!(
            "span `{}` missing field `{name}`: {:?}",
            rec.name, rec.fields
        )
    })
}

/// A direct run with two steps: the workflow span carries the identity
/// attributes and the recorded outcome; each step span nests under it with
/// its name and checkpoint sequence.
#[tokio::test]
async fn workflow_and_step_spans_carry_dbos_attributes() -> Result<()> {
    let (cap, _guard) = capture();

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "traced-wf",
        |ctx: DurableContext, greeting: String| async move {
            let a = ctx
                .step("compose", || async move {
                    Ok::<_, Error>(format!("{greeting}!"))
                })
                .await?;
            ctx.step_with(StepOptions::new("deliver"), || {
                let a = a.clone();
                async move { Ok::<_, Error>(a.len() as i64) }
            })
            .await
        },
    );
    engine.launch().await?;

    let out: i64 = engine
        .start::<String, i64>(
            "traced-wf",
            "hello".into(),
            WorkflowOptions::with_id("wf-obs-1"),
        )
        .await?
        .await?;
    assert_eq!(out, 6);

    let wf_spans = cap.named("workflow");
    assert_eq!(wf_spans.len(), 1, "one workflow execution, one span");
    let wf = &wf_spans[0];
    assert_eq!(field(wf, "otel.name"), "traced-wf");
    assert_eq!(field(wf, "dbos.operation.type"), "workflow");
    assert_eq!(field(wf, "dbos.operation.workflow_id"), "wf-obs-1");
    assert_eq!(field(wf, "dbos.executor.id"), "local");
    assert!(!field(wf, "dbos.application.version").is_empty());
    assert_eq!(field(wf, "dbos.workflow.status"), "SUCCESS");
    assert_eq!(field(wf, "otel.status_code"), "OK");
    // A direct, anonymous run: no queue, no identity.
    assert!(!wf.fields.contains_key("dbos.queue.name"));
    assert!(!wf.fields.contains_key("dbos.user.name"));

    let steps = cap.named("step");
    assert_eq!(steps.len(), 2, "two durable operations, two spans");
    for (rec, (name, seq)) in steps.iter().zip([("compose", "0"), ("deliver", "1")]) {
        assert_eq!(field(rec, "otel.name"), name);
        assert_eq!(field(rec, "dbos.operation.type"), "step");
        assert_eq!(field(rec, "dbos.operation.workflow_id"), "wf-obs-1");
        assert_eq!(field(rec, "dbos.step.id"), seq);
        assert_eq!(field(rec, "otel.status_code"), "OK");
        assert!(!rec.fields.contains_key("dbos.step.replayed"), "fresh run");
        assert_eq!(rec.parent, Some(wf.id), "step nests under its workflow");
    }
    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// A queued run's span records the queue it was claimed from and the identity
/// it was started with.
#[tokio::test]
async fn queued_span_carries_queue_and_identity() -> Result<()> {
    let (cap, _guard) = capture();

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("queued-wf", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("obs-q"));
    engine.launch().await?;

    let opts = WorkflowOptions {
        workflow_id: Some("wf-obs-q1".into()),
        queue: Some("obs-q".into()),
        authenticated_user: Some("alice".into()),
        authenticated_roles: vec!["admin".into()],
        ..Default::default()
    };
    engine.start::<(), ()>("queued-wf", (), opts).await?.await?;

    let wf = &cap.named("workflow")[0];
    assert_eq!(field(wf, "dbos.queue.name"), "obs-q");
    assert_eq!(field(wf, "dbos.user.name"), "alice");
    assert_eq!(field(wf, "dbos.user.roles"), r#"["admin"]"#);
    assert_eq!(field(wf, "dbos.workflow.status"), "SUCCESS");
    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// A failing step and its failing workflow both record `ERROR`, and the
/// terminal DBOS status lands on the workflow span.
#[tokio::test]
async fn failure_records_error_status() -> Result<()> {
    let (cap, _guard) = capture();

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("failing-wf", |ctx: DurableContext, (): ()| async move {
        ctx.step("explode", || async { Err::<(), _>(Error::app("boom")) })
            .await
    });
    engine.launch().await?;

    let err = engine
        .start::<(), ()>("failing-wf", (), WorkflowOptions::with_id("wf-obs-err"))
        .await?
        .await
        .expect_err("the workflow fails");
    assert!(err.to_string().contains("boom"));

    let wf = &cap.named("workflow")[0];
    assert_eq!(field(wf, "dbos.workflow.status"), "ERROR");
    assert_eq!(field(wf, "otel.status_code"), "ERROR");
    let step = &cap.named("step")[0];
    assert_eq!(field(step, "otel.name"), "explode");
    assert_eq!(field(step, "otel.status_code"), "ERROR");
    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// A child workflow's span nests under the parent workflow's span.
#[tokio::test]
async fn child_workflow_span_parents_under_parent() -> Result<()> {
    let (cap, _guard) = capture();

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("obs-child", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>("done".to_string())
    });
    engine.register("obs-parent", |ctx: DurableContext, (): ()| async move {
        let child = ctx
            .start_workflow::<(), String>("obs-child", (), WorkflowOptions::default())
            .await?;
        child.await
    });
    engine.launch().await?;

    let out: String = engine
        .start::<(), String>("obs-parent", (), WorkflowOptions::with_id("wf-obs-parent"))
        .await?
        .await?;
    assert_eq!(out, "done");

    let wf_spans = cap.named("workflow");
    assert_eq!(wf_spans.len(), 2, "parent and child");
    let parent = wf_spans
        .iter()
        .find(|s| field(s, "otel.name") == "obs-parent")
        .expect("parent span");
    let child = wf_spans
        .iter()
        .find(|s| field(s, "otel.name") == "obs-child")
        .expect("child span");
    assert_eq!(child.parent, Some(parent.id));
    assert!(parent.parent.is_none(), "top-level run is a root span");
    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// The recovery story in trace form: after a crash mid-workflow, the re-run's
/// span tree shows the already-checkpointed step as replayed and only the
/// frontier step doing fresh work.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn recovered_run_marks_replayed_steps() -> Result<()> {
    use durare::{EngineConfig, SqliteProvider};
    use std::sync::atomic::{AtomicBool, Ordering};

    let (cap, _guard) = capture();
    static STALL: AtomicBool = AtomicBool::new(true);
    static S1_RAN: AtomicBool = AtomicBool::new(false);

    let mut path = std::env::temp_dir();
    path.push(format!("durare-obs-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let register = |engine: &mut DurableEngine| {
        engine.register("obs-recover", |ctx: DurableContext, (): ()| async move {
            ctx.step("first", || async {
                S1_RAN.store(true, Ordering::SeqCst);
                Ok::<_, Error>(1_i64)
            })
            .await?;
            if STALL.load(Ordering::SeqCst) {
                // The crash: this run never finishes, like a killed process.
                std::future::pending::<()>().await;
            }
            ctx.step("second", || async { Ok::<_, Error>(2_i64) }).await
        });
    };

    // Run 1 checkpoints `first`, then stalls; dropping the engine is the crash.
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let mut crashed = DurableEngine::with_config(
        provider.clone(),
        EngineConfig::default().executor_id("obs-exec-a"),
    )
    .await?;
    register(&mut crashed);
    crashed.launch().await?;
    let _ = crashed
        .start::<(), i64>("obs-recover", (), WorkflowOptions::with_id("wf-obs-rec"))
        .await?;
    while !S1_RAN.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await; // let the checkpoint land
    drop(crashed);

    // Run 2 takes over and completes; `first` must replay, `second` runs.
    STALL.store(false, Ordering::SeqCst);
    let provider2 = Arc::new(SqliteProvider::connect(&url).await?);
    let mut takeover =
        DurableEngine::with_config(provider2, EngineConfig::default().executor_id("obs-exec-b"))
            .await?;
    register(&mut takeover);
    takeover.launch().await?;
    let recovered = takeover
        .recover_pending_for(&["obs-exec-a".to_string()])
        .await?;
    assert_eq!(recovered, vec!["wf-obs-rec".to_string()]);

    let firsts: Vec<_> = cap
        .named("step")
        .into_iter()
        .filter(|s| field(s, "otel.name") == "first")
        .collect();
    assert_eq!(firsts.len(), 2, "one fresh run, one replay");
    assert!(!firsts[0].fields.contains_key("dbos.step.replayed"));
    assert_eq!(field(&firsts[1], "dbos.step.replayed"), "true");
    assert_eq!(field(&firsts[1], "otel.status_code"), "OK");

    let seconds: Vec<_> = cap
        .named("step")
        .into_iter()
        .filter(|s| field(s, "otel.name") == "second")
        .collect();
    assert_eq!(seconds.len(), 1, "the frontier step runs once, fresh");
    assert!(!seconds[0].fields.contains_key("dbos.step.replayed"));

    let statuses: Vec<_> = cap
        .named("workflow")
        .iter()
        .map(|s| s.fields.get("dbos.workflow.status").cloned())
        .collect();
    // The crashed run records no terminal status; the takeover records SUCCESS.
    assert_eq!(statuses, vec![None, Some("SUCCESS".to_string())]);

    takeover.shutdown(Duration::from_secs(5)).await?;
    drop(provider);
    let _ = std::fs::remove_file(&path);
    Ok(())
}
