//! The dynamic-SQL invariant, enforced from the outside.
//!
//! The invariant: **no caller-supplied string ever becomes SQL text.** Every
//! fragment interpolated into a query via `format!`/`QueryBuilder::push` is a
//! compile-time constant (column lists, dialect keywords, enum-derived
//! clauses); every runtime value — ids, names, keys, topics, filter values —
//! travels as a bind parameter. The one identifier that comes from
//! configuration, the Postgres system-tables schema, is validated as a plain
//! identifier before it is stored.
//!
//! Rather than pin the source (which a refactor would silently invalidate),
//! this suite pushes hostile strings through every string-typed public input —
//! workflow ids and names, queue names, dedup and partition keys, topics,
//! event and stream keys, step names, schedule names, and every list-filter
//! field — and asserts they round-trip verbatim as *data* with the system
//! tables intact afterwards. If any of these ever reached SQL text, the sweep
//! would fail loudly: the statements would break on the embedded quotes, or
//! the drop would destroy the tables the final assertions read.

use durare::{
    DurableContext, DurableEngine, Error, ListFilter, Result, ScheduleOptions, StateProvider,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

mod common;

/// The classic: closes the string literal, ends the statement, drops the
/// central table, comments out the remainder.
const EVIL: &str = "'; DROP TABLE workflow_status; --";

/// Push `EVIL` through every string-typed input of one engine and assert it
/// stays data. Shared by the SQLite and Postgres runs below.
async fn hostile_sweep(provider: Arc<dyn StateProvider>) -> Result<()> {
    let wf_name = format!("wf{EVIL}");
    let queue_name = format!("q{EVIL}");
    let topic = format!("topic{EVIL}");
    let event_key = format!("event{EVIL}");
    let stream_key = format!("stream{EVIL}");
    let step_name = format!("step{EVIL}");
    let schedule_name = format!("sched{EVIL}");
    let direct_id = format!("direct{EVIL}");
    let queued_id = format!("queued{EVIL}");
    let user = format!("user{EVIL}");
    let version = format!("ver{EVIL}");

    let mut engine = DurableEngine::new(provider).await?;
    {
        let (topic, event_key, stream_key, step_name) = (
            topic.clone(),
            event_key.clone(),
            stream_key.clone(),
            step_name.clone(),
        );
        engine.register(&wf_name, move |ctx: DurableContext, (): ()| {
            let (topic, event_key, stream_key, step_name) = (
                topic.clone(),
                event_key.clone(),
                stream_key.clone(),
                step_name.clone(),
            );
            async move {
                // recv consumes a persisted notification on a hostile topic …
                let msg = ctx.recv::<String>(&topic, Duration::from_secs(10)).await?;
                // … and the other durable ops write under hostile keys/names.
                ctx.set_event(&event_key, EVIL).await?;
                ctx.write_stream(&stream_key, EVIL).await?;
                ctx.close_stream(&stream_key).await?;
                ctx.step(&step_name, || async { Ok::<_, Error>(()) })
                    .await?;
                Ok::<_, Error>(msg.unwrap_or_default())
            }
        });
    }
    engine.register_queue(WorkflowQueue::new(&queue_name).partitioned());
    engine.listen_queues([queue_name.clone()]);
    engine.launch().await?;

    // Direct run: hostile workflow id, authenticated user, role, app version.
    let handle = engine
        .start::<(), String>(
            &wf_name,
            (),
            WorkflowOptions {
                workflow_id: Some(direct_id.clone()),
                authenticated_user: Some(user.clone()),
                assumed_role: Some(format!("role{EVIL}")),
                app_version: Some(version.clone()),
                ..Default::default()
            },
        )
        .await?;
    // Notifications are durable, so sending before the recv is reached is safe.
    engine
        .send_with_idempotency_key(&direct_id, EVIL.to_string(), &topic, &format!("ikey{EVIL}"))
        .await?;
    assert_eq!(handle.await?, EVIL, "message round-trips verbatim");

    // Queued run: hostile queue name, dedup id, and partition key drive the
    // dispatcher's counting/claiming SQL with hostile binds.
    let queued = engine
        .start::<(), String>(
            &wf_name,
            (),
            WorkflowOptions {
                workflow_id: Some(queued_id.clone()),
                queue: Some(queue_name.clone()),
                dedup_id: Some(format!("dedup{EVIL}")),
                partition_key: Some(format!("part{EVIL}")),
                ..Default::default()
            },
        )
        .await?;
    engine.send(&queued_id, EVIL.to_string(), &topic).await?;
    assert_eq!(queued.await?, EVIL);

    // Event and stream values written under hostile keys read back verbatim.
    let event: Option<String> = engine
        .get_event(&direct_id, &event_key, Duration::from_secs(5))
        .await?;
    assert_eq!(event.as_deref(), Some(EVIL));
    let (values, closed): (Vec<String>, bool) = engine.read_stream(&direct_id, &stream_key).await?;
    assert_eq!(values, vec![EVIL.to_string()]);
    assert!(closed);

    // The hostile step name landed in the step log as data.
    let steps = engine.get_workflow_steps(&direct_id).await?;
    assert!(
        steps.iter().any(|s| s.name == step_name),
        "hostile step name recorded verbatim: {steps:?}"
    );

    // Schedule CRUD under a hostile schedule name (a far-future cron so it
    // never fires).
    engine
        .create_schedule(
            &schedule_name,
            &wf_name,
            "0 0 0 1 1 *",
            ScheduleOptions::new(),
        )
        .await?;
    assert!(engine.get_schedule(&schedule_name).await?.is_some());
    assert!(engine.pause_schedule(&schedule_name).await?);
    assert!(engine.delete_schedule(&schedule_name).await?);

    // Every string-typed list filter loaded with the hostile value at once:
    // the query executes cleanly and matches nothing.
    let miss = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![EVIL.into()],
            workflow_id_prefix: vec![EVIL.into()],
            name: vec![EVIL.into()],
            status: vec![EVIL.into()],
            queue_name: vec![EVIL.into()],
            app_version: vec![EVIL.into()],
            executor_ids: vec![EVIL.into()],
            authenticated_users: vec![EVIL.into()],
            forked_from: vec![EVIL.into()],
            parent_workflow_ids: vec![EVIL.into()],
            ..Default::default()
        })
        .await?;
    assert!(miss.is_empty());

    // The same filters with the *stored* hostile values match exactly the
    // direct run — hostile strings compare as data on the way back out too.
    let hit = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![direct_id.clone()],
            name: vec![wf_name.clone()],
            authenticated_users: vec![user.clone()],
            app_version: vec![version.clone()],
            ..Default::default()
        })
        .await?;
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0].id, direct_id);

    // The system tables survived the whole sweep: both runs are readable and
    // terminal. (Had any input reached SQL text, the DROP would have landed or
    // a statement would have broken on the quotes long before this.)
    let survivors = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec![direct_id.clone(), queued_id.clone()],
            ..Default::default()
        })
        .await?;
    assert_eq!(survivors.len(), 2, "workflow_status intact");

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// The sweep against SQLite — bind parameters and static SQL only.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_rejects_sql_injection_everywhere() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-security-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let result = hostile_sweep(provider).await;
    let _ = std::fs::remove_file(&path);
    result
}

/// The sweep against Postgres, in a hermetic database.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_rejects_sql_injection_everywhere() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_rejects_sql_injection_everywhere: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_security").await;

    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let result = hostile_sweep(provider).await;

    common::drop_hermetic_pg_db(&admin, &dbname).await;
    result
}

/// The one config-supplied identifier that *is* interpolated into SQL — the
/// Postgres system-tables schema — is validated before it is stored, so a
/// hostile schema name is rejected at the constructor, before any connection.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_schema_name_must_be_a_plain_identifier() {
    use durare::PostgresProvider;

    for bad in [
        "dbos; DROP TABLE workflow_status",
        "a-b",
        "1abc",
        "",
        "a\"b",
    ] {
        // The URL is never dialed: validation fails first.
        let err =
            match PostgresProvider::connect_with_schema("postgres://localhost:1/none", bad).await {
                Err(e) => e,
                Ok(_) => panic!("hostile schema {bad:?} must be rejected"),
            };
        assert!(
            err.to_string().contains("invalid schema name"),
            "unexpected error for {bad:?}: {err}"
        );
    }
}
