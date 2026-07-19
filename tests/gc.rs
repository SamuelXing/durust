//! Garbage collection: the retention delete. Terminal (and dead-letter)
//! history strictly older than the resolved cutoff goes — steps, events, and
//! streams with it — while in-flight and still-queued work survives regardless
//! of age. The cutoff is the newer of the absolute bound and the
//! `rows_threshold`-th-newest workflow's `created_at`, matching the other DBOS
//! SDKs.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

mod common;

/// A cutoff far in the future: everything already created is "older".
fn far_future_ms() -> i64 {
    chrono::Utc::now().timestamp_millis() + 3_600_000
}

async fn all_ids(engine: &DurableEngine) -> Result<Vec<String>> {
    Ok(engine
        .list_workflows(&ListFilter::default())
        .await?
        .into_iter()
        .map(|w| w.id)
        .collect())
}

/// The GC predicate on the in-memory backend (the trait's default
/// implementation): terminal history goes, in-flight and queued work survives
/// any cutoff.
#[tokio::test]
async fn gc_deletes_terminal_history_and_spares_in_flight_work() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("done", |ctx: DurableContext, (): ()| async move {
        ctx.set_event("k", "v").await?;
        Ok::<_, Error>(())
    });
    engine.register("waiter", |ctx: DurableContext, (): ()| async move {
        ctx.recv::<String>("go", Duration::from_secs(30)).await?;
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("gc-parked"));
    engine.launch().await?;

    for n in 0..2 {
        engine
            .start::<(), ()>(
                "done",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-done-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
    }
    // ENQUEUED forever: nothing listens to this queue.
    engine
        .start::<(), ()>(
            "done",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-queued".into()),
                queue: Some("gc-parked".into()),
                ..Default::default()
            },
        )
        .await?;
    // PENDING: parked on a recv.
    let waiting = engine
        .start::<(), ()>(
            "waiter",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pending".into()),
                ..Default::default()
            },
        )
        .await?;

    let deleted = engine.garbage_collect(Some(far_future_ms()), None).await?;
    assert_eq!(deleted, 2, "exactly the two terminal runs");
    let mut survivors = all_ids(&engine).await?;
    survivors.sort();
    assert_eq!(survivors, vec!["gc-pending", "gc-queued"]);

    // The survivor is intact and completes normally after collection.
    engine.send("gc-pending", "on".to_string(), "go").await?;
    waiting.await?;

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// `rows_threshold` on SQLite (the single-statement override): keep the N
/// newest, delete the rest — and the children of the deleted rows cascade.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_gc_rows_threshold_keeps_newest_and_cascades() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("step-wf", |ctx: DurableContext, (): ()| async move {
        ctx.step("record", || async { Ok::<_, Error>(1) }).await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for n in 0..5 {
        engine
            .start::<(), ()>(
                "step-wf",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-th-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
        // The threshold cutoff compares `created_at` in milliseconds; keep the
        // five runs on distinct timestamps so "the 2 newest" is well-defined.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let deleted = engine.garbage_collect(None, Some(2)).await?;
    assert_eq!(deleted, 3, "all but the 2 newest");
    let mut survivors = all_ids(&engine).await?;
    survivors.sort();
    assert_eq!(survivors, vec!["gc-th-3", "gc-th-4"]);

    // The deleted workflows' step checkpoints cascaded away with them.
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let orphans: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operation_outputs WHERE workflow_uuid = 'gc-th-0'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphans, 0, "cascade removed the step rows");
    pool.close().await;

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// With both bounds given, the more restrictive (newer) cutoff wins: a
/// harmless absolute cutoff plus `rows_threshold = 1` still trims to the
/// single newest workflow.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_gc_more_restrictive_bound_wins() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for n in 0..3 {
        engine
            .start::<(), ()>(
                "noop",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-mix-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Absolute cutoff 0 would delete nothing on its own; the rows bound is
    // newer and takes precedence.
    let deleted = engine.garbage_collect(Some(0), Some(1)).await?;
    assert_eq!(deleted, 2);
    assert_eq!(all_ids(&engine).await?, vec!["gc-mix-2"]);

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// The edges: no bounds is a no-op, a non-positive threshold is an error, and
/// a threshold larger than the table (with no absolute cutoff) collects
/// nothing.
#[tokio::test]
async fn gc_edge_cases() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>("noop", (), WorkflowOptions::default())
        .await?
        .await?;

    assert_eq!(engine.garbage_collect(None, None).await?, 0, "no bounds");
    assert!(engine.garbage_collect(None, Some(0)).await.is_err());
    assert!(engine.garbage_collect(None, Some(-3)).await.is_err());
    assert_eq!(
        engine.garbage_collect(None, Some(100)).await?,
        0,
        "fewer rows than the threshold"
    );
    assert_eq!(all_ids(&engine).await?.len(), 1, "nothing was collected");

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// The Postgres override, end to end in a hermetic database: cutoff
/// semantics, survivors, and step-row cascade.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_gc_deletes_terminal_history_and_cascades() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_gc_deletes_terminal_history_and_cascades: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_gc").await;

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register("step-wf", |ctx: DurableContext, (): ()| async move {
        ctx.step("record", || async { Ok::<_, Error>(1) }).await?;
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("gc-parked"));
    engine.launch().await?;

    engine
        .start::<(), ()>(
            "step-wf",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pg-done".into()),
                ..Default::default()
            },
        )
        .await?
        .await?;
    engine
        .start::<(), ()>(
            "step-wf",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pg-queued".into()),
                queue: Some("gc-parked".into()),
                ..Default::default()
            },
        )
        .await?;

    let deleted = engine.garbage_collect(Some(far_future_ms()), None).await?;
    assert_eq!(deleted, 1, "the completed run only");
    assert_eq!(all_ids(&engine).await?, vec!["gc-pg-queued"]);

    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    let orphans: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dbos.operation_outputs WHERE workflow_uuid = 'gc-pg-done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphans, 0, "cascade removed the step rows");
    pool.close().await;

    engine.shutdown(Duration::from_secs(2)).await?;
    drop(engine);
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}
