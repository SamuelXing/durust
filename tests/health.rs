//! The readiness probe: `DurableEngine::health` reports each axis (backend,
//! dispatch) with a reason when unhealthy, across the engine lifecycle and
//! against genuinely degraded backends.

mod common;

use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

/// The dispatch axis follows the lifecycle: not launched → ready → shut down,
/// and a deactivated engine reports the deliberate state by name.
#[tokio::test]
async fn health_tracks_the_engine_lifecycle() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });

    let report = engine.health().await;
    assert!(!report.is_ready());
    assert!(report.database.is_none(), "in-memory backend is healthy");
    assert!(
        report.dispatch.as_deref().unwrap().contains("not launched"),
        "before launch: {report:?}"
    );

    engine.launch().await?;
    let report = engine.health().await;
    assert!(report.is_ready(), "launched and healthy: {report:?}");

    // A workflow runs fine on a ready engine (the probe is honest).
    engine
        .start::<(), ()>("noop", (), WorkflowOptions::with_id("wf-health-1"))
        .await?
        .await?;

    engine.shutdown(Duration::from_secs(2)).await?;
    let report = engine.health().await;
    assert!(!report.is_ready());
    assert!(
        report.dispatch.as_deref().unwrap().contains("shut down"),
        "after shutdown: {report:?}"
    );
    Ok(())
}

/// Deactivation is reported as itself — the deliberate drain state — not as
/// its side effect (aborted dispatcher tasks).
#[tokio::test]
async fn health_reports_deactivation_by_name() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.launch().await?;
    assert!(engine.health().await.is_ready());

    engine.deactivate();
    let report = engine.health().await;
    assert!(!report.is_ready());
    assert!(
        report.dispatch.as_deref().unwrap().contains("deactivated"),
        "deactivated: {report:?}"
    );
    Ok(())
}

/// The SQLite backend's ping verifies the migration ledger: current schema is
/// ready; a schema behind the binary (simulated by deleting the newest
/// applied-migration row) is reported with both versions.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_health_verifies_schema_currency() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-health-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let engine = DurableEngine::new(provider).await?;
    engine.launch().await?;
    assert!(engine.health().await.is_ready());

    // Roll the ledger back one version, as if this binary were newer than the
    // database it reconnected to.
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    sqlx::query(
        "DELETE FROM _sqlx_migrations
         WHERE version = (SELECT max(version) FROM _sqlx_migrations)",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let report = engine.health().await;
    assert!(!report.is_ready());
    assert!(
        report.database.as_deref().unwrap().contains("behind"),
        "rolled-back ledger: {report:?}"
    );

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// Losing the Postgres database mid-flight flips the database axis: the probe
/// reports the connection failure instead of erroring.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_health_reports_a_lost_database() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_health_reports_a_lost_database: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_health").await;

    let provider = Arc::new(PostgresProvider::connect(&url).await?);
    let engine = DurableEngine::new(provider).await?;
    engine.launch().await?;
    assert!(engine.health().await.is_ready());

    // The database disappears out from under the engine (failover gone wrong,
    // dropped environment). FORCE terminates the engine's live connections.
    common::drop_hermetic_pg_db(&admin, &dbname).await;

    let report = engine.health().await;
    assert!(!report.is_ready(), "lost database: {report:?}");
    assert!(report.database.is_some(), "lost database: {report:?}");

    drop(engine);
    Ok(())
}
