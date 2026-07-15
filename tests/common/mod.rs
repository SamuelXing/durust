//! Shared helpers for the integration tests.
#![allow(dead_code)] // each test binary uses a subset

use sqlx::postgres::PgPool;

/// Swap the database name in a Postgres URL (`…/olddb?params` → `…/newdb?params`).
pub fn with_database(url: &str, dbname: &str) -> String {
    let (head, query) = match url.split_once('?') {
        Some((h, q)) => (h, Some(q)),
        None => (url, None),
    };
    let (prefix, _) = head
        .rsplit_once('/')
        .expect("postgres URL should end in /dbname");
    match query {
        Some(q) => format!("{prefix}/{dbname}?{q}"),
        None => format!("{prefix}/{dbname}"),
    }
}

/// Create a private, per-run database and return `(admin pool, url, dbname)`.
///
/// Tests whose *subject* is global mutable state — the "latest registered
/// version" above all — use this: in the shared test database any
/// concurrently launching engine (or freshly rebuilt test binary) can steal
/// "latest", so such tests get a database of their own instead of racing it.
pub async fn hermetic_pg_db(base_url: &str, prefix: &str) -> (PgPool, String, String) {
    let dbname = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPool::connect(base_url)
        .await
        .expect("connect to the base database");
    sqlx::raw_sql(&format!("CREATE DATABASE {dbname}"))
        .execute(&admin)
        .await
        .expect("create the hermetic database");
    let url = with_database(base_url, &dbname);
    (admin, url, dbname)
}

/// Best-effort teardown; `FORCE` terminates any connection a pool has not
/// finished closing yet (Postgres 13+).
pub async fn drop_hermetic_pg_db(admin: &PgPool, dbname: &str) {
    if let Err(e) = sqlx::raw_sql(&format!("DROP DATABASE {dbname} WITH (FORCE)"))
        .execute(admin)
        .await
    {
        eprintln!("hermetic db cleanup: leaving {dbname} behind: {e}");
    }
}
