//! Postgres backend tests. Skipped unless `DATABASE_URL` points at a reachable
//! Postgres instance (ideally an empty database — `init` runs the migrations).
//!
//!   createdb durust_test && DATABASE_URL=postgres://localhost/durust_test cargo test --test postgres

use durust::{
    DurableContext, DurableEngine, Error, ErrorCode, PostgresProvider, Result, Serializer,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty())
}

async fn engine_with(url: &str, fmt: Serializer) -> Result<DurableEngine> {
    let provider = PostgresProvider::connect(url).await?.with_serializer(fmt);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("greet", |ctx: DurableContext, name: String| async move {
        let msg = ctx
            .step("build", || async { Ok::<_, Error>(format!("hi {name}")) })
            .await?;
        Ok::<_, Error>(msg)
    });
    Ok(engine)
}

/// Round-trip a workflow's input/step-output/result through Postgres, and prove
/// a provider in a different serialization format still decodes them.
#[tokio::test]
async fn pg_serialization_cross_format() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_serialization_cross_format: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("wf-ser-{}", uuid::Uuid::new_v4());

    {
        let engine = engine_with(&url, Serializer::Portable).await?;
        let out: String = engine
            .run_workflow::<_, String>("greet", "ada".to_string(), WorkflowOptions::with_id(&id))
            .await?
            .get_result()
            .await?;
        assert_eq!(out, "hi ada");
    }
    {
        let engine = engine_with(&url, Serializer::Json).await?;
        let mut handle = engine.retrieve_workflow::<String>(&id).await?;
        let status = handle.get_status().await?;
        assert_eq!(status.input, serde_json::json!("ada"));
        assert_eq!(status.output, Some(serde_json::json!("hi ada")));
        assert_eq!(handle.get_result().await?, "hi ada");
    }
    Ok(())
}

/// The run identity round-trips through Postgres: the three auth columns are
/// written on insert (roles as a JSON array in the text column), read back into
/// the status, threaded into the workflow context, and copied onto a fork.
#[tokio::test]
async fn pg_auth_context_round_trip() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_auth_context_round_trip: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("whoami", |ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(format!(
            "{}/{}/{}",
            ctx.authenticated_user().unwrap_or("-"),
            ctx.assumed_role().unwrap_or("-"),
            ctx.authenticated_roles().join(","),
        ))
    });

    let id = format!("wf-auth-{tag}");
    let opts = WorkflowOptions::with_id(&id)
        .authenticated_user("alice")
        .assumed_role("admin")
        .authenticated_roles(["admin", "user"]);
    let mut handle = engine.run_workflow::<_, String>("whoami", (), opts).await?;
    assert_eq!(handle.get_result().await?, "alice/admin/admin,user");

    let status = handle.get_status().await?;
    assert_eq!(status.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(status.assumed_role.as_deref(), Some("admin"));
    assert_eq!(status.authenticated_roles, vec!["admin", "user"]);

    let fork_id = format!("wf-auth-fork-{tag}");
    let forked = engine
        .fork_workflow::<String>(&id, 0, WorkflowOptions::with_id(&fork_id))
        .await?;
    let fstatus = forked.get_status().await?;
    assert_eq!(fstatus.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(fstatus.authenticated_roles, vec!["admin", "user"]);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Postgres surfaces the dedup unique-violation and the destination FK violation
/// as typed, classifiable errors (verifies the sqlx Postgres driver mapping).
#[tokio::test]
async fn pg_typed_db_errors() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_typed_db_errors: DATABASE_URL unset");
        return Ok(());
    };
    let tag = uuid::Uuid::new_v4();
    let provider = PostgresProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new(format!("q-{tag}")));

    let dedup = format!("once-{tag}");
    let mut opts = WorkflowOptions::with_id(format!("wf-a-{tag}"));
    opts.dedup_id = Some(dedup.clone());
    engine
        .enqueue::<_, ()>(&format!("q-{tag}"), "noop", (), opts)
        .await?;

    let mut opts = WorkflowOptions::with_id(format!("wf-b-{tag}"));
    opts.dedup_id = Some(dedup);
    let err = match engine
        .enqueue::<_, ()>(&format!("q-{tag}"), "noop", (), opts)
        .await
    {
        Ok(_) => panic!("dedup reuse must be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.code(), ErrorCode::QueueDeduplicated);

    let err = engine
        .send(&format!("ghost-{tag}"), 1_i64, "topic")
        .await
        .expect_err("send to nonexistent workflow must fail");
    assert_eq!(err.code(), ErrorCode::NonExistentWorkflow);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
