//! Out-of-process control plane: submit and observe work with no local registry.
//!
//! A `Client` talks to the same database as your engine but registers no
//! workflows of its own — it only *enqueues* work and *observes* it. That is the
//! split between an API server (produces jobs, reports status) and a worker fleet
//! (runs them). Here both share one in-memory provider for a self-contained demo;
//! in production they would be separate processes pointing at the same Postgres.
//!
//! ```text
//! cargo run --example client
//! ```

use durare::{
    Client, DurableContext, DurableEngine, InMemoryProvider, ListFilter, Result, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

#[durare::step]
async fn render(ctx: &DurableContext, month: String) -> Result<String> {
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(format!("report for {month}: 42 pages"))
}

#[durare::workflow]
async fn monthly_report(ctx: DurableContext, month: String) -> Result<String> {
    render(&ctx, month).await
}

#[tokio::main]
async fn main() -> Result<()> {
    // The shared backend. In production this is a Postgres URL both sides open.
    let provider = Arc::new(InMemoryProvider::new());

    // --- The worker: registers the workflow + queue and runs jobs. ---
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register_queue(WorkflowQueue::new("reports"));
    engine.launch().await?;

    // --- The client: no registry, just the provider. ---
    let client = Client::new(provider.clone());

    // Enqueue a job the client itself does not know how to run.
    println!("[client] enqueue monthly_report for 2026-07");
    let handle = client
        .enqueue::<_, String>(
            "reports",
            "monthly_report",
            "2026-07".to_string(),
            WorkflowOptions::with_id("report-jul"),
        )
        .await?;

    // Observe the row the worker is running, purely through the client.
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: vec!["report-".to_string()],
            ..Default::default()
        })
        .await?;
    println!(
        "[client] observing {} workflow(s): {} is {}",
        rows.len(),
        rows[0].id,
        rows[0].status
    );

    // Await the result the worker produced.
    let report = handle.result().await?;
    println!("[client] result: {report}");

    // Inspect its durable steps, and re-attach to it by id later.
    let steps = client.get_workflow_steps("report-jul").await?;
    println!(
        "[client] {} step(s) recorded: {:?}",
        steps.len(),
        steps.iter().map(|s| &s.name).collect::<Vec<_>>()
    );

    let again = client.retrieve_workflow::<String>("report-jul").await?;
    println!("[client] re-retrieved by id: {}", again.result().await?);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
