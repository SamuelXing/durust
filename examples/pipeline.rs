//! Queue fan-out: enqueue many jobs, process them with bounded parallelism.
//!
//! A queue decouples *submitting* work from *running* it. Any executor listening
//! on the queue claims jobs and runs them, subject to the queue's limits:
//! `worker_concurrency` (per process), `global_concurrency` (across all
//! executors), and a `rate_limiter`. Here nine images are enqueued at once but a
//! `worker_concurrency(3)` queue only ever runs three at a time — the demo
//! prints the observed peak to prove it.
//!
//! Each job is a durable workflow, so a crash mid-batch loses no work: on
//! restart the unfinished jobs are still ENQUEUED and get picked up again.
//!
//! ```text
//! cargo run --example pipeline
//! ```

use durust::{
    DurableContext, DurableEngine, InMemoryProvider, RateLimiter, Result, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// How many jobs are running right now, and the most we ever saw at once. In a
// real service the work would be I/O; the counters just let the demo show that
// `worker_concurrency(3)` is actually enforced.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// The actual work — a durable step, checkpointed once per job.
#[durust::step]
async fn resize(ctx: &DurableContext, image: String) -> Result<u64> {
    // Pretend resizing takes a moment, so several jobs overlap.
    tokio::time::sleep(Duration::from_millis(60)).await;
    Ok(image.len() as u64 * 1024)
}

#[durust::workflow]
async fn make_thumbnail(ctx: DurableContext, image: String) -> Result<u64> {
    let running = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
    PEAK.fetch_max(running, Ordering::SeqCst);

    let bytes = resize(&ctx, image.clone()).await?;
    println!("  >> processed {image} -> {bytes} bytes  ({running} in flight)");

    IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    Ok(bytes)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // A queue with three workers. `rate_limiter` (and `global_concurrency`) are
    // the other throttles — a generous limit here so it never trips.
    engine.register_queue(
        WorkflowQueue::new("thumbnails")
            .worker_concurrency(3)
            .rate_limiter(RateLimiter {
                limit: 1000,
                period: Duration::from_secs(1),
            }),
    );
    // launch() starts the queue's dispatcher task.
    engine.launch().await?;

    // Enqueue nine jobs up front. They return immediately as ENQUEUED; the
    // dispatcher runs at most three concurrently.
    let images = [
        "cat", "dog", "bird", "fish", "frog", "newt", "mole", "wolf", "lynx",
    ];
    println!("[enqueue] {} images onto a 3-worker queue", images.len());
    let mut handles = Vec::new();
    for name in images {
        handles.push(
            engine
                .start::<_, u64>(
                    "make_thumbnail",
                    name.to_string(),
                    WorkflowOptions::with_id(format!("thumb-{name}")).queue("thumbnails"),
                )
                .await?,
        );
    }

    // Await every job's result.
    let mut total = 0u64;
    for h in handles {
        total += h.result().await?;
    }

    println!("[done] {total} bytes across all thumbnails");
    println!(
        "[peak] at most {} jobs ran at once (worker_concurrency was 3)",
        PEAK.load(Ordering::SeqCst)
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
