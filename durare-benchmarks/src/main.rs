//! Performance harness for the durare durable-execution SDK.
//!
//! Kept out of the durare workspace (own `[workspace]` in Cargo.toml) so its
//! dependencies and long run times never touch the SDK's build or CI.
//!
//! Two workloads today:
//!
//! * `steps` — a workflow of N sequential transaction-steps (read + write),
//!   timing end-to-end workflow duration. This mirrors the upstream
//!   `dbos-workflow-benchmarks` `benchmarkWorkflow`, so the numbers line up with
//!   the DBOS Go/Python/TypeScript SDKs on the same workload and database.
//! * `memory` — resident-set growth per in-flight (durably parked) workflow. The
//!   metric a compiled, GC-free async runtime is expected to win: each parked
//!   workflow is a small async state machine holding no database connection,
//!   versus a goroutine stack or a Python coroutine object.
//! * `concurrent` — N workflows through a bounded concurrency window:
//!   per-workflow latency percentiles under load, where a runtime's tail
//!   behavior (scheduling, GC pauses elsewhere) actually shows.
//! * `serve` — the upstream `dbos-benchmark-app`'s HTTP contract (`GET /:num`
//!   → `{output, runtime}`, runtime in server-side ms), so the upstream
//!   `benchmark_dbos.py` driver runs against durare **unmodified**.
//!
//! Honest framing (see README): DBOS workflow throughput is dominated by
//! Postgres round-trips, not the language runtime, so `steps` largely measures
//! the database. `memory` and tail latency under load are where the runtime
//! choice actually shows.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use durare::{params, DurableContext, DurableEngine, Error, PostgresProvider, WorkflowOptions};
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "Performance benchmarks for the durare durable-execution SDK")]
struct Cli {
    /// Postgres connection URL (falls back to $DATABASE_URL). Use a throwaway
    /// database — the benchmarks create tables and leave workflow rows behind.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// A workflow of N sequential transaction-steps (read + write), timing
    /// end-to-end workflow duration. Mirrors dbos-workflow-benchmarks.
    Steps {
        /// Transaction-steps per workflow (upstream's functions-per-workflow, -i).
        #[arg(long, default_value_t = 10)]
        steps: i64,
        /// How many workflow runs to time (upstream's -n).
        #[arg(long, default_value_t = 50)]
        iterations: usize,
    },
    /// Memory per in-flight workflow: park N workflows on a durable sleep and
    /// report resident-set growth per workflow.
    Memory {
        /// Number of concurrently-parked workflows.
        #[arg(long, default_value_t = 1000)]
        count: usize,
    },
    /// N workflows through a bounded concurrency window: per-workflow latency
    /// percentiles and total throughput under load — where tail behavior shows.
    Concurrent {
        /// Total workflows to run.
        #[arg(long, default_value_t = 200)]
        workflows: usize,
        /// How many run at once.
        #[arg(long, default_value_t = 32)]
        concurrency: usize,
        /// Transaction-steps per workflow.
        #[arg(long, default_value_t = 5)]
        steps: i64,
    },
    /// Serve the upstream dbos-benchmark-app's HTTP contract (GET /:num →
    /// {output, runtime}) so dbos-workflow-benchmarks' benchmark_dbos.py can
    /// drive durare unmodified.
    Serve {
        /// Port to listen on.
        #[arg(long, default_value_t = 18808)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Steps { steps, iterations } => {
            steps_workload(&cli.database_url, steps, iterations).await
        }
        Cmd::Memory { count } => memory_workload(&cli.database_url, count).await,
        Cmd::Concurrent {
            workflows,
            concurrency,
            steps,
        } => concurrent_workload(&cli.database_url, workflows, concurrency, steps).await,
        Cmd::Serve { port } => serve_workload(&cli.database_url, port).await,
    }
}

async fn build_engine(url: &str) -> Result<DurableEngine> {
    let provider = PostgresProvider::connect(url)
        .await
        .context("connect to Postgres")?;
    DurableEngine::new(Arc::new(provider))
        .await
        .context("build engine (runs migrations)")
}

fn rss_bytes() -> usize {
    memory_stats::memory_stats()
        .map(|s| s.physical_mem)
        .unwrap_or(0)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// --- `steps` workload: mirrors dbos-workflow-benchmarks ---------------------

/// Register the upstream-mirroring workflows: `bench_steps` (N sequential
/// read-then-write transactions on one counter row), `bench_setup` (its
/// schema), and `bench_sleeper` (durably parked, for memory measurements).
/// Shared by the `steps`, `concurrent`, and `serve` modes.
fn register_bench(engine: &mut DurableEngine) {
    engine.register("bench_sleeper", |ctx: DurableContext, _: ()| async move {
        ctx.sleep(Duration::from_secs(3600)).await?;
        Ok::<_, Error>(())
    });
    // The benchmark workflow: `n` sequential transactions, each a read-then-write
    // on a shared counter row — matching upstream's benchmarkWorkflow, whose
    // benchmarkTransaction reads a greet_count and writes it back incremented.
    engine.register("bench_steps", |ctx: DurableContext, n: i64| async move {
        let mut last = 0i64;
        for _ in 0..n {
            last = ctx
                .transaction::<i64, _>("bench_txn", |tx| {
                    Box::pin(async move {
                        let row = tx
                            .query_one(
                                "SELECT greet_count FROM bench_hello WHERE name = 'dbos'",
                                &params![],
                            )
                            .await?;
                        let count = row.get::<i64>("greet_count");
                        tx.execute(
                            "UPDATE bench_hello SET greet_count = ? WHERE name = 'dbos'",
                            &params![count + 1],
                        )
                        .await?;
                        Ok(count)
                    })
                })
                .await?;
        }
        Ok::<_, Error>(last)
    });

    // One-time schema setup as a transaction (DDL is transactional on Postgres).
    engine.register("bench_setup", |ctx: DurableContext, _: ()| async move {
        ctx.transaction::<(), _>("setup", |tx| {
            Box::pin(async move {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS bench_hello \
                     (name TEXT PRIMARY KEY, greet_count BIGINT NOT NULL)",
                    &params![],
                )
                .await?;
                tx.execute(
                    "INSERT INTO bench_hello (name, greet_count) VALUES ('dbos', 0) \
                     ON CONFLICT (name) DO NOTHING",
                    &params![],
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok::<_, Error>(())
    });
}

async fn steps_workload(url: &str, steps: i64, iterations: usize) -> Result<()> {
    let mut engine = build_engine(url).await?;
    register_bench(&mut engine);

    engine.launch().await?;

    // Setup + one warm-up run (primes the connection pool and query plans).
    run_steps(&engine, 0).await?; // setup
    run_steps(&engine, steps).await?; // warm-up

    let mut hist = Histogram::<u64>::new(3).expect("histogram");
    let overall = Instant::now();
    for _ in 0..iterations {
        let t = Instant::now();
        run_steps(&engine, steps).await?;
        hist.record(t.elapsed().as_micros() as u64).ok();
    }
    let total = overall.elapsed();

    let ms = |micros: f64| micros / 1000.0;
    println!("durare — steps workload (mirrors dbos-workflow-benchmarks)");
    println!("  transaction-steps / workflow : {steps}");
    println!("  timed iterations             : {iterations}");
    println!(
        "  workflow duration  p50       : {:.2} ms",
        ms(hist.value_at_quantile(0.50) as f64)
    );
    println!(
        "  workflow duration  p99       : {:.2} ms",
        ms(hist.value_at_quantile(0.99) as f64)
    );
    println!("  workflow duration  mean      : {:.2} ms", ms(hist.mean()));
    println!(
        "  per-step (mean / steps)      : {:.2} ms",
        ms(hist.mean()) / steps.max(1) as f64
    );
    println!(
        "  throughput                   : {:.0} workflows/s",
        iterations as f64 / total.as_secs_f64()
    );

    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

async fn run_steps(engine: &DurableEngine, steps: i64) -> Result<()> {
    if steps == 0 {
        engine
            .start::<(), ()>("bench_setup", (), WorkflowOptions::default())
            .await?
            .result()
            .await?;
        return Ok(());
    }
    engine
        .start::<i64, i64>("bench_steps", steps, WorkflowOptions::default())
        .await?
        .result()
        .await?;
    Ok(())
}

// --- `memory` workload: RSS per in-flight workflow --------------------------

async fn memory_workload(url: &str, count: usize) -> Result<()> {
    let mut engine = build_engine(url).await?;

    // The parked workflow (`bench_sleeper`, registered with the bench set):
    // in-flight and checkpointed, holding an async state machine but no
    // database connection.
    register_bench(&mut engine);
    engine.launch().await?;

    // Baseline taken after the engine and pool are established, so the delta is
    // the marginal cost of the parked workflows, not the runtime's fixed floor.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let baseline = rss_bytes();

    // Start `count` workflows without awaiting them — each parks on the durable
    // sleep. Handles are held so the observers stay alive alongside the tasks.
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        handles.push(
            engine
                .start::<(), ()>("bench_sleeper", (), WorkflowOptions::default())
                .await?,
        );
    }

    // Let them settle into the parked state before measuring.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = rss_bytes();
    let delta = after.saturating_sub(baseline);

    println!("durare — memory per in-flight workflow");
    println!("  in-flight workflows          : {count}");
    println!("  RSS baseline                 : {:.1} MiB", mib(baseline));
    println!("  RSS with {count} parked        : {:.1} MiB", mib(after));
    println!("  growth                       : {:.1} MiB", mib(delta));
    println!(
        "  per in-flight workflow       : {:.2} KiB",
        delta as f64 / count.max(1) as f64 / 1024.0
    );

    engine.shutdown(Duration::from_millis(500)).await.ok();
    Ok(())
}

// --- `serve` mode: the upstream benchmark app's HTTP contract ---------------

/// Serve `GET /:num` with the upstream `dbos-benchmark-app` response shape —
/// `{ output, runtime }`, `runtime` being the server-side workflow duration in
/// milliseconds — so `dbos-workflow-benchmarks/benchmarks/benchmark_dbos.py`
/// drives durare with no modification:
///
/// ```text
/// durare-benchmarks serve --port 18808
/// python3 benchmarks/benchmark_dbos.py -u http://127.0.0.1:18808 -n 50 -i 10
/// ```
async fn serve_workload(url: &str, port: u16) -> Result<()> {
    let mut engine = build_engine(url).await?;
    register_bench(&mut engine);
    engine.launch().await?;
    let engine = Arc::new(engine);
    run_steps(&engine, 0).await?; // schema setup
    run_steps(&engine, 1).await?; // warm-up: pool + query plans

    let app = axum::Router::new()
        .route("/park/:count", axum::routing::get(park_handler))
        .route("/:num", axum::routing::get(bench_handler))
        .with_state(engine);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .context("bind benchmark server")?;
    println!(
        "durare benchmark app on http://{} — GET /:num (upstream dbos-workflow-benchmarks contract)",
        listener.local_addr()?
    );
    axum::serve(listener, app).await.context("serve")?;
    Ok(())
}

async fn bench_handler(
    axum::extract::Path(num): axum::extract::Path<i64>,
    axum::extract::State(engine): axum::extract::State<Arc<DurableEngine>>,
) -> axum::Json<serde_json::Value> {
    // Time exactly what the upstream handler times: workflow invocation to
    // completion, inside the server (no HTTP in the measured window).
    let start = Instant::now();
    let result = run_steps(&engine, num).await;
    let runtime = start.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(()) => axum::Json(serde_json::json!({
            "output": format!("ran {num} transaction-steps"),
            "runtime": runtime,
        })),
        Err(e) => {
            // A failed run must not silently blend into the latency sample.
            eprintln!("benchmark workflow failed: {e:#}");
            axum::Json(serde_json::json!({ "error": e.to_string(), "runtime": runtime }))
        }
    }
}

// --- `concurrent` workload: tail latency under load --------------------------

/// Run `workflows` workflows of `steps` transaction-steps each, at most
/// `concurrency` in flight at once, and report per-workflow latency
/// percentiles plus total throughput. Latency here includes time spent
/// waiting on the shared connection pool — that contention is the point.
async fn concurrent_workload(
    url: &str,
    workflows: usize,
    concurrency: usize,
    steps: i64,
) -> Result<()> {
    let mut engine = build_engine(url).await?;
    register_bench(&mut engine);
    engine.launch().await?;
    let engine = Arc::new(engine);
    run_steps(&engine, 0).await?; // schema setup
    run_steps(&engine, steps).await?; // warm-up

    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let hist = Arc::new(std::sync::Mutex::new(
        Histogram::<u64>::new(3).expect("histogram"),
    ));
    let overall = Instant::now();
    let mut tasks = Vec::with_capacity(workflows);
    for _ in 0..workflows {
        let engine = engine.clone();
        let sem = sem.clone();
        let hist = hist.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore");
            let t = Instant::now();
            let out = run_steps(&engine, steps).await;
            hist.lock()
                .unwrap()
                .record(t.elapsed().as_micros() as u64)
                .ok();
            out
        }));
    }
    for t in tasks {
        t.await.expect("benchmark task")?;
    }
    let total = overall.elapsed();

    let hist = hist.lock().unwrap();
    let ms = |micros: f64| micros / 1000.0;
    println!("durare — concurrent workload (tail latency under load)");
    println!("  workflows                    : {workflows}");
    println!("  concurrency window           : {concurrency}");
    println!("  transaction-steps / workflow : {steps}");
    println!(
        "  workflow latency  p50        : {:.2} ms",
        ms(hist.value_at_quantile(0.50) as f64)
    );
    println!(
        "  workflow latency  p99        : {:.2} ms",
        ms(hist.value_at_quantile(0.99) as f64)
    );
    println!(
        "  workflow latency  max        : {:.2} ms",
        ms(hist.max() as f64)
    );
    println!(
        "  throughput                   : {:.0} workflows/s",
        workflows as f64 / total.as_secs_f64()
    );

    engine.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// `GET /park/:count` — start `count` durably-parked workflows and return this
/// process's pid, so an external harness can measure RSS-per-workflow the same
/// way across SDKs (`ps -o rss= -p <pid>` before and after).
async fn park_handler(
    axum::extract::Path(count): axum::extract::Path<usize>,
    axum::extract::State(engine): axum::extract::State<Arc<DurableEngine>>,
) -> axum::Json<serde_json::Value> {
    for _ in 0..count {
        if let Err(e) = engine
            .start::<(), ()>("bench_sleeper", (), WorkflowOptions::default())
            .await
        {
            return axum::Json(serde_json::json!({ "error": e.to_string() }));
        }
    }
    axum::Json(serde_json::json!({ "parked": count, "pid": std::process::id() }))
}
