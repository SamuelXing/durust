#![cfg(all(feature = "sqlite", feature = "postgres"))]
//! Crash-consistency sweep: simulate a hard crash at **every** checkpoint
//! boundary of a three-step workflow and assert recovery's bookkeeping
//! exactly.
//!
//! The crash simulation: the run parks forever at the armed boundary — from
//! the database's point of view it is as dead as a killed process (its rows
//! stay `PENDING`, its checkpoints stop where they stopped) — its engine is
//! dropped, and a second engine takes over the executor id with an explicit
//! recovery. Per boundary the sweep asserts an *exact* effect count for every
//! step, not "at least once":
//!
//! - a step whose checkpoint committed before the crash **never** re-runs
//!   (count stays 1 — replay serves the recorded outcome);
//! - the single step whose effect ran but whose checkpoint did not commit
//!   re-runs (count exactly 2) — the documented at-least-once window, and the
//!   sweep shows it is exactly **one step wide** at every crash point;
//! - the workflow always completes with the right output, and the body has
//!   entered exactly twice (the crashed execution and the recovery's replay).
//!
//! Runs on SQLite unconditionally; the same sweep runs against Postgres when
//! `DATABASE_URL` is set.

use durare::{
    DurableContext, DurableEngine, EngineConfig, Error, PostgresProvider, Result, SqliteProvider,
    StateProvider, WorkflowOptions, STATUS_PENDING, STATUS_SUCCESS,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// The seven crash boundaries of a three-step workflow, in execution order.
/// `InStep(i)` parks after step *i*'s effect but before its checkpoint —
/// inside the at-least-once window. `AfterStep(i)` parks after the checkpoint
/// committed. `Entry` parks before any step.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Crash {
    Entry,
    InStep(u8),
    AfterStep(u8),
}

const BOUNDARIES: [Crash; 7] = [
    Crash::Entry,
    Crash::InStep(1),
    Crash::AfterStep(1),
    Crash::InStep(2),
    Crash::AfterStep(2),
    Crash::InStep(3),
    Crash::AfterStep(3),
];

/// Effect counters keyed by `{run_tag}/{probe}` — shared across the crashed
/// engine and the recovering engine (same process).
fn counters() -> &'static Mutex<HashMap<String, usize>> {
    static M: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    M.get_or_init(Default::default)
}
fn bump(key: String) -> usize {
    let mut m = counters().lock().unwrap();
    let n = m.entry(key).or_insert(0);
    *n += 1;
    *n
}
fn count(key: &str) -> usize {
    counters().lock().unwrap().get(key).copied().unwrap_or(0)
}

/// Park this task forever — the in-process stand-in for a process kill: the
/// future never resumes, so nothing past this point (checkpoints, status
/// writes) ever happens on the crashed execution.
async fn die() -> ! {
    std::future::pending::<()>().await;
    unreachable!()
}

enum Backend {
    Sqlite(String),
    Postgres(String),
}

impl Backend {
    async fn provider(&self) -> Result<Arc<dyn StateProvider>> {
        Ok(match self {
            Backend::Sqlite(url) => Arc::new(SqliteProvider::connect(url).await?),
            Backend::Postgres(url) => Arc::new(PostgresProvider::connect(url).await?),
        })
    }
}

/// Register the three-step workflow on `engine`. `armed` holds the boundary
/// the *next* execution should crash at (`None` = run to completion).
fn register(engine: &mut DurableEngine, name: &str, tag: String, armed: Arc<Mutex<Option<Crash>>>) {
    engine.register(name, move |ctx: DurableContext, _: ()| {
        let tag = tag.clone();
        let armed = armed.clone();
        async move {
            let crash = *armed.lock().unwrap();
            bump(format!("{tag}/body"));
            if crash == Some(Crash::Entry) {
                die().await;
            }
            for i in 1u8..=3 {
                let effect = format!("{tag}/s{i}");
                ctx.step(&format!("s{i}"), || async {
                    bump(effect);
                    if crash == Some(Crash::InStep(i)) {
                        die().await;
                    }
                    Ok::<_, Error>(())
                })
                .await?;
                if crash == Some(Crash::AfterStep(i)) {
                    die().await;
                }
            }
            Ok::<_, Error>("done".to_string())
        }
    });
}

/// The effect counts that must exist *before* the crashed engine is dropped —
/// used to wait for the run to reach its parking spot deterministically.
fn pre_crash_effects(crash: Crash) -> u8 {
    match crash {
        Crash::Entry => 0,
        Crash::InStep(i) | Crash::AfterStep(i) => i,
    }
}

async fn sweep(backend: Backend) -> Result<()> {
    for crash in BOUNDARIES {
        let tag = format!("cc-{}", uuid::Uuid::new_v4().simple());
        let wf = format!("three-step-{tag}");
        let wf_id = format!("run-{tag}");
        let exec = format!("exec-{tag}");
        let armed = Arc::new(Mutex::new(Some(crash)));

        // Execution one: run until the armed boundary, where the task parks
        // forever — then drop the engine, leaving the database exactly as a
        // crash would.
        {
            let provider = backend.provider().await?;
            let config = EngineConfig::default().executor_id(exec.as_str());
            let mut engine = DurableEngine::with_config(provider, config).await?;
            register(&mut engine, &wf, tag.clone(), armed.clone());
            let _handle = engine
                .start::<(), String>(&wf, (), WorkflowOptions::with_id(&wf_id))
                .await?;

            // Wait until the run reaches its parking spot: the boundary's
            // preceding effects have all fired…
            let want = pre_crash_effects(crash) as usize;
            for _ in 0..200 {
                let done = count(&format!("{tag}/body")) == 1
                    && (1..=3).all(|i| {
                        let c = count(&format!("{tag}/s{i}"));
                        if i as usize <= want {
                            c == 1
                        } else {
                            c == 0
                        }
                    });
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            // …and any in-flight checkpoint write has settled.
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // The crash left the row recoverable, never terminal.
        let probe = backend.provider().await?;
        assert_eq!(
            probe.get_workflow_status(&wf_id).await?.unwrap().status,
            STATUS_PENDING,
            "{crash:?}: the crashed run is left PENDING"
        );

        // Execution two: disarm, take over the dead executor, and replay.
        *armed.lock().unwrap() = None;
        let provider = backend.provider().await?;
        let config = EngineConfig::default().executor_id(format!("takeover-{tag}"));
        let mut engine = DurableEngine::with_config(provider, config).await?;
        register(&mut engine, &wf, tag.clone(), armed.clone());
        let recovered = engine
            .recover_pending_for(std::slice::from_ref(&exec))
            .await?;
        assert!(
            recovered.contains(&wf_id),
            "{crash:?}: recovery takes over the crashed executor's run"
        );
        assert_eq!(
            probe.get_workflow_status(&wf_id).await?.unwrap().status,
            STATUS_SUCCESS,
            "{crash:?}: the recovered run completes"
        );

        // The exact bookkeeping, per boundary.
        assert_eq!(
            count(&format!("{tag}/body")),
            2,
            "{crash:?}: body entered exactly twice (crashed execution + replay)"
        );
        for i in 1u8..=3 {
            let expected = if crash == Crash::InStep(i) { 2 } else { 1 };
            assert_eq!(
                count(&format!("{tag}/s{i}")),
                expected,
                "{crash:?}: step s{i} effect count — checkpointed steps never \
                 re-run; only the effect-committed/checkpoint-lost step re-runs"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn sqlite_crash_sweep_at_every_boundary() -> Result<()> {
    let mut path = std::env::temp_dir();
    path.push(format!("durare-cc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let result = sweep(Backend::Sqlite(url)).await;
    let _ = std::fs::remove_file(&path);
    result
}

#[tokio::test]
async fn pg_crash_sweep_at_every_boundary() -> Result<()> {
    let Some(url) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_crash_sweep_at_every_boundary: DATABASE_URL unset");
        return Ok(());
    };
    sweep(Backend::Postgres(url)).await
}
