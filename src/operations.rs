//! Running durare in production: connections, pool sizing, and the resource
//! model.
//!
//! The engine is a library in your process — there is no server to size, only
//! the database connections it uses. This guide is the cost model: what
//! durare opens, who holds a connection for how long, and how to size pools
//! for one process and for a fleet.
//!
//! # What durare opens
//!
//! One connection pool per provider, shared by everything in the process —
//! the engine's checkpoints, queue dispatchers, the admin server and
//! conductor client (they call engine methods), and any application queries
//! you run through the provider's `pool()` accessor.
//!
//! | Backend | Default pool | Notes |
//! |---|---|---|
//! | Postgres | 8 connections | the `LISTEN`/`NOTIFY` listener holds **one** for its lifetime, leaving 7 for queries |
//! | SQLite | 5 connections | WAL journaling, `synchronous = NORMAL`, a 5s busy timeout |
//!
//! Both defaults suit development and modest services. For deliberate sizing,
//! build the pool yourself and hand it over with `from_pool` — for Postgres,
//! set the `search_path` so the system tables resolve in the `dbos` schema
//! the other DBOS SDKs share:
//!
//! ```no_run
//! # use durare::{PostgresProvider, Result};
//! # async fn run() -> Result<()> {
//! use std::str::FromStr;
//! let opts = sqlx::postgres::PgConnectOptions::from_str(
//!     "postgres://user:pass@db.internal:5432/app",
//! )?
//! .options([("search_path", "dbos")]);
//! let pool = sqlx::postgres::PgPoolOptions::new()
//!     .max_connections(24)
//!     .connect_with(opts)
//!     .await?;
//! let provider = PostgresProvider::from_pool(pool);
//! # let _ = provider;
//! # Ok(())
//! # }
//! ```
//!
//! # Who holds a connection, and for how long
//!
//! Sizing follows from the hold times, not from workflow counts:
//!
//! - **Ordinary steps** acquire a connection per checkpoint write —
//!   milliseconds. Hundreds of concurrent workflows share a small pool
//!   comfortably, because a workflow between steps holds *nothing*.
//! - **Transactional steps** ([`transaction`](crate::DurableContext::transaction))
//!   hold one connection for the whole body, retries included. These are your
//!   pool's real occupants: budget one connection per transactional step you
//!   expect to run concurrently.
//! - **Blocked `recv` / `get_event`** hold no connection on Postgres — the
//!   `LISTEN`/`NOTIFY` listener wakes them, and each re-check is a brief
//!   acquire. (On backends without push, waiters poll every 25ms, still
//!   holding nothing between polls.)
//! - **Queue dispatchers** run one claim query per queue per poll interval;
//!   the schedule reconciler adds one every 500ms. Rounding noise.
//!
//! The per-process rule of thumb:
//!
//! ```text
//! max_connections ≥ peak concurrent transactional steps
//!                 + a few for ordinary-step bursts and dispatchers  (2–4)
//!                 + 1 for the LISTEN/NOTIFY listener (Postgres)
//! ```
//!
//! Queue [`worker_concurrency`](crate::WorkflowQueue::worker_concurrency)
//! bounds how many queued runs this process executes at once, which in turn
//! bounds their concurrent steps — the two knobs work together: concurrency
//! caps demand, the pool caps supply. A pool smaller than demand doesn't
//! break anything; steps queue on `acquire` and throughput flattens.
//!
//! # The fleet math
//!
//! Postgres defaults to `max_connections = 100`, and every durare executor
//! brings its whole pool:
//!
//! ```text
//! executors × pool size  ≤  Postgres max_connections − everything else
//! ```
//!
//! Ten executors at the default 8 already claim 80 connections. Scaling out
//! usually means sizing each process's pool *down* (queue concurrency limits
//! are fleet-wide, so more executors don't need proportionally more
//! connections) or putting a server-side pooler (e.g. PgBouncer in session
//! mode) in front. Session mode matters: the provider relies on
//! per-connection state (`search_path`, `LISTEN`), which transaction-mode
//! pooling would break.
//!
//! # Statement caching
//!
//! Every pooled connection keeps a prepared-statement cache (sqlx's default:
//! 100 statements). durare's own query set is far smaller, so the default is
//! never the bottleneck; if your application shares the pool and runs many
//! distinct queries, raise it on the connect options when building a
//! `from_pool` pool.
//!
//! # SQLite specifics
//!
//! The SQLite backend is the light option — one file, no server, the same
//! durability contract. Its shape to plan around:
//!
//! - **One writer at a time.** WAL mode lets readers proceed under a writer,
//!   but writes serialize; the 5-second busy timeout absorbs contention, and
//!   transactional steps additionally retry `SQLITE_BUSY`/`SQLITE_LOCKED`
//!   conflicts until they clear (or the workflow is cancelled). Throughput
//!   ceilings are write-rate ceilings.
//! - **No push signals**: blocked `recv`/`get_event` poll (25ms), so a very
//!   large number of concurrent waiters is cheaper on Postgres.
//! - It is a per-process database: two processes can share the file, but a
//!   fleet belongs on Postgres.
//!
//! # History retention
//!
//! Every workflow leaves rows behind — its status, step checkpoints, events,
//! and streams — and nothing removes them automatically: the history tables
//! grow without bound until you decide otherwise. That is deliberate
//! (history is the audit trail and the recovery substrate), but a production
//! deployment needs a retention decision, because table and index bloat on
//! `workflow_status` eventually taxes the hottest queries in the system —
//! the queue dispatchers'.
//!
//! [`garbage_collect`](crate::DurableEngine::garbage_collect) is the
//! trimming primitive, with the same semantics as the other DBOS SDKs: pass
//! an absolute `created_at` cutoff, a keep-the-newest-N rows bound, or both
//! (the newer cutoff wins), and everything terminal that falls outside is
//! deleted — in-flight and queued work survives regardless of age. Run it
//! from a scheduled workflow, a cron job against the admin server's
//! `POST /dbos-garbage-collect`, or let the DBOS console's retention policy
//! drive it through the conductor. Deleted history is unrecoverable; export
//! what you need first
//! ([`export_workflow`](crate::DurableEngine::export_workflow)).
//!
//! # Watching it
//!
//! The [`observability`](crate::observability) guide covers the runtime
//! signals this guide's numbers should be checked against:
//! [`metrics`](crate::DurableEngine::metrics) exposes queue depth and
//! in-flight runs (a persistently deep queue with idle workers suggests the
//! pool, not the workers, is the limit), and [`health`](crate::DurableEngine::health)
//! reports a backend that stopped answering.

// This module is documentation only.
