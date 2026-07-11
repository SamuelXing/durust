# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

First release. A DBOS-compatible durable-execution SDK for Rust: write ordinary
async code, checkpoint every step to Postgres or SQLite, and resume unfinished
workflows after a crash.

### Added

- Durable workflows and steps — `#[durare::workflow]`, `#[durare::step]`, and
  `ctx.step` / `ctx.step_with` with exponential-backoff retry policies.
- Transactions — `#[durare::transaction]` commits SQL and its checkpoint in one
  database transaction, making the step exactly-once.
- Durable timers — `ctx.sleep` with a persisted wake instant that does not drift
  across restarts.
- Queues — per-process and global concurrency limits, rate limiting, priorities,
  delayed enqueue, deduplication, and partitioned queues.
- Scheduling — six-field cron via `#[durare::workflow(schedule = "…")]`, plus a
  managed schedule API (create, pause, resume, trigger, backfill).
- Messaging, events, and streams — durable FIFO `send` / `recv`, idempotency-key
  sends, `set_event` / `get_event`, and append-only streams a consumer can tail.
- Child workflows — `ctx.start_workflow` with deterministic ids and parent links.
- Recovery and versioning — `recover()` by application version, a version
  registry for fleet routing, and a recovery-attempt cap.
- Management — list, cancel, resume, and fork (from an arbitrary step) workflows;
  per-workflow timeouts; `ctx.patch` for evolving in-flight workflows; debouncing.
- Operations — an admin HTTP server with the standard DBOS endpoints, and a DBOS
  Conductor client.
- A registry-free `Client` for out-of-process producers.
- Backends — Postgres, SQLite, and in-memory, behind one `StateProvider` trait.
- DBOS compatibility — state is stored in the `dbos` system schema with the same
  tables the DBOS Transact SDKs use, plus a portable cross-SDK serialization
  envelope.

[Unreleased]: https://github.com/SamuelXing/durare
