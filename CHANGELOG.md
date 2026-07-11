# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Documentation

- Documented every public API item and enabled `#![warn(missing_docs)]`, now
  enforced in CI so the public surface stays fully documented.
- Rewrote the crate-level docs (the docs.rs landing page) around a tested
  example of the `#[durare::workflow]` + `start_with` path, with a capability
  map linking every major API.
- Converted all `ignore`d doc examples to compiled (most of them runnable)
  doctests, so every example in the docs is checked by `cargo test`.
- Added examples and `# Errors` sections to the hot-path APIs — `step`,
  `step_with`, `sleep`, `send`/`recv`, `set_event`/`get_event`,
  `write_stream`, `start_workflow`, `DurableEngine::start`,
  `WorkflowHandle::result`, and `Client` — plus `#[doc(alias)]`es ("cron",
  "signal", "delay", "timer") for docs.rs search.
- Added crates.io and docs.rs badges, an MSRV policy section, and a
  `CONTRIBUTING.md`.
- Added four `std`-style concept guides as public modules — `durability`
  (checkpoints, replay, and the determinism contract), `queues`, `messaging`,
  and `transactions` — each a module-level essay with tested, mostly runnable
  examples.

## [0.1.0] - 2026-07-10

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

[Unreleased]: https://github.com/SamuelXing/durare/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/SamuelXing/durare/releases/tag/v0.1.0
