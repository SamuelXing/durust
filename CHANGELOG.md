# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **(breaking)** The DBOS Conductor client — `Conductor`, `ConductorConfig`,
  `AlertHandler` — now lives behind an opt-in `conductor` cargo feature, off by
  default. Enable it with `features = ["conductor"]`. This keeps its
  `tokio-tungstenite` (TLS websocket) and `flate2` (gzip) dependencies out of
  builds that never talk to the DBOS control plane.

### Documentation

- Added a "Cargo features" section to the crate docs and the README documenting
  the opt-in `conductor` feature, and corrected the README quick-start dependency
  to `durare = "0.2"` (a `"0.1"` requirement does not resolve to 0.2.x).

## [0.2.0] - 2026-07-11

This release proves durare's on-the-wire compatibility with the other DBOS
Transact SDKs (Python, Go, TypeScript, Java) and carries one small breaking
change to keep `Result<_, Error>` cheap. `durare-macros` is unchanged and stays
at `0.1.0`.

### Added

- Cross-SDK serialization conformance tests (`tests/interop.rs`) asserting durare
  reproduces the shared DBOS golden `portable_json` strings byte-for-byte
  (encode, decode, both input-envelope orderings, structured errors, round-trip).
- End-to-end cross-SDK conformance test (`tests/interop_db.rs`, SQLite +
  Postgres) mirroring the other SDKs' direct-insert replay: portable rows are
  written to the `dbos` schema via raw SQL (as a Python/Go/TS/Java producer
  would), and durare's engine claims the `ENQUEUED` workflow, runs it (portable
  input → event → stream → consuming a portable message), and writes
  byte-identical output/event/stream.
- Conformance test that durare reads a workflow another SDK ran and *failed*:
  the portable error envelope surfaces as structured `error_info` and
  `result()` reconstructs the typed `Error::Portable`.

### Changed

- **(breaking)** `Error::Portable` now wraps a `Box<PortableWorkflowError>`
  rather than a bare `PortableWorkflowError`, so `Error` (and every
  `Result<_, Error>`) stays small after the `preserve_order` change enlarged
  `serde_json::Value`. Construct it as
  `Error::Portable(Box::new(PortableWorkflowError { … }))` or via the unchanged
  `Error::portable(name, message)` constructor; field access on a matched value
  is unaffected (the `Box` auto-derefs).

### Fixed

- **Portable serialization now preserves object key order** (enabled
  `serde_json`'s `preserve_order`). durare previously sorted object keys
  alphabetically, so its `portable_json` records — though still readable — were
  not byte-identical to those written by the Python, Go, TypeScript, and Java
  SDKs. Cross-SDK portable records are now byte-compatible.

## [0.1.1] - 2026-07-11

Documentation-only release: no library code changed, so `durare-macros` stays at
`0.1.0`. Every improvement below is visible on [docs.rs](https://docs.rs/durare).

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

[Unreleased]: https://github.com/SamuelXing/durare/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/SamuelXing/durare/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/SamuelXing/durare/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/SamuelXing/durare/releases/tag/v0.1.0
