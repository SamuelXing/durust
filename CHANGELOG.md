# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Metrics snapshot: `DurableEngine::metrics()` returns an `EngineMetrics` —
  poll-style, like tokio's runtime metrics, so no metrics-system choice is
  made for you and no dependency is added. Gauges: in-flight workflow runs on
  this process, `ENQUEUED` depth per registered queue (fleet-wide, stable
  keys). Process-lifetime counters: workflows recovered, step retries,
  dead-lettered workflows, failed dequeue polls. Wiring examples in the
  `observability` guide's new Metrics section.
- Readiness probe: `DurableEngine::health()` returns a `HealthReport` with a
  reason per unhealthy axis — the state backend (reachable, dbos schema
  present and migration-current, via the new `StateProvider::ping` method,
  default healthy) and dispatch (launched, not deactivated, not shut down,
  every dispatcher task alive). Never fails; failures are the report's
  content. The admin server serves it as `GET /readyz` (`200`/`503` with the
  per-axis report) — a durare extension alongside the cross-SDK static
  `GET /dbos-healthz` liveness probe, so an orchestrator can drain a
  deactivated process without restarting it.

## [0.3.3] - 2026-07-15

Observability and DBOS-console compatibility: the engine now emits `tracing`
spans around every workflow and step, and the conductor client works against
the live DBOS console — connecting a demo app to it surfaced (and fixed) a
dead documented endpoint and a wire-shape incompatibility. `durare-macros` is
unchanged and stays at `0.1.0`.

### Added

- **Tracing spans.** The engine now emits `tracing` spans around every
  workflow execution (direct, queued, scheduled, child, and recovery runs)
  and every durable operation (`step`, `step_with`, `transaction`), carrying
  the DBOS trace attributes (`dbos.operation.workflow_id`,
  `dbos.application.version`, `dbos.executor.id`, `dbos.queue.name`, the
  user identity, and the recorded outcome). Step spans nest under their
  workflow span, child workflows under their parent, and replayed steps are
  marked `dbos.step.replayed = true` — so a post-crash trace shows exactly
  which steps were served from checkpoints. Spans follow the
  `tracing-opentelemetry` conventions (`otel.name`, `otel.status_code`), so
  bridging them to an OTLP exporter needs no engine configuration. See the
  new `observability` module guide.

### Changed

- An empty `ConductorConfig::url` now defaults to the hosted DBOS conductor,
  `wss://cloud.dbos.dev/conductor/v1alpha1` (the domain honors the
  `DBOS_DOMAIN` env var), matching the Go and Python SDKs — previously an
  empty URL was rejected.

### Fixed

- The conductor documentation pointed at `wss://conductor.dbos.dev`, a
  hostname that does not exist — following it produced an endless
  DNS-failure retry loop. The real endpoint is the default above.
- The conductor now tolerates explicit JSON `null` for list-typed request
  fields (`workflow_uuids`, `workflow_ids`, `executor_ids`). The conductor
  service marshals absent lists as `null`, so the console's very first
  workflow-list query failed with `serialization error: invalid type: null,
  expected a sequence` — every list view in the console was broken against
  a Rust process. Found connecting a demo app to the live console.

## [0.3.2] - 2026-07-13

Recovery ergonomics and shutdown correctness: launch can now (opt-in) resume
the work a previous run left pending, and shutdown promptly stops the
background loops and genuinely drains every in-flight run — including recovered
ones. `durare-macros` is unchanged and stays at `0.1.0`.

One compatibility note: `EngineConfig` gained a public field
(`recover_on_launch`), which is technically breaking for code constructing it
as an exhaustive struct literal. The documented construction path —
`EngineConfig::default()` plus setters — and `..Default::default()` literals
are unaffected, and no such literal usage is known. (Marking the config structs
`#[non_exhaustive]` is queued for the pre-1.0 API review, so field additions
stop being breaking at all.)

### Added

- Opt-in recovery on launch: `EngineConfig::recover_on_launch(true)` (or the
  builder's `recover_on_launch(true)`) makes `DurableEngine::launch` recover this
  executor's workflows left pending by a previous run, re-dispatching them on a
  background task — so a crash and restart resumes unfinished work without a
  separate `recover()` call. **Off by default** (no behavior change): it is
  opt-in because it is only sound when each live process has a *unique* executor
  id — recovering "this executor's" pending work assumes the previous owner is
  gone, not running concurrently. Enable it for a single-process app, or when you
  set a distinct `DBOS__VMID` per process; otherwise keep driving recovery
  yourself with `recover()`. (A future release may default it on once recovery
  is liveness-aware.) Recovery honors the graceful-shutdown contract: runs it
  re-dispatches count as in-flight, so `shutdown` drains them, and a shutdown
  that begins mid-recovery stops further dispatch — the run in flight finishes,
  the untouched remainder stays pending for a later recovery.

### Changed

- `shutdown` now stops the background loops promptly: they are signalled through
  a cancellation token they await, instead of a flag they polled between
  iterations — previously a queue dispatcher asleep on its poll interval would
  not notice shutdown until it woke (up to the queue's base polling interval).
  In-flight runs are likewise drained through a task tracker that counts a run
  from the moment it is spawned. Internal modernization (`tokio-util`'s
  `CancellationToken` + `TaskTracker`); no API change.

## [0.3.1] - 2026-07-12

### Added

- `Error::MaxRecoveryAttemptsExceeded` and the matching
  `ErrorCode::MaxRecoveryAttemptsExceeded`: a workflow that exceeds its
  recovery-attempt cap and is parked in the `MAX_RECOVERY_ATTEMPTS_EXCEEDED`
  dead-letter state now surfaces this typed error when its result is awaited, so
  a caller can distinguish a parked workflow from one that ran to completion.
- The queue registry is now persisted to the `queues` table on `launch` — the
  database-backed, fleet-wide registry the DBOS conductor and control plane read —
  and `DurableEngine::list_queues()` reads it back. A queue registered by any
  executor against a shared database is visible to every conductor pointed at it,
  matching the Go and Python SDKs. The write is version-gated and resolved on
  launch: a process self-elects as latest when it first registers its version (so
  its queue config lands on the first launch), and an already-registered
  older-version straggler will not overwrite a newer queue's configuration.
- Durable `ctx.now()`, `ctx.uuid()`, and `ctx.random()`: read the wall clock,
  mint a v4 UUID, or draw an `f64` in `[0, 1)` inside a workflow and have the
  value **checkpointed** — recorded on first execution and replayed identically
  after a recovery, instead of silently breaking determinism the way a bare
  `Utc::now()` / `Uuid::new_v4()` would. Each consumes one step slot, like
  `ctx.sleep`.

### Changed

- The Conductor client's queue views (`list_queues` / `get_queue`) now read the
  database-backed `queues` table (fleet-wide) rather than this process's in-memory
  registry, so a conductor sees queues registered by every executor. The admin
  server's `/dbos-workflow-queues-metadata` still reports the local in-process
  registry (matching Go).

### Fixed

- Awaiting a dead-lettered workflow (`WorkflowHandle::result` /
  `retrieve_workflow` + `await`) no longer falls through to output decoding —
  which for a unit-typed workflow silently returned `Ok(())`, masking the
  failure, and for other output types produced a confusing deserialization
  error. It now returns the typed error above.
- A panic in a workflow or step body is now caught rather than unwinding past the
  terminal-status write, which previously left the row non-terminal (`PENDING`)
  with any polling observer waiting forever. A panic in a **step** becomes a step
  error subject to that step's retry policy (a step that panics once can succeed
  on retry). A panic in the **workflow body** is treated as a recoverable failure,
  like a crash: the row is left non-terminal and a later `recover()` re-runs it
  from its checkpoints (bounded by the recovery-attempt cap — a deterministic
  panic eventually dead-letters), matching the durable-execution model where only
  a returned error terminates a workflow. (Requires the default `panic = "unwind"`;
  under `panic = "abort"` there is nothing to catch.)

### Documentation

- Added a `determinism` concept guide — a `std`-style companion to the
  `durability` guide covering how to write a correct workflow body: the catalog
  of non-determinism foot-guns (wall clock, RNG, `HashMap` iteration order,
  `spawn`/task races, `Drop` side effects, direct env/config/file/network reads)
  and their durable fixes; the durable-safe data rules for values that cross a
  checkpoint-and-replay or cross-SDK boundary (no `NaN`/infinity, string-encoded
  integers past 2⁵³, ordered maps for byte-stable records); and the
  dependency-injection pattern — build a pool/client/config once at startup into
  a process global and read it inside steps, never in durable state — with a note
  on why workflows stay free functions and the trigger that would justify a
  method-based API.
- Added `examples/dependencies.rs`, a runnable companion to that guide's
  dependency-injection section: a `PricingService` (stand-in for an HTTP client
  and its config) wired through a global `OnceLock` and read inside a step, with
  a re-run proving the dependency is invoked exactly once while the replay serves
  the checkpoint.

## [0.3.0] - 2026-07-12

The feature-gating release: optional components you don't use no longer weigh
down your build. The Postgres and SQLite backends are cargo features (both on by
default; at least one required), and the Conductor client and admin HTTP server
are opt-in. `durare-macros` is unchanged and stays at `0.1.0`.

### Changed

- **(breaking)** The DBOS Conductor client — `Conductor`, `ConductorConfig`,
  `AlertHandler` — now lives behind an opt-in `conductor` cargo feature, off by
  default. Enable it with `features = ["conductor"]`. This keeps its
  `tokio-tungstenite` (TLS websocket) and `flate2` (gzip) dependencies out of
  builds that never talk to the DBOS control plane.
- The Postgres and SQLite backends are now cargo features (`postgres`,
  `sqlite`), both enabled by default. Enable a single backend to drop the
  other's driver: a Postgres-only build skips SQLite's bundled C library, and a
  SQLite-only build skips the Postgres network/TLS driver. **At least one backend
  is required** — a build with neither is a compile error. `InMemoryProvider`
  stays available in every build.
- **(breaking)** The admin HTTP server (`AdminServer`) is now behind an opt-in
  `admin` cargo feature, off by default. Enable it with `features = ["admin"]`.
  This keeps the axum/hyper/tower HTTP stack out of builds that don't expose the
  DBOS admin endpoints.

### Documentation

- Added a "Cargo features" section to the crate docs and the README documenting
  the `postgres`, `sqlite`, and opt-in `conductor`/`admin` features, and fixed
  the stale README quick-start version requirement (it had pinned `0.1`, which
  does not resolve to newer releases).

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

[Unreleased]: https://github.com/SamuelXing/durare/compare/v0.3.3...HEAD
[0.3.3]: https://github.com/SamuelXing/durare/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/SamuelXing/durare/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/SamuelXing/durare/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/SamuelXing/durare/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/SamuelXing/durare/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/SamuelXing/durare/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/SamuelXing/durare/releases/tag/v0.1.0
