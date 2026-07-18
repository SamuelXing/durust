# Cross-SDK baseline

The same workload, the same driver, the same Postgres server — durare
measured against the DBOS Python and Go SDKs. Read the
[honest framing](README.md#honest-framing) first: durable-execution latency
is dominated by database round-trips, so these numbers characterize *SDK
overhead around the database floor*, and the floor here (local Postgres,
sub-millisecond RTT) is an order of magnitude lower than any cloud
deployment's. Never compare these numbers against ones measured elsewhere.

## Methodology

- **Workload**: the upstream `dbos-workflow-benchmarks` `benchmarkWorkflow` —
  a workflow of N sequential transactions, each a read-then-write increment
  of one counter row, with the durability checkpoint committed atomically
  with the SQL (each SDK's transactional-step primitive:
  `ctx.transaction` / `@DBOS.transaction` / `RunAsTransaction`).
- **Driver**: the upstream `benchmark_dbos.py`, **unmodified**, against each
  app's `GET /:num → {output, runtime}` endpoint; `runtime` is measured
  server-side around workflow invocation → completion, excluding HTTP.
  Parameters: `-n 50 -i 10` (50 timed workflows of 10 transactions), one
  warm-up run per app before timing.
- **Memory**: from a fresh process and a wiped database, RSS (`ps`, minimum
  of six samples over ~10 s — stable under macOS lazy page reclaim) before
  and after starting 2 000 workflows parked on a durable one-hour sleep,
  handles dropped. A 25 s post-startup settle precedes the baseline read.
- **Isolation**: one Postgres server (localhost), one database per SDK, apps
  and driver run sequentially, nothing else on the machine.
- **Environment**: Apple M5, 32 GiB, macOS; PostgreSQL 14.20 (Homebrew,
  local socket-latency); durare @ `e7c62b4` (0.3.3 + this harness), rustc
  1.96.1; `dbos` (Python) 1.14.0 on CPython 3.9.6; `dbos-transact-golang`
  v0.20.0 on go 1.26.4. 2026-07-16.

## Latency (50 × 10-transaction workflows, server-side ms)

| SDK | median | mean | p99 | min | max |
|---|---|---|---|---|---|
| **durare** | **4.52** | 5.04 | 10.98 | 3.90 | 12.89 |
| dbos (Python) | 10.91 | 11.59 | 20.83 | 10.33 | 23.25 |
| dbos-transact-golang | 16.09 | 19.46 | 49.23 | 6.33 | 54.42 |

Per-transaction slope: durare ≈ 0.45 ms, Python ≈ 1.1 ms, Go ≈ 1.6 ms.

One structural note the numbers force: the three SDKs implement
transactional durability with different write plumbing. durare commits the
checkpoint **in** the application transaction (one commit per step); Python
records `transaction_outputs` inside the application transaction (one
commit); Go's `RunAsTransaction` writes a `transaction_completion` row in
the application database **and** a checkpoint in the system database (two
commits per step — its data-source design supports splitting the two across
different databases). Same exactly-once guarantee, different write
amplification; on a local database the extra commit is most of Go's gap.

## Memory per parked in-flight workflow (2 000 parked, marginal RSS)

| SDK | marginal RSS / workflow | what holds it |
|---|---|---|
| **durare** | **≈ 0.2 KiB** | one tokio task (an async state machine; no thread, no connection) |
| dbos-transact-golang | ≈ 1.9 KiB | one goroutine (lazily-committed stack) |
| dbos (Python) | ≈ 1.5 KiB *committed* | **one OS thread per workflow** (512 KiB reserved stack each) |

The committed-RSS numbers undersell the structural difference, so state it
plainly: at 2 300 parked workflows the Python process held 2 306 OS threads
(verified with `ps -M`) — its in-flight ceiling is the platform's thread
limit, thousands, regardless of RSS. Go parks a goroutine per workflow and
scales to hundreds of thousands. durare parks a heap-allocated future —
sub-kilobyte marginal cost and no scheduler entity of its own — so parked
workflows are effectively free until the database, not the runtime, is the
limit. (durare's in-process `memory` workload, which additionally holds a
result handle per workflow, measures ≈ 4.8 KiB — the conservative upper
bound.)

## Reproducing

```bash
# durare (terminal 1) — and equivalently cross/py, cross/go on their ports
DATABASE_URL=postgres://localhost/durare_bench cargo run --release -- serve --port 18808

# the upstream driver, verbatim (terminal 2)
cd ../dbos-workflow-benchmarks
python3 benchmarks/benchmark_dbos.py -u http://127.0.0.1:18808 -n 50 -i 10

# memory: GET /park/2000 on a fresh app + wiped DB; RSS via ps before/after
```

Caveats to keep honest: single machine, single run per cell (variance
across repeated runs was < 10% for median latency during development);
macOS RSS accounting (compressed memory, lazy reclaim) is why the protocol
uses minimum-of-samples and long settles; Python 3.9 is the system
interpreter — newer CPythons will differ; and a cloud database's network
RTT would compress all three SDKs' relative gaps toward the wire floor.
