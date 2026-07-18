# durare-benchmarks

Performance benchmarks for [`durare`](../). This is a **separate, unpublished
crate with its own workspace**, so its dependencies and long run times never
touch the SDK's build, lint gate, or CI. It depends on `durare` by path, so the
benchmarks always measure the code in this checkout.

## Honest framing

DBOS-style durable execution is **dominated by Postgres round-trips**, not the
language runtime: every step, sleep, and state transition is a database write.
So a raw "workflows per second" number largely measures your database and will
converge across SDKs on the same Postgres. Chasing a throughput headline over
Go/Python is a trap.

These benchmarks are designed to separate the two things:

- **`steps`** measures the database-bound path, on the *exact* workload the
  upstream [`dbos-workflow-benchmarks`](https://github.com/dbos-inc/dbos-workflow-benchmarks)
  uses (`benchmarkWorkflow`: a workflow of *N* sequential read-then-write
  transactions), so durare's numbers are directly comparable to the DBOS Go,
  Python, and TypeScript SDKs on the same database.
- **`memory`** measures where the runtime choice actually shows: resident memory
  per in-flight, durably-parked workflow. Each parked workflow is a compact async
  state machine holding no database connection — no goroutine stack, no GC.

No cross-SDK headline numbers are published from here until they are grounded in
a real demo app (see the SDK roadmap's Track C gate); this crate exists to
measure durare honestly and to guard against performance regressions.

## Running

Point it at a **throwaway** Postgres database (the benchmarks create tables and
leave workflow rows behind):

```bash
createdb durare_bench
export DATABASE_URL="postgres://localhost:5432/durare_bench"

# Workflow of 10 transaction-steps, timed over 50 runs (mirrors upstream):
cargo run --release -- steps --steps 10 --iterations 50

# Resident memory growth across 5000 durably-parked workflows:
cargo run --release -- memory --count 5000
```

Always build `--release`; debug numbers are meaningless.

## Workloads

| Command | Reports | Comparable to |
| --- | --- | --- |
| `steps --steps N --iterations M` | workflow duration p50 / p99 / mean, per-step, throughput | upstream `benchmarkWorkflow` (DBOS Go/Py/TS) |
| `memory --count N` | RSS baseline, growth, per-in-flight-workflow | runtime footprint (Rust's expected edge) |

## Planned

Tail latency under concurrent load, queue dequeue throughput, and recovery time
for *N* pending workflows after a crash — then the cross-SDK comparison against
Go/Python on an identical Postgres and workload.

## Driving durare with the upstream harness

`serve` exposes the exact HTTP contract of the upstream `dbos-benchmark-app`
(`GET /:num` → `{output, runtime}`, `runtime` being the server-side workflow
duration in milliseconds), so the upstream driver runs against durare with no
modification — the same script, parameters, and statistics as against the
TypeScript app:

```bash
# terminal 1 — durare as the benchmark target
DATABASE_URL=postgres://localhost/durare_bench \
  cargo run --release -- serve --port 18808

# terminal 2 — the upstream driver, verbatim (needs `pip install requests numpy`)
cd ../../dbos-workflow-benchmarks
python3 benchmarks/benchmark_dbos.py -u http://127.0.0.1:18808 -n 50 -i 10
```

For a fair cross-SDK number, point the other SDKs' benchmark apps at the
**same Postgres** and drive them with the same `-n`/`-i`; never compare a
local-Postgres run against DBOS Cloud numbers (the round-trip floors differ
by an order of magnitude).

## Cross-SDK baseline

[`RESULTS.md`](RESULTS.md) holds the measured baseline — durare vs the DBOS
Python and Go SDKs, same workload, same driver, same Postgres server — with
the full methodology and its caveats. The comparison apps live under
[`cross/`](cross/): each exposes the same HTTP contract, so the one upstream
driver measures all three identically.
