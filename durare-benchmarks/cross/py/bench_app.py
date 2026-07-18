"""The benchmark app on dbos-transact-py, exposing the same HTTP contract as
the upstream dbos-benchmark-app (GET /:num -> {output, runtime}) so
benchmark_dbos.py drives it unmodified — same workload as durare's `steps`:
N sequential read-then-write transactions on one counter row.

GET /park/<count> additionally starts <count> workflows parked on a durable
sleep, for the memory-per-in-flight-workflow measurement (RSS is read from
outside via `ps`, uniformly across SDKs).

    pip install dbos
    DATABASE_URL=postgres://localhost/durare_bench_py python3 bench_app.py --port 18809
"""

import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import sqlalchemy as sa
from dbos import DBOS

DB_URL = os.environ["DATABASE_URL"]

DBOS(
    config={
        "name": "durare-bench-py",
        "system_database_url": DB_URL,
        "application_database_url": DB_URL,
        "run_admin_server": False,
        "log_level": "WARNING",
    }
)


@DBOS.transaction()
def benchmark_transaction() -> int:
    row = DBOS.sql_session.execute(
        sa.text("SELECT greet_count FROM bench_hello_py WHERE name = 'dbos'")
    ).fetchone()
    count = row[0]
    DBOS.sql_session.execute(
        sa.text("UPDATE bench_hello_py SET greet_count = :c WHERE name = 'dbos'"),
        {"c": count + 1},
    )
    return count


@DBOS.workflow()
def benchmark_workflow(num: int) -> int:
    out = 0
    for _ in range(num):
        out = benchmark_transaction()
    return out


@DBOS.workflow()
def sleeper() -> None:
    DBOS.sleep(3600)


def setup_schema() -> None:
    # SQLAlchemy spells the dialect `postgresql://`; DBOS accepts either.
    engine = sa.create_engine(DB_URL.replace("postgres://", "postgresql://", 1))
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "CREATE TABLE IF NOT EXISTS bench_hello_py "
                "(name TEXT PRIMARY KEY, greet_count BIGINT NOT NULL)"
            )
        )
        conn.execute(
            sa.text(
                "INSERT INTO bench_hello_py (name, greet_count) VALUES ('dbos', 0) "
                "ON CONFLICT (name) DO NOTHING"
            )
        )
    engine.dispose()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 (stdlib naming)
        parts = self.path.strip("/").split("/")
        if parts[0] == "park":
            count = int(parts[1])
            for _ in range(count):
                DBOS.start_workflow(sleeper)
            body = {"parked": count, "pid": os.getpid()}
        else:
            num = int(parts[0])
            start = time.perf_counter()
            output = benchmark_workflow(num)
            runtime_ms = (time.perf_counter() - start) * 1000.0
            body = {"output": output, "runtime": runtime_ms}
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # keep the driver's output clean
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18809)
    args = parser.parse_args()

    setup_schema()
    DBOS.launch()
    benchmark_workflow(1)  # warm-up: pool + query plans

    print(f"dbos-py benchmark app on http://127.0.0.1:{args.port} (pid {os.getpid()})", flush=True)
    HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
