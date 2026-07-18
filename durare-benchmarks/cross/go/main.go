// The benchmark app on dbos-transact-golang, exposing the same HTTP contract
// as the upstream dbos-benchmark-app (GET /:num -> {output, runtime}) so
// benchmark_dbos.py drives it unmodified — same workload as durare's `steps`:
// N sequential read-then-write transactions on one counter row.
//
// GET /park/<count> additionally starts <count> workflows parked on a durable
// sleep, for the memory-per-in-flight-workflow measurement (RSS is read from
// outside via `ps`, uniformly across SDKs).
//
//	DATABASE_URL=postgres://localhost/durare_bench_go go run . --port 18810
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dbosCtx dbos.DBOSContext
	ds      *dbos.DataSource
)

func benchmarkWorkflow(ctx dbos.DBOSContext, num int) (int64, error) {
	var out int64
	for i := 0; i < num; i++ {
		count, err := dbos.RunAsTransaction(ctx, ds, func(c context.Context, tx dbos.Tx) (int64, error) {
			var n int64
			if err := tx.QueryRow(c,
				"SELECT greet_count FROM bench_hello_go WHERE name = 'dbos'").Scan(&n); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(c,
				"UPDATE bench_hello_go SET greet_count = $1 WHERE name = 'dbos'", n+1); err != nil {
				return 0, err
			}
			return n, nil
		})
		if err != nil {
			return 0, err
		}
		out = count
	}
	return out, nil
}

func sleeper(ctx dbos.DBOSContext, _ int) (string, error) {
	_, err := dbos.Sleep(ctx, time.Hour)
	return "", err
}

func main() {
	port := flag.Int("port", 18810, "port to listen on")
	flag.Parse()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		panic("set DATABASE_URL")
	}

	var err error
	dbosCtx, err = dbos.NewDBOSContext(context.Background(), dbos.Config{
		AppName:     "durare-bench-go",
		DatabaseURL: dbURL,
	})
	if err != nil {
		panic(err)
	}

	// The application pool: the counter table lives here, and the data source
	// commits each transaction's writes together with its durability row.
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		panic(err)
	}
	if _, err := pool.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS bench_hello_go (name TEXT PRIMARY KEY, greet_count BIGINT NOT NULL)"); err != nil {
		panic(err)
	}
	if _, err := pool.Exec(context.Background(),
		"INSERT INTO bench_hello_go (name, greet_count) VALUES ('dbos', 0) ON CONFLICT (name) DO NOTHING"); err != nil {
		panic(err)
	}
	ds, err = dbos.NewDataSource(dbosCtx, pool)
	if err != nil {
		panic(err)
	}

	dbos.RegisterWorkflow(dbosCtx, benchmarkWorkflow)
	dbos.RegisterWorkflow(dbosCtx, sleeper)
	if err := dbos.Launch(dbosCtx); err != nil {
		panic(err)
	}
	defer dbos.Shutdown(dbosCtx, 2*time.Second)

	// Warm-up: pool + query plans.
	if h, err := dbos.RunWorkflow(dbosCtx, benchmarkWorkflow, 1); err == nil {
		_, _ = h.GetResult()
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		w.Header().Set("Content-Type", "application/json")
		if parts[0] == "park" {
			count, _ := strconv.Atoi(parts[1])
			for i := 0; i < count; i++ {
				if _, err := dbos.RunWorkflow(dbosCtx, sleeper, 0); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parked": count, "pid": os.Getpid()})
			return
		}
		num, err := strconv.Atoi(parts[0])
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		start := time.Now()
		handle, err := dbos.RunWorkflow(dbosCtx, benchmarkWorkflow, num)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out, err := handle.GetResult()
		runtime := float64(time.Since(start).Microseconds()) / 1000.0
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"output": out, "runtime": runtime})
	})

	fmt.Printf("dbos-go benchmark app on http://127.0.0.1:%d (pid %d)\n", *port, os.Getpid())
	panic(http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", *port), nil))
}
