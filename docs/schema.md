# DBOS system database schema

The durable state that makes DBOS workflows recoverable. **10 tables**, defined
by the 37 migrations under [`migrations/`](../migrations). The shape is 1:1 with
the Go/Python SDKs — the same rows are readable by any DBOS SDK connected to the
database.

Design in one line: **`workflow_status` is the hub**; the durable side-effect
tables (steps, events, streams, mailbox) hang off it by `workflow_uuid` with
`ON DELETE CASCADE`, and four standalone tables hold registries/config that
outlive any single run.

All timestamps are **epoch-milliseconds `BIGINT`** (not SQL `timestamp`) — a
deliberate cross-language choice so every SDK stores identical bytes.

## Entity relationships

```mermaid
erDiagram
    workflow_status ||--o{ operation_outputs : "steps & child calls"
    workflow_status ||--o{ notifications : "send/recv mailbox"
    workflow_status ||--o{ workflow_events : "set_event/get_event"
    workflow_status ||--o{ workflow_events_history : "event history"
    workflow_status ||--o{ streams : "write_stream"

    workflow_status {
        TEXT    workflow_uuid PK
        TEXT    status
        TEXT    name
        TEXT    inputs
        TEXT    output
        TEXT    error
        TEXT    executor_id
        TEXT    application_version
        BIGINT  recovery_attempts
        TEXT    queue_name
        TEXT    queue_partition_key
        INTEGER priority
        TEXT    deduplication_id
        BOOLEAN rate_limited
        BIGINT  delay_until_epoch_ms
        TEXT    parent_workflow_id
        TEXT    forked_from
        BOOLEAN was_forked_from
        BIGINT  workflow_timeout_ms
        BIGINT  workflow_deadline_epoch_ms
        BIGINT  started_at_epoch_ms
        BIGINT  completed_at
        BIGINT  created_at
        BIGINT  updated_at
        TEXT    authenticated_user
        TEXT    assumed_role
        TEXT    authenticated_roles
        TEXT    request
        TEXT    owner_xid
        TEXT    serialization
        VARCHAR class_name
        VARCHAR config_name
        TEXT    application_id
    }

    operation_outputs {
        TEXT    workflow_uuid PK
        INTEGER function_id PK
        TEXT    function_name
        TEXT    output
        TEXT    error
        TEXT    child_workflow_id
        BIGINT  started_at_epoch_ms
        BIGINT  completed_at_epoch_ms
        TEXT    serialization
    }

    notifications {
        TEXT    message_uuid PK
        TEXT    destination_uuid FK
        TEXT    topic
        TEXT    message
        BOOLEAN consumed
        BIGINT  created_at_epoch_ms
        TEXT    serialization
    }

    workflow_events {
        TEXT    workflow_uuid PK
        TEXT    key PK
        TEXT    value
        TEXT    serialization
    }

    workflow_events_history {
        TEXT    workflow_uuid PK
        INTEGER function_id PK
        TEXT    key PK
        TEXT    value
        TEXT    serialization
    }

    streams {
        TEXT    workflow_uuid PK
        TEXT    key PK
        INTEGER offset PK
        TEXT    value
        TEXT    serialization
    }

    workflow_schedules {
        TEXT    schedule_id PK
        TEXT    schedule_name UK
        TEXT    workflow_name
        TEXT    schedule
        TEXT    status
        TEXT    context
        TEXT    last_fired_at
        BOOLEAN automatic_backfill
        TEXT    cron_timezone
        TEXT    queue_name
        TEXT    workflow_class_name
    }

    queues {
        TEXT    queue_id PK
        TEXT    name UK
        INTEGER concurrency
        INTEGER worker_concurrency
        INTEGER rate_limit_max
        BOOLEAN priority_enabled
        BOOLEAN partition_queue
        BIGINT  created_at
        BIGINT  updated_at
    }

    application_versions {
        TEXT    version_id PK
        TEXT    version_name UK
        BIGINT  version_timestamp
        BIGINT  created_at
    }

    event_dispatch_kv {
        TEXT    service_name PK
        TEXT    workflow_fn_name PK
        TEXT    key PK
        TEXT    value
        NUMERIC update_seq
        NUMERIC update_time
    }
```

> `workflow_schedules`, `queues`, `application_versions`, and
> `event_dispatch_kv` have **no FK** to `workflow_status` — they are registries /
> external-source bookkeeping, so they're shown unconnected above.

## The hub — `workflow_status`

One row per workflow execution; the anchor for crash recovery. Columns by concern:

| Concern | Columns |
|---|---|
| Identity / lineage | `workflow_uuid` (PK), `name`, `class_name`, `config_name`, `parent_workflow_id`, `forked_from`, `was_forked_from` |
| Execution state | `status`, `executor_id`, `recovery_attempts`, `owner_xid`, `application_version`, `application_id` |
| I/O | `inputs`, `output`, `error`, `serialization` |
| Queue / dispatch | `queue_name`, `queue_partition_key`, `priority`, `deduplication_id`, `rate_limited`, `delay_until_epoch_ms` |
| Timing / timeout | `created_at`, `updated_at`, `started_at_epoch_ms`, `completed_at`, `workflow_timeout_ms`, `workflow_deadline_epoch_ms` |
| Auth / context | `authenticated_user`, `assumed_role`, `authenticated_roles`, `request` |

`status` state machine:
`ENQUEUED → DELAYED → PENDING → SUCCESS | ERROR | CANCELLED | MAX_RECOVERY_ATTEMPTS_EXCEEDED`.

The original `UNIQUE(queue_name, deduplication_id)` constraint was **dropped**
(migration 28) and replaced by a *partial* index (27), so dedup applies only to
active rows.

## Spoke tables (FK → `workflow_status`, cascade delete)

| Table | Purpose | Key | Powers |
|---|---|---|---|
| `operation_outputs` | **Step checkpoints** — one row per step or child-workflow call; holds an `output`/`error` or a `child_workflow_id` | `(workflow_uuid, function_id)` | durable replay, exactly-once steps, step introspection, child lineage |
| `notifications` | `send`/`recv` **mailbox** | `message_uuid` | messaging; `AFTER INSERT` NOTIFY on `dbos_notifications_channel` wakes blocked `recv` |
| `workflow_events` | `set_event`/`get_event` key→value | `(workflow_uuid, key)` | events; twin NOTIFY on `dbos_workflow_events_channel` |
| `workflow_events_history` | versioned event history | `(workflow_uuid, function_id, key)` | deterministic event replay |
| `streams` | append-only durable **streams** (`write_stream`) | `(workflow_uuid, key, offset)` | streaming; close sentinel `__DBOS_STREAM_CLOSED__` stored as a row |

## Standalone tables (registries / config — no workflow FK)

| Table | Purpose | Notable columns |
|---|---|---|
| `workflow_schedules` | **cron definitions** driving the scheduler | `schedule` (cron), `status` (ACTIVE/PAUSED), `context`, `last_fired_at`, `automatic_backfill`, `cron_timezone`, `queue_name` |
| `queues` | queue registry (informational; runtime config is in-process) | `concurrency`, `worker_concurrency`, `rate_limit_max`, `priority_enabled`, `partition_queue` |
| `application_versions` | deployed-version registry for version-gated recovery | `version_name` (unique), `version_timestamp` |
| `event_dispatch_kv` | external-event (e.g. Kafka) exactly-once bookkeeping | `(service_name, workflow_fn_name, key)`, `update_seq`, `update_time` |

## Cross-cutting patterns

1. **Everything durable is an idempotent, deterministically-keyed row.** Steps,
   events, sends, stream writes, and schedule fires are all "insert-if-absent,
   else read back" — so replay and crash recovery need no in-memory truth.
2. **The `serialization` column** (migration 11, on every payload table) makes
   the store polyglot: any SDK decodes another's rows by dispatching on it
   (`DBOS_JSON` / `portable_json` / `DBOS_GOB`).
3. **`ON DELETE CASCADE` from the hub** means one `DELETE` on `workflow_status`
   removes all of a workflow's steps/events/streams/notifications.
4. **LISTEN/NOTIFY triggers** on `notifications` + `workflow_events` turn polling
   into push for `recv`/`get_event` (Postgres only; SQLite polls).
5. **Partial indexes** (migrations 22–37) — many migrations drop full indexes and
   recreate them scoped to hot rows (pending-only, failed-only, in-flight-only),
   keeping the dispatch scan cheap.

---

*Generated from the migrations under [`migrations/postgres`](../migrations/postgres).
SQLite mirrors the same shape with SQLite-native types.*
