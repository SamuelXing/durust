-- Initial durust schema (Postgres), aligned with the DBOS Go SDK's canonical
-- tables: workflow_status, operation_outputs, notifications, workflow_events.
--
-- Conventions (matching Go for cross-dialect portability):
--   * Serialized values (inputs/output/error/event values) are stored as TEXT.
--   * Timestamps are epoch-milliseconds (BIGINT); callers supply them, so the
--     schema does not depend on driver-side defaults/UDFs.
--
-- Applied and version-tracked by `sqlx::migrate!` from PostgresProvider::init.
-- Migration files are append-only once released: evolve the schema by adding a
-- new numbered file rather than editing this one.

CREATE TABLE workflow_status (
    workflow_uuid              TEXT PRIMARY KEY,
    status                     TEXT    NOT NULL,
    name                       TEXT    NOT NULL,
    inputs                     TEXT,
    output                     TEXT,
    error                      TEXT,
    executor_id                TEXT    NOT NULL DEFAULT '',
    application_version        TEXT    NOT NULL DEFAULT '',
    queue_name                 TEXT,
    priority                   INTEGER NOT NULL DEFAULT 0,
    deduplication_id           TEXT,
    recovery_attempts          BIGINT  NOT NULL DEFAULT 0,
    parent_workflow_id         TEXT,
    workflow_timeout_ms        BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    created_at                 BIGINT  NOT NULL,
    updated_at                 BIGINT  NOT NULL
);

CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON workflow_status (status);
-- Dispatcher lookup: enqueued rows of a queue ordered by priority.
CREATE INDEX workflow_status_queue_index ON workflow_status (queue_name, status, priority);
-- Queue-scoped deduplication: NULLs are distinct, so non-queued rows are
-- unconstrained.
CREATE UNIQUE INDEX uq_workflow_status_queue_name_dedup_id
    ON workflow_status (queue_name, deduplication_id);

CREATE TABLE operation_outputs (
    workflow_uuid     TEXT    NOT NULL,
    function_id       INTEGER NOT NULL,
    function_name     TEXT    NOT NULL DEFAULT '',
    output            TEXT,
    error             TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE notifications (
    message_uuid        TEXT   NOT NULL PRIMARY KEY,
    destination_uuid    TEXT   NOT NULL,
    topic               TEXT,
    message             TEXT   NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL,
    FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);

CREATE TABLE workflow_events (
    workflow_uuid TEXT NOT NULL,
    key           TEXT NOT NULL,
    value         TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
