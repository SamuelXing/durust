-- Initial durust schema (SQLite), aligned with the DBOS Go SDK's canonical
-- tables: workflow_status, operation_outputs, notifications, workflow_events.
--
-- SQLite-flavor conventions (matching Go):
--   * INTEGER for epoch-ms columns (BIGINT is an alias for INTEGER in SQLite).
--   * Serialized values (inputs/output/error/event values) are stored as TEXT.
--   * No DEFAULTs for timestamp/uuid columns — callers supply them explicitly,
--     so the schema does not depend on driver-side UDFs.
--   * Foreign keys require `PRAGMA foreign_keys = ON`, set on connect by
--     SqliteProvider.
--
-- Applied and version-tracked by `sqlx::migrate!` from SqliteProvider::init.

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
    recovery_attempts          INTEGER NOT NULL DEFAULT 0,
    parent_workflow_id         TEXT,
    workflow_timeout_ms        INTEGER,
    workflow_deadline_epoch_ms INTEGER,
    created_at                 INTEGER NOT NULL,
    updated_at                 INTEGER NOT NULL
);

CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);
CREATE INDEX workflow_status_executor_id_index ON workflow_status (executor_id);
CREATE INDEX workflow_status_status_index ON workflow_status (status);
CREATE INDEX workflow_status_queue_index ON workflow_status (queue_name, status, priority);
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
    message_uuid        TEXT    NOT NULL PRIMARY KEY,
    destination_uuid    TEXT    NOT NULL,
    topic               TEXT,
    message             TEXT    NOT NULL,
    created_at_epoch_ms INTEGER NOT NULL,
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
