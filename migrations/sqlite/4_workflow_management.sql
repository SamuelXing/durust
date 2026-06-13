-- Workflow management & fork lineage — SQLite dialect of postgres/4.

ALTER TABLE workflow_status ADD COLUMN completed_at INTEGER;
ALTER TABLE workflow_status ADD COLUMN forked_from TEXT;
ALTER TABLE workflow_status ADD COLUMN was_forked_from BOOLEAN NOT NULL DEFAULT FALSE;
