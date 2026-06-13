-- Workflow management & fork lineage, ported from the Go SDK's migrations
-- 36 (completed_at), 4 (forked_from), and 18 (was_forked_from).
--
--   * completed_at      — epoch ms when the workflow reached a terminal state.
--   * forked_from       — on a forked workflow, the id it was forked from.
--   * was_forked_from   — TRUE on an original workflow once it has been forked.

ALTER TABLE workflow_status ADD COLUMN completed_at BIGINT;
ALTER TABLE workflow_status ADD COLUMN forked_from TEXT;
ALTER TABLE workflow_status ADD COLUMN was_forked_from BOOLEAN NOT NULL DEFAULT FALSE;
