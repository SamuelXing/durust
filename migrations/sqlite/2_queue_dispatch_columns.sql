-- Queue dispatch support — SQLite dialect of postgres/2.
-- BOOLEAN is stored as INTEGER 0/1; partial indexes are supported.

ALTER TABLE workflow_status ADD COLUMN started_at_epoch_ms INTEGER;
ALTER TABLE workflow_status ADD COLUMN rate_limited BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_status ADD COLUMN delay_until_epoch_ms INTEGER;

CREATE INDEX idx_workflow_status_delayed
    ON workflow_status (delay_until_epoch_ms)
    WHERE status = 'DELAYED';
