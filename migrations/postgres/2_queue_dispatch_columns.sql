-- Queue dispatch support, ported from the Go SDK's migrations
-- 5 (started_at), 16 (delay_until / DELAYED), and 33 (rate_limited).
--
--   * started_at_epoch_ms  — set when a workflow is claimed (ENQUEUED→PENDING);
--                            the rate limiter counts starts newer than its window.
--   * rate_limited         — TRUE when the workflow was dequeued from a
--                            rate-limited queue, so only those count against it.
--   * delay_until_epoch_ms — for DELAYED workflows; the dispatcher transitions
--                            them to ENQUEUED once the delay expires.

ALTER TABLE workflow_status ADD COLUMN started_at_epoch_ms BIGINT;
ALTER TABLE workflow_status ADD COLUMN rate_limited BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE workflow_status ADD COLUMN delay_until_epoch_ms BIGINT;

CREATE INDEX idx_workflow_status_delayed
    ON workflow_status (delay_until_epoch_ms)
    WHERE status = 'DELAYED';
