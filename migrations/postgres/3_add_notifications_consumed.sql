-- Workflow messaging (send/recv), ported from the Go SDK's migration 12.
--
-- recv claims the oldest unconsumed message by flipping `consumed` instead of
-- deleting the row, so delivered messages stay visible for observability; rows
-- are cleaned up by FK cascade when the parent workflow is deleted.

ALTER TABLE notifications ADD COLUMN consumed BOOLEAN NOT NULL DEFAULT FALSE;
