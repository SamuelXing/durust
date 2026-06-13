-- Workflow messaging (send/recv) — SQLite dialect of postgres/3.

ALTER TABLE notifications ADD COLUMN consumed BOOLEAN NOT NULL DEFAULT FALSE;
