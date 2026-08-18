-- event_id is the provider's idempotency key: it is stable across
-- redeliveries. 001 indexed it but never constrained it, so nothing in the
-- database stopped the same event being stored twice - the service could only
-- dedupe by reading before writing, which two concurrent deliveries both win.
--
-- Making it UNIQUE moves the decision into Postgres, where it is atomic:
-- exactly one concurrent INSERT succeeds and the rest conflict.

-- Collapse anything a running instance already duplicated, keeping the copy
-- that arrived first.
DELETE FROM events a
      USING events b
      WHERE a.event_id = b.event_id
        AND a.id > b.id;

-- The unique constraint creates its own index, so the plain one is redundant.
DROP INDEX IF EXISTS idx_events_event_id;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'events_event_id_key'
    ) THEN
        ALTER TABLE events ADD CONSTRAINT events_event_id_key UNIQUE (event_id);
    END IF;
END
$$;

-- The aggregate is rebuilt per account and the test harness cleans up by
-- account, both of which scan calls by account_id.
CREATE INDEX IF NOT EXISTS idx_calls_account_id ON calls (account_id);
