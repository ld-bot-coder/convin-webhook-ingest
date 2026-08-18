-- event_id provider ki idempotency key hai aur redeliveries me stable rehti
-- hai. 001 ne ise index to kiya par constraint nahi lagaya, isliye same event
-- do baar store ho sakta tha - app ko read-before-write karna padta tha, jo do
-- parallel deliveries dono jeet leti hain. UNIQUE karne se faisla Postgres ka
-- ho jata hai aur woh atomic hai.

-- Jo pehle se duplicate pade hain unhe hata do, sabse pehla wala rakh ke.
DELETE FROM events a
      USING events b
      WHERE a.event_id = b.event_id
        AND a.id > b.id;

-- Unique constraint apna index khud banata hai, plain wala redundant hai.
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

-- Test harness account ke hisaab se cleanup karta hai, yeh us scan ke liye.
CREATE INDEX IF NOT EXISTS idx_calls_account_id ON calls (account_id);
