# SOLUTION

## What was broken, and why

**Dedup was a check-then-act race.** `EventExists` then `InsertEvent` are two
statements. The provider retries hard, so one `event_id` is often in flight
several times; every delivery read "absent" before any wrote, so all of them
inserted and all of them incremented. `001` indexed `event_id` but never
constrained it, so the database permitted it. 16 overlapping deliveries of one
event gave 16 rows and `call_count = 16` — the duplicates and the drift.

**`call_count` counted events, not calls.** `calls` upserts on `call_id`, but
every accepted event incremented the aggregate. A correction or late status
update for a known call left one row in `calls` and two in the aggregate:
counts drifting above the real number of calls with no duplicates involved.

**The three writes were not atomic.** Event insert, call upsert and aggregate
update ran independently against the pool. A failure between them stranded a
stored event whose totals were never updated — and since the event was stored,
every retry that followed was dropped as a duplicate, so it never caught up.

**Recording work ran on a cancelled context.** It was given to a goroutine that
kept the *request* context, which `net/http` cancels as the handler returns.
The `UPDATE` 50ms later failed with `context canceled`, and the error went into
an empty `// TODO: handle`. That is both "never marked processed" and "nothing
in the logs".

**Nothing waited for that goroutine.** On SIGTERM the server drained handlers
and exited, killing recordings mid-flight — silently, with the event already
stored so no retry ever came. That is the disappearing work on deploy.

**The read path answered from a per-process cache.** After a deploy it reported
zero until new webhooks arrived. `Record` also took no lock while `Get` took the
read lock, so increments were lost and concurrent map writes could kill the
process.

## Why this deduplication strategy

**A `UNIQUE` constraint on `events.event_id`, with the insert as the dedupe
point, inside the transaction that does the rest of the work.**
`INSERT ... ON CONFLICT DO NOTHING RETURNING id` returns a row to exactly one
of any number of concurrent deliveries. The claim and the effects it authorises
commit together, so there is no window where an event is recorded but uncounted.

Alternatives I rejected:

- **Redis `SETNX` as the authority.** Fastest, but keys expire, get evicted and
  vanish on restart, and a lost key silently permits a double count. The deeper
  problem is that the claim would live in a different system from the write it
  authorises: claim before the transaction and a failed transaction loses the
  event forever; claim after and two concurrent deliveries both get through.
  No ordering makes two systems atomic without distributed commit.
- **`SELECT ... FOR UPDATE` on the event row.** Needs a row to lock, and the
  first delivery is exactly when none exists. Solves contention, not first-write
  races.
- **Unique constraint for dedup only.** Idempotent writes, but the aggregate is
  still maintained separately and can still drift from `calls`.

I did build the Redis fast path — key written only after commit, misses falling
through — and then took it out. It is safe in production, but it creates state
nothing cleans up: the test harness resets Postgres per account while Redis
keeps the key for its TTL, so the same `event_id` posted again after a reset is
silently skipped and the suite stops being repeatable. The saving is one indexed
lookup. Not worth a second place where "have I seen this?" is answered.

Counting is idempotent by construction: `call_count` moves only when the call
row is created, and a repeat event adjusts duration by the difference. The
aggregate row is locked before the call is read, which makes that read-then-write
safe. The cache is then fed the totals that transaction returned, so the two
cannot drift.

## At 10,000 webhooks/second

Today every delivery is a synchronous transaction, serialised per account on the
`account_stats` row lock — one hot account is the first thing to fall over.

- **Acknowledge and decouple**: handler appends to a log partitioned by
  `account_id` (Kafka/Redis Streams) and returns; consumers do the upsert and
  aggregation. `event_id` stays the idempotency key, so replay is safe.
- **Stop writing a row per event**: aggregate in windows, or treat
  `account_stats` as a rollup over append-only per-call rows. Removes the
  per-account lock from the hot path.
- **Dedup in Redis first**, with the constraint as the correctness backstop, so
  it becomes a capacity decision rather than a correctness one — at that volume
  it earns the extra moving part it does not earn today.
- **Batch** several deliveries per account into one transaction.
- **Make recording work a durable queue**, not goroutines.

## What I would do next

- Recording is still at-most-once across a hard kill: the event commits, the
  goroutine dies, and no retry comes because the event is now a duplicate. An
  outbox row written in the ingest transaction closes it. Biggest remaining gap.
- Metrics (accepted / duplicate / failed, aggregate lag) — a duplicate 200 is
  indistinguishable from a first delivery to anyone reading the logs.
- `occurred_at` is never stored outside the payload, so out-of-order deliveries
  can't be detected; late corrections win on arrival order, not event time.
- Out of scope per the brief, but absent: signature verification, body size
  limit, rate limiting.

## Running it

`002_event_id_unique.sql` adds the constraint the dedup relies on, and Postgres
applies migrations only on the first start of an empty volume — so on an
existing volume run `make reset`, not `docker compose up`, then `go test ./...`.
The suite is also clean under `go test ./... -race`.
