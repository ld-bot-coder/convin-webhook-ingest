// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// EventExists reports whether an event with this ID has already been stored.
func (s *Store) EventExists(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// InsertEvent stores the raw delivery.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)`,
		e.EventID, e.CallID, e.AccountID, e.Payload)
	return err
}

// UpsertCall creates or refreshes the call record for this event.
func (s *Store) UpsertCall(ctx context.Context, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// IncrementAccountStats folds one completed call into the durable aggregate.
func (s *Store) IncrementAccountStats(ctx context.Context, accountID string, durationSec int) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET
		     call_count         = account_stats.call_count + 1,
		     total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
		accountID, durationSec)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

// IngestResult reports what one delivery actually changed.
type IngestResult struct {
	// Duplicate is true when this event_id had already been ingested, in
	// which case the delivery changed nothing.
	Duplicate bool
	// NewCall is true when the delivery created the call row rather than
	// correcting one that already existed.
	NewCall bool
	// Stats holds the account's durable totals as of this transaction.
	Stats Stats
}

// IngestEvent records one delivery and folds it into the account aggregate in
// a single transaction, exactly once per event_id.
//
// The three writes used to be independent statements against the pool, which
// left two holes. Any of them could fail after an earlier one had committed,
// stranding a stored event whose totals were never updated - and because the
// event was stored, every retry the provider sent was then discarded as a
// duplicate, so the aggregate never caught up. And the "have I seen this
// event?" read was separate from the insert that answered it, so overlapping
// redeliveries all read "absent" and all wrote.
//
// Both are closed here: one transaction, and the dedupe decision delegated to
// the unique constraint on events.event_id.
func (s *Store) IngestEvent(ctx context.Context, e Event) (IngestResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, err := claimEvent(ctx, tx, e)
	if err != nil {
		return IngestResult{}, err
	}
	if !claimed {
		// Someone already ingested this event. Report the totals as they
		// stand so the caller can still refresh its cache from them.
		st, err := readAccountStats(ctx, tx, e.AccountID)
		if err != nil {
			return IngestResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Duplicate: true, Stats: st}, nil
	}

	// Take the account's aggregate row lock before reading the call. Ingests
	// for one account are serialised from here to commit, which is what makes
	// the read-then-write on `calls` below safe: no other delivery for this
	// account can slip between them.
	if _, err := lockAccountStats(ctx, tx, e.AccountID); err != nil {
		return IngestResult{}, err
	}

	var prevDuration int
	err = tx.QueryRow(ctx,
		`SELECT duration_sec FROM calls WHERE call_id = $1`, e.CallID).Scan(&prevDuration)
	isNewCall := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !isNewCall {
		return IngestResult{}, err
	}

	if err := upsertCall(ctx, tx, e); err != nil {
		return IngestResult{}, err
	}

	// call_count counts calls, not events. A second event about a call that
	// already exists is a correction: it adjusts the duration by the
	// difference and leaves the count alone.
	countDelta, durationDelta := 0, e.DurationSec-prevDuration
	if isNewCall {
		countDelta, durationDelta = 1, e.DurationSec
	}

	var st Stats
	err = tx.QueryRow(ctx,
		`UPDATE account_stats
		    SET call_count         = call_count + $2,
		        total_duration_sec = total_duration_sec + $3
		  WHERE account_id = $1
		RETURNING call_count, total_duration_sec`,
		e.AccountID, countDelta, durationDelta).Scan(&st.CallCount, &st.TotalDurationSec)
	if err != nil {
		return IngestResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return IngestResult{}, err
	}
	return IngestResult{NewCall: isNewCall, Stats: st}, nil
}

// claimEvent stores the delivery and reports whether this caller is the one
// that stored it. The unique constraint on event_id is the atomic dedupe
// point: of any number of concurrent deliveries of one event, exactly one
// INSERT lands and the rest come back empty.
func claimEvent(ctx context.Context, tx pgx.Tx, e Event) (bool, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO events (event_id, call_id, account_id, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING id`,
		e.EventID, e.CallID, e.AccountID, e.Payload).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// lockAccountStats returns the account's aggregate row, creating it if this is
// the account's first call, and holds a row lock on it until the transaction
// ends.
//
// The no-op DO UPDATE is deliberate: ON CONFLICT DO NOTHING would return no
// row and take no lock, whereas assigning the column to itself locks the
// existing row and returns it in the same round trip.
func lockAccountStats(ctx context.Context, tx pgx.Tx, accountID string) (Stats, error) {
	var st Stats
	err := tx.QueryRow(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec)
		 VALUES ($1, 0, 0)
		 ON CONFLICT (account_id) DO UPDATE SET account_id = account_stats.account_id
		 RETURNING call_count, total_duration_sec`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	return st, err
}

// readAccountStats reads the aggregate inside a transaction without locking it.
func readAccountStats(ctx context.Context, tx pgx.Tx, accountID string) (Stats, error) {
	var st Stats
	err := tx.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	return st, err
}

// upsertCall creates or refreshes the call row inside a transaction.
func upsertCall(ctx context.Context, tx pgx.Tx, e Event) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (call_id) DO UPDATE SET
		     status        = EXCLUDED.status,
		     duration_sec  = EXCLUDED.duration_sec,
		     recording_url = EXCLUDED.recording_url,
		     updated_at    = now()`,
		e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL)
	return err
}
