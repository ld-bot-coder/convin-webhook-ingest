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

// IngestResult reports what one delivery changed.
type IngestResult struct {
	// Duplicate true matlab yeh event_id pehle hi aa chuka tha.
	Duplicate bool
	// Stats are the account's durable totals as of this transaction.
	Stats Stats
}

// IngestEvent ek delivery ko store karta hai aur aggregate update karta hai,
// ek hi transaction me, per event_id exactly once.
//
// Pehle teeno writes alag alag chal rahe the: beech me fail hone par event
// store ho jata tha par totals update nahi hote the, aur uske baad har retry
// duplicate maan ke drop ho jati thi - to aggregate kabhi catch up nahi karta.
// Dedup ka faisla ab events.event_id ke unique constraint ka hai.
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
		st, err := readAccountStats(ctx, tx, e.AccountID)
		if err != nil {
			return IngestResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Duplicate: true, Stats: st}, nil
	}

	// Account row ka lock pehle lo. Isse ek account ki ingests yahan se commit
	// tak serialise ho jati hain, tabhi neeche ka read-then-write safe hai.
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

	// call_count calls ginta hai, events nahi. Purane call ka dobara event aaye
	// to woh correction hai: sirf duration ka difference lagta hai.
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
	return IngestResult{Stats: st}, nil
}

// claimEvent event store karta hai aur batata hai ki isi caller ne store kiya.
// Unique constraint hi atomic dedup point hai: kitni bhi parallel deliveries
// ho, INSERT sirf ek ka lagta hai, baaki ko zero rows milte hain.
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

// lockAccountStats aggregate row return karta hai (nahi hai to bana ke) aur
// transaction khatam hone tak uska row lock rakhta hai.
//
// No-op DO UPDATE jaan bujh ke hai: DO NOTHING na row deta hai na lock leta.
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

// readAccountStats reads the aggregate inside a transaction, bina lock liye.
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
