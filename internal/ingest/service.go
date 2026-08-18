// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const (
	// recordingWork stands in for downloading and transcoding a recording.
	recordingWork = 50 * time.Millisecond
	// recordingTimeout bounds that work once it is off the request context.
	recordingTimeout = 30 * time.Second
	// statsReadTimeout bounds the read-through to Postgres on a cache miss.
	statsReadTimeout = 2 * time.Second
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// mu guards closed and the Add side of inFlight, so that shutdown and a
	// late delivery cannot race over whether work is still being accepted.
	mu       sync.Mutex
	closed   bool
	inFlight sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the totals for an account.
//
// The in-memory aggregate only knows about ingests this process handled, so on
// a miss it reads through to Postgres and caches the answer. Without that, a
// process that has just come up after a deploy reports zero for every account
// until new webhooks happen to arrive for it.
func (s *Service) Stats(accountID string) stats.AccountStats {
	if st, ok := s.cache.Lookup(accountID); ok {
		return st
	}

	ctx, cancel := context.WithTimeout(context.Background(), statsReadTimeout)
	defer cancel()

	durable, err := s.store.AccountStats(ctx, accountID)
	if err != nil {
		s.log.Error("read account stats", "account_id", accountID, "err", err)
		return stats.AccountStats{}
	}

	st := stats.AccountStats{
		CallCount:        durable.CallCount,
		TotalDurationSec: durable.TotalDurationSec,
	}
	s.cache.Set(accountID, st)
	return st
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// The provider delivers at least once, so this is idempotent on event_id:
// storing the event, updating the call, and folding it into the account
// aggregate all happen in one transaction that only one delivery of a given
// event can win.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	res, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}

	// Postgres just told us the committed totals, so the cache is refreshed
	// from those rather than incremented independently. The two cannot drift.
	s.cache.Set(rec.AccountID, stats.AccountStats{
		CallCount:        res.Stats.CallCount,
		TotalDurationSec: res.Stats.TotalDurationSec,
	})

	if res.Duplicate {
		s.log.Info("duplicate delivery ignored", "event_id", rec.EventID, "call_id", rec.CallID)
		return nil
	}

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.startRecordingWork(ctx, rec)
	}

	return nil
}

// startRecordingWork runs the recording pipeline off the request path while
// keeping it accounted for, so that shutdown can wait for it.
func (s *Service) startRecordingWork(ctx context.Context, rec store.Event) {
	work := func() {
		// Detach from the request context but keep its values. net/http
		// cancels the request context the moment the handler returns, so the
		// UPDATE at the end of this work used to be refused with
		// "context canceled" - and the error was discarded, which is why
		// nothing about it ever reached the logs.
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordingTimeout)
		defer cancel()

		if err := s.processRecording(workCtx, rec); err != nil {
			s.log.Error("process recording",
				"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
		}
	}

	s.mu.Lock()
	if s.closed {
		// Shutting down: nothing will be left to wait for this goroutine, so
		// run it inline rather than dropping the work on the floor.
		s.mu.Unlock()
		work()
		return
	}
	s.inFlight.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.inFlight.Done()
		work()
	}()
}

// Shutdown stops accepting new background work and waits for what is already
// running, so that a deploy does not discard recordings that are mid-flight.
// It returns ctx.Err() if the wait does not finish in time.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	select {
	case <-time.After(recordingWork):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
