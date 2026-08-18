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
	recordingWork    = 50 * time.Millisecond
	recordingTimeout = 30 * time.Second
	statsReadTimeout = 2 * time.Second
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	// mu closed aur inFlight.Add ko guard karta hai, taki shutdown aur late
	// delivery ke beech race na ho.
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
// Cache me sirf isi process ke ingests hote hain, isliye miss par Postgres se
// padh ke cache karte hain. Warna deploy ke baad har account zero dikhta hai.
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
// Provider at-least-once bhejta hai, to yeh event_id par idempotent hai.
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

	// Totals wahi jo transaction ne commit kiye, alag se increment nahi -
	// isliye cache aur DB drift nahi kar sakte.
	s.cache.Set(rec.AccountID, stats.AccountStats{
		CallCount:        res.Stats.CallCount,
		TotalDurationSec: res.Stats.TotalDurationSec,
	})

	if res.Duplicate {
		s.log.Info("duplicate delivery ignored", "event_id", rec.EventID, "call_id", rec.CallID)
		return nil
	}

	if rec.RecordingURL != "" {
		s.startRecordingWork(ctx, rec)
	}

	return nil
}

// startRecordingWork recording ko request path se hata deta hai, par count me
// rakhta hai taki shutdown uska wait kar sake.
func (s *Service) startRecordingWork(ctx context.Context, rec store.Event) {
	work := func() {
		// Request ctx se detach, values rakh ke. net/http handler return hote
		// hi request ctx cancel kar deta hai - isi wajah se aakhri UPDATE
		// "context canceled" se fail hota tha.
		workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordingTimeout)
		defer cancel()

		if err := s.processRecording(workCtx, rec); err != nil {
			s.log.Error("process recording",
				"event_id", rec.EventID, "call_id", rec.CallID, "err", err)
		}
	}

	s.mu.Lock()
	if s.closed {
		// Shutdown chalu hai, koi wait karne wala nahi bacha - inline chala do.
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

// Shutdown naya background work lena band karta hai aur jo chal raha hai uska
// wait karta hai, taki deploy par mid-flight recordings na chhoot jaayein.
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
