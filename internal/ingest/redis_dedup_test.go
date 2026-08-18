package ingest_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestRedisRemembersCommittedEvents checks the fast path in front of Postgres.
//
// The key must exist only once the transaction has committed, and must carry a
// TTL - an idempotency key with no expiry is a slow memory leak, one entry per
// event forever.
func TestRedisRemembersCommittedEvents(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	cfg := config.Load()
	rdb, err := redisclient.New(ctx, cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	key := "dedup:event:" + eventID
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	if err := rdb.Del(ctx, key).Err(); err != nil {
		t.Fatalf("clear key: %v", err)
	}

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	n, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if n != 1 {
		t.Fatalf("no dedup key for a committed event")
	}

	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("dedup key TTL is %v, want a positive value no greater than 24h", ttl)
	}

	// A redelivery is now answered from Redis. It must still be a no-op.
	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("redelivery: got %d, want 200", resp.StatusCode)
	}

	var stored int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&stored); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if stored != 1 {
		t.Fatalf("stored %d copies, want 1", stored)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", got)
	}
}

// TestIngestSurvivesRedisOutage points the service at a Redis that is not
// there. Because Redis is only an optimisation in front of Postgres, a dead
// one must cost latency and nothing else: deliveries still land, still dedupe,
// and still count correctly.
func TestIngestSurvivesRedisOutage(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Port 1 refuses connections; no retries, so the test stays quick.
	dead := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  200 * time.Millisecond,
		MaxRetries:   -1,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	t.Cleanup(func() { _ = dead.Close() })

	svc := ingest.New(st, stats.NewCache(), dead, slog.New(slog.NewTextHandler(io.Discard, nil)))

	evt := ingest.Event{
		EventID:     eventID,
		CallID:      callID,
		AccountID:   accountID,
		Status:      "completed",
		DurationSec: 143,
		OccurredAt:  time.Now().UTC(),
	}
	for i := range 3 {
		if err := svc.Ingest(ctx, evt); err != nil {
			t.Fatalf("delivery %d with redis down: %v", i, err)
		}
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", got)
	}
}
