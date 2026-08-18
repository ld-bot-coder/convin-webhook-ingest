package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// waitFor polls cond until it returns true or the deadline passes. Recording
// processing is asynchronous, so tests have to give it a moment to land.
func waitFor(t *testing.T, within time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// TestConcurrentRedeliveriesAreCountedOnce reproduces the duplicate records and
// inflated call-counts operations reported.
//
// The provider delivers at least once and retries aggressively, so the same
// event_id can be in flight several times at once. "Does it exist yet?"
// followed by "insert it" is not atomic: every concurrent delivery reads
// "absent" before any of them has written, so all of them insert and all of
// them increment the aggregate.
func TestConcurrentRedeliveriesAreCountedOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	const deliveries = 16
	body := eventJSON(eventID, callID, accountID)

	// Warm the pgx pool. It opens connections lazily, so an unwarmed pool
	// serialises the first burst behind connection setup and hides the race.
	warmPool(t, st, deliveries)

	// Each goroutine warms its own keep-alive connection first and then waits
	// at the barrier, so the deliveries actually overlap instead of being
	// staggered by connection setup. Without this the race is real but only
	// shows up intermittently.
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	codes := make([]int, deliveries)
	for i := range deliveries {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			warm, err := http.Get(srv.URL + "/healthz")
			if err == nil {
				_ = warm.Body.Close()
			}
			ready.Done()
			<-start
			codes[i] = post(t, srv.URL+"/webhooks/calls", body).StatusCode
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for i, code := range codes {
		if code != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, code)
		}
	}

	var stored int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&stored); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored %d copies of %s, want 1", stored, eventID)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Errorf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", got)
	}
}

// TestSequentialRedeliveriesAreCountedOnce is the uncontended version of the
// same guarantee: the provider redelivers even after a 200.
func TestSequentialRedeliveriesAreCountedOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := range 3 {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Errorf("durable stats: got %+v, want CallCount=1 TotalDurationSec=143", got)
	}

	live := statsEndpoint(t, srv.URL, accountID)
	if live.CallCount != 1 || live.TotalDurationSec != 143 {
		t.Errorf("stats endpoint: got %+v, want CallCount=1 TotalDurationSec=143", live)
	}
}

// TestMultipleEventsForOneCallCountAsOneCall covers the other half of the
// inflated-counts report: call_count is meant to track calls, but it is
// incremented once per accepted *event*. Two distinct events about the same
// call (a correction, a late status update) leave one row in `calls` and two
// in the aggregate, which is exactly "counts drifting higher than the actual
// number of calls".
func TestMultipleEventsForOneCallCountAsOneCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	first := eventJSONWithDuration(eventID+"_a", callID, accountID, 100)
	if resp := post(t, srv.URL+"/webhooks/calls", first); resp.StatusCode != http.StatusOK {
		t.Fatalf("first delivery: got %d, want 200", resp.StatusCode)
	}
	second := eventJSONWithDuration(eventID+"_b", callID, accountID, 150)
	if resp := post(t, srv.URL+"/webhooks/calls", second); resp.StatusCode != http.StatusOK {
		t.Fatalf("second delivery: got %d, want 200", resp.StatusCode)
	}

	var calls int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stored %d call rows, want 1", calls)
	}

	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 {
		t.Errorf("call_count is %d, want 1 — one call was reported twice", got.CallCount)
	}
	if got.TotalDurationSec != 150 {
		t.Errorf("total_duration_sec is %d, want 150 — the corrected duration", got.TotalDurationSec)
	}
}

// TestRecordingIsMarkedProcessed reproduces "calls are landing but their
// recordings never get marked processed, and there is nothing in the logs".
//
// The recording work is handed to a goroutine that keeps using the *request*
// context. net/http cancels that context the moment the handler returns, so
// the UPDATE 50ms later is refused with "context canceled" — and the error is
// dropped on the floor by an empty error branch, so nothing is logged.
func TestRecordingIsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	var processed bool
	ok := waitFor(t, 3*time.Second, func() bool {
		row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			return false
		}
		return processed
	})
	if !ok {
		t.Fatal("recording_processed is still false: the background work never completed")
	}
}

// TestStatsSurviveProcessRestart checks the read path against the durable
// numbers. The in-memory aggregate is only ever written by ingests handled by
// the current process, so a fresh process reports zero for accounts that
// plainly have calls in Postgres.
func TestStatsSurviveProcessRestart(t *testing.T) {
	before, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	if resp := post(t, before.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// A second server stands in for the process that comes up after a deploy:
	// same database, empty in-memory cache.
	after, _ := testutil.NewServer(t)

	got := statsEndpoint(t, after.URL, accountID)
	if got.CallCount != 1 || got.TotalDurationSec != 143 {
		t.Fatalf("stats after restart: got %+v, want CallCount=1 TotalDurationSec=143", got)
	}
}

type statsResponse struct {
	AccountID        string `json:"account_id"`
	CallCount        int64  `json:"call_count"`
	TotalDurationSec int64  `json:"total_duration_sec"`
}

func statsEndpoint(t *testing.T, baseURL, accountID string) statsResponse {
	t.Helper()
	resp, err := http.Get(baseURL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var out statsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return out
}

// eventJSONWithDuration builds a payload with an explicit duration.
func eventJSONWithDuration(eventID, callID, accountID string, durationSec int) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  %d,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, durationSec, callID)
}

// warmPool forces the connection pool to open n connections up front.
func warmPool(t *testing.T, st *store.Store, n int) {
	t.Helper()
	ctx := context.Background()
	conns := make([]*pgxpool.Conn, 0, n)
	for range n {
		c, err := st.Pool().Acquire(ctx)
		if err != nil {
			break
		}
		if _, err := c.Exec(ctx, "SELECT 1"); err != nil {
			t.Fatalf("warm pool: %v", err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Release()
	}
}
