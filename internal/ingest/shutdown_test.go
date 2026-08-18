package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestShutdownDrainsInFlightRecordingWork covers the last symptom: "every time
// we deploy, whatever was in flight seems to just disappear".
//
// Recording work was handed to a bare goroutine that nothing tracked. On
// SIGTERM the HTTP server drained its handlers and the process exited straight
// afterwards, so any recording still being processed died with it - silently,
// and with the event already stored, so the provider never retried it.
//
// The assertion is deliberately made with no polling: once Shutdown returns,
// the work must already be done.
func TestShutdownDrainsInFlightRecordingWork(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now().UTC(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("Shutdown returned while recording work was still outstanding")
	}
}

// TestIngestAfterShutdownStillCompletesItsWork guards the edge the drain
// creates: a delivery that arrives once shutdown has begun has nothing left to
// wait for it, so it runs its recording work inline rather than handing it to
// a goroutine nobody will drain.
func TestIngestAfterShutdownStillCompletesItsWork(t *testing.T) {
	svc, st := testutil.NewService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	evt := ingest.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  60,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Now().UTC(),
	}
	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("work accepted after shutdown began was dropped")
	}
}
