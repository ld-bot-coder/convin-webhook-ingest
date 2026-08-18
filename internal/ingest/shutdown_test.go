package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// "Deploy par in-flight kaam gayab ho jata hai" wala symptom. Goroutine ko
// koi track nahi karta tha, SIGTERM par process turant exit ho jata tha.
// Yahan polling jaan bujh ke nahi hai: Shutdown lautte hi kaam ho chuka ho.
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

// Drain ka edge case: shutdown shuru hone ke baad aayi delivery ka koi wait
// karne wala nahi bacha, to woh apna kaam inline karti hai.
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
