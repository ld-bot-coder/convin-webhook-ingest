package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

// TestCacheRecordIsSafeUnderConcurrency drives Record from many goroutines,
// which is how it is really used: one call per in-flight webhook.
//
// Get takes the read lock but Record takes no lock at all, so the map write
// and the two counter increments race. Under `go test -race` this reports a
// data race; without it the increments are simply lost, which is one of the
// ways the dashboard numbers drift. Concurrent writes to the map itself can
// also take the whole process down with a fatal "concurrent map writes".
func TestCacheRecordIsSafeUnderConcurrency(t *testing.T) {
	c := stats.NewCache()

	const workers, perWorker = 16, 250
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				c.Record("acc_concurrent", 2)
			}
		}()
	}
	wg.Wait()

	got := c.Get("acc_concurrent")
	if want := int64(workers * perWorker); got.CallCount != want {
		t.Errorf("CallCount is %d, want %d — %d increments were lost", got.CallCount, want, want-got.CallCount)
	}
	if want := int64(workers * perWorker * 2); got.TotalDurationSec != want {
		t.Errorf("TotalDurationSec is %d, want %d", got.TotalDurationSec, want)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}
