// Package stats keeps a hot-path, in-memory view of per-account call totals.
//
// The durable copy of these numbers lives in Postgres; this cache exists so
// the stats endpoint does not hit the database on every read. Postgres stays
// the source of truth: the cache is only ever fed snapshots that the database
// has already committed.
package stats

import "sync"

// AccountStats is a point-in-time view of one account's totals.
type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Cache holds per-account running totals. It is safe for concurrent use.
type Cache struct {
	mu sync.RWMutex
	m  map[string]AccountStats
}

// NewCache returns an empty cache.
func NewCache() *Cache {
	return &Cache{m: make(map[string]AccountStats)}
}

// Get returns a snapshot of an account's totals. Unknown accounts read as zero.
func (c *Cache) Get(accountID string) AccountStats {
	s, _ := c.Lookup(accountID)
	return s
}

// Lookup returns an account's totals and reports whether the account was
// cached at all. Callers use the flag to tell "no calls yet" apart from
// "this process has never seen this account", which is the difference between
// answering zero and reading through to Postgres.
func (c *Cache) Lookup(accountID string) (AccountStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.m[accountID]
	return s, ok
}

// Record folds one completed call into an account's running totals.
func (c *Cache) Record(accountID string, durationSec int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s := c.m[accountID]
	s.CallCount++
	s.TotalDurationSec += int64(durationSec)
	c.m[accountID] = s
}

// Set replaces an account's totals with an authoritative snapshot — the
// numbers Postgres returned when it committed the aggregate.
//
// Ingests for one account commit in an order the service does not control, so
// a goroutine can arrive holding an older snapshot than one already cached.
// call_count never decreases, so a snapshot with a lower count is stale and is
// dropped rather than allowed to walk the cache backwards. Equal counts are
// applied, because a correction to an existing call changes the duration
// without changing the count.
func (c *Cache) Set(accountID string, s AccountStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cur, ok := c.m[accountID]; ok && cur.CallCount > s.CallCount {
		return
	}
	c.m[accountID] = s
}
