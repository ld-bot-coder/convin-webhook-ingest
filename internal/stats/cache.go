// Package stats keeps a hot-path, in-memory view of per-account call totals.
// Durable copy Postgres me hai, yeh sirf read ko sasta karta hai.
package stats

import "sync"

// AccountStats is a point-in-time view of one account's totals.
type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

// Cache holds per-account running totals. Safe for concurrent use.
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

// Lookup batata hai account cached tha ya nahi. "zero calls" aur "kabhi dekha
// hi nahi" me farq karne ke liye - dusre case me Postgres se padhna padta hai.
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

// Set overwrites totals with a snapshot Postgres already commit kar chuka hai.
// Ingests kis order me commit honge pata nahi, isliye purana snapshot (kam
// call_count) drop kar dete hain. Barabar count matlab duration correction hai,
// woh apply hota hai.
func (c *Cache) Set(accountID string, s AccountStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cur, ok := c.m[accountID]; ok && cur.CallCount > s.CallCount {
		return
	}
	c.m[accountID] = s
}
