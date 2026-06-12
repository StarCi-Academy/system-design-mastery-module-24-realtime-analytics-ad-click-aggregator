package main

import (
	"log"
	"sync"
)

// clickEnvelope is the value shape emitted by ingest-api.
type clickEnvelope struct {
	ClickID   string                 `json:"clickId"`
	AdID      string                 `json:"adId"`
	Payload   map[string]interface{} `json:"payload"`
	Timestamp string                 `json:"timestamp"`
}

// snapshot is the JSON returned by GET /counts.
type snapshot struct {
	Counts             map[string]int `json:"counts"`
	TotalUnique        int            `json:"totalUnique"`
	DuplicatesRejected int            `json:"duplicatesRejected"`
}

// counter holds in-memory counting + deduplication state for ad clicks.
//
// The dedup store (seen) is the idempotency mechanism: the first time a clickId
// arrives it is counted; any later arrival of the SAME clickId is dropped. This
// makes consumption idempotent under at-least-once delivery, where Kafka may
// redeliver a record after a rebalance or restart.
//
// The store is in-memory on purpose to keep the lesson dependency-free; the
// theory section discusses why production swaps it for Redis/RocksDB so dedup
// survives a consumer crash.
type counter struct {
	mu sync.Mutex
	// seen is the set of clickIds already counted — the deduplication index.
	seen map[string]struct{}
	// counts is the per-ad accepted (unique) click count.
	counts map[string]int
	// duplicates is the total duplicates rejected since boot, for observability.
	duplicates int
}

func newCounter() *counter {
	return &counter{
		seen:   make(map[string]struct{}),
		counts: make(map[string]int),
	}
}

// record processes one click, deduplicating by clickId. It returns true if the
// click was counted as new, false if rejected as a duplicate.
func (c *counter) record(env clickEnvelope) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Duplicate check FIRST — an already-seen clickId never re-increments.
	if _, ok := c.seen[env.ClickID]; ok {
		c.duplicates++
		log.Printf("DUPLICATE dropped clickId=%s adId=%s", env.ClickID, env.AdID)
		return false
	}
	c.seen[env.ClickID] = struct{}{}
	c.counts[env.AdID]++
	log.Printf("COUNTED clickId=%s adId=%s total=%d", env.ClickID, env.AdID, c.counts[env.AdID])
	return true
}

// snapshot returns the current per-ad counts, total unique clicks and the
// number of duplicates rejected.
func (c *counter) snapshot() snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make(map[string]int, len(c.counts))
	total := 0
	for adID, n := range c.counts {
		counts[adID] = n
		total += n
	}
	return snapshot{Counts: counts, TotalUnique: total, DuplicatesRejected: c.duplicates}
}
