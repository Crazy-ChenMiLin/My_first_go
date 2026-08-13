package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

type cacheEntry struct {
	text string
	exp  time.Time
}

// ttlCache 是带过期时间与容量上限的极简缓存。
// 淘汰策略是 FIFO 而非严格 LRU —— 对回答缓存来说，实现复杂度的收益不划算。
type ttlCache struct {
	mu    sync.Mutex
	m     map[string]cacheEntry
	order []string
	max   int
}

func newCache(max int) *ttlCache {
	if max <= 0 {
		max = 1000
	}
	return &ttlCache{m: make(map[string]cacheEntry, max), max: max}
}

func (c *ttlCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return "", false
	}
	if time.Now().After(e.exp) {
		delete(c.m, key)
		return "", false
	}
	return e.text, true
}

func (c *ttlCache) Set(key, text string, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.m[key]; !exists {
		c.order = append(c.order, key)
		for len(c.order) > c.max {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.m, oldest)
		}
	}
	c.m[key] = cacheEntry{text: text, exp: time.Now().Add(ttl)}
}

func (c *ttlCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

func (c *ttlCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = make(map[string]cacheEntry, c.max)
	c.order = nil
}

func cacheKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
