package trace

import "sync"

// Recorder 用固定容量的环形结构保存最近若干条 trace。
//
// 为什么不用无界 map：高并发下每秒可能产生上千条 trace，
// 无界保存必然 OOM。这里用「插入即淘汰最旧」的策略把内存钉死。
type Recorder struct {
	mu    sync.RWMutex
	max   int
	order []string          // 按写入顺序保存 traceID
	byID  map[string]*Trace // traceID -> trace
}

func NewRecorder(max int) *Recorder {
	if max <= 0 {
		max = 500
	}
	return &Recorder{max: max, byID: make(map[string]*Trace, max)}
}

// Put 写入一条 trace，超出容量时淘汰最旧的一条。
func (r *Recorder) Put(t *Trace) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[t.id] = t
	r.order = append(r.order, t.id)
	for len(r.order) > r.max {
		oldest := r.order[0]
		r.order = r.order[1:]
		delete(r.byID, oldest)
	}
}

// Get 按 ID 取回 trace。
func (r *Recorder) Get(id string) (*Trace, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byID[id]
	return t, ok
}

// Recent 返回最近 limit 条 trace 的摘要，最新的在前。
func (r *Recorder) Recent(limit int) []TraceView {
	r.mu.RLock()
	ids := make([]string, 0, len(r.order))
	for i := len(r.order) - 1; i >= 0 && len(ids) < limit; i-- {
		ids = append(ids, r.order[i])
	}
	traces := make([]*Trace, 0, len(ids))
	for _, id := range ids {
		if t, ok := r.byID[id]; ok {
			traces = append(traces, t)
		}
	}
	r.mu.RUnlock()

	// 快照在锁外做，避免持有 recorder 锁时再去抢 trace/span 锁造成锁竞争。
	out := make([]TraceView, 0, len(traces))
	for _, t := range traces {
		out = append(out, t.View(false))
	}
	return out
}

// Len 返回当前保存的 trace 数量。
func (r *Recorder) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.order)
}
