package trace

import "sync"

// Event 是推送给前端的链路事件。
type Event struct {
	Type  string     `json:"type"` // span.start | span.end | trace.end
	TS    int64      `json:"ts"`
	Span  *SpanView  `json:"span,omitempty"`
	Trace *TraceView `json:"trace,omitempty"`
}

// Hub 是一个极简的发布订阅总线，供 SSE 广播链路事件。
//
// 核心原则：Publish 永不阻塞。
// 业务 goroutine 正在处理请求，绝不能因为某个浏览器标签页读得慢就被卡住。
// 订阅者 channel 满了就直接丢弃该事件，并计数为 dropped —— 观测性让位于可用性。
type Hub struct {
	mu      sync.RWMutex
	subs    map[int]chan Event
	next    int
	dropped int64
}

func NewHub() *Hub {
	return &Hub{subs: make(map[int]chan Event)}
}

// Subscribe 返回订阅 ID 与事件通道。调用方必须在结束时 Unsubscribe。
func (h *Hub) Subscribe(buf int) (int, <-chan Event) {
	if buf <= 0 {
		buf = 256
	}
	ch := make(chan Event, buf)
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = ch
	h.mu.Unlock()
	return id, ch
}

func (h *Hub) Unsubscribe(id int) {
	h.mu.Lock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
	h.mu.Unlock()
}

// Publish 非阻塞广播。
func (h *Hub) Publish(ev Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default:
			h.dropped++ // 读锁下的自增有轻微竞态，但这只是观测指标，不值得为它加写锁
		}
	}
}

// Subscribers 返回当前订阅者数量。
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
