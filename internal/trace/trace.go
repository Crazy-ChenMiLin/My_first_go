// Package trace 是本项目的链路追踪内核。
//
// 设计目标：用最少的代码把「一次 AI 请求在服务端到底发生了什么」完整记录下来，
// 并且做到 context 原生 —— span 挂在 context.Context 上随调用链向下传递，
// 父 span 结束/取消时，子 span 能立刻感知。
//
// 刻意不引入 OpenTelemetry：概念一致（Trace / Span / Attributes / Events），
// 但零依赖、可读、可改，适合作为理解链路传播的教学与生产骨架。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Kind 表示 span 所处的链路环节，前端据此上色。
type Kind string

const (
	KindHTTP      Kind = "http"      // HTTP 请求根节点
	KindGuard     Kind = "guard"     // 输入校验 / 内容安全
	KindCache     Kind = "cache"     // 缓存查询（含 singleflight 合并）
	KindQueue     Kind = "queue"     // 排队等待 worker
	KindLimiter   Kind = "limiter"   // 限流 / 信号量 / 熔断判定
	KindRetrieval Kind = "retrieval" // 检索扇出
	KindPrompt    Kind = "prompt"    // 提示词拼装
	KindLLM       Kind = "llm"       // 模型调用
	KindPost      Kind = "post"      // 后处理
)

// Status 是 span 的终态。区分 canceled / timeout / rejected 是本项目的重点：
// 它们在 context 语义下含义完全不同，排障时必须一眼分清。
type Status string

const (
	StatusRunning  Status = "running"  // 进行中
	StatusOK       Status = "ok"       // 正常完成
	StatusError    Status = "error"    // 业务或下游错误
	StatusCanceled Status = "canceled" // 上游主动取消（多为客户端断开）
	StatusTimeout  Status = "timeout"  // context deadline 到期
	StatusRejected Status = "rejected" // 被限流 / 熔断 / 队列满拒绝，未真正执行
)

// ErrRejected 供 resilience 包包装使用，End 时会被识别为 StatusRejected。
var ErrRejected = errors.New("rejected")

// Attr 是 span 上的键值标注。
type Attr struct {
	K string
	V any
}

// A 构造一个 Attr，纯粹为了调用处短一点。
func A(k string, v any) Attr { return Attr{K: k, V: v} }

// SpanEvent 是 span 生命周期内的时间点里程碑，例如 LLM 的首 token。
type SpanEvent struct {
	Name string `json:"name"`
	TS   int64  `json:"ts"`
}

// Span 是链路上的一段工作。字段全部私有，通过方法读写以保证并发安全 ——
// 高并发场景下同一个 span 可能被业务 goroutine 与 SSE 推送 goroutine 同时访问。
type Span struct {
	mu       sync.Mutex
	id       string
	traceID  string
	parentID string
	name     string
	kind     Kind
	start    time.Time
	end      time.Time
	status   Status
	errMsg   string
	attrs    map[string]any
	events   []SpanEvent
	ended    bool
	tracer   *Tracer
	trace    *Trace
}

// SpanView 是 span 的只读快照，用于 JSON 序列化给前端。
type SpanView struct {
	ID         string         `json:"id"`
	TraceID    string         `json:"traceId"`
	ParentID   string         `json:"parentId,omitempty"`
	Name       string         `json:"name"`
	Kind       Kind           `json:"kind"`
	StartMS    int64          `json:"startMs"`
	EndMS      int64          `json:"endMs,omitempty"`
	DurationMS float64        `json:"durationMs"`
	Status     Status         `json:"status"`
	Error      string         `json:"error,omitempty"`
	Attrs      map[string]any `json:"attrs,omitempty"`
	Events     []SpanEvent    `json:"events,omitempty"`
}

// Trace 是一次完整请求的所有 span 集合。
type Trace struct {
	mu      sync.Mutex
	id      string
	name    string
	start   time.Time
	end     time.Time
	status  Status
	live    bool // live=true 才逐 span 推送事件；压测流量只推最终摘要，避免事件风暴
	spans   []*Span
	rootID  string
	tokens  int
	tracer  *Tracer
	ended   bool
	attrs   map[string]any
	pending int
}

// TraceView 是 trace 的只读快照。
type TraceView struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	StartMS    int64          `json:"startMs"`
	EndMS      int64          `json:"endMs,omitempty"`
	DurationMS float64        `json:"durationMs"`
	Status     Status         `json:"status"`
	SpanCount  int            `json:"spanCount"`
	Tokens     int            `json:"tokens"`
	Attrs      map[string]any `json:"attrs,omitempty"`
	Spans      []SpanView     `json:"spans,omitempty"`
}

type ctxKey int

const (
	ctxKeySpan ctxKey = iota
	ctxKeyTracer
)

// Tracer 持有记录器与事件总线，是整个追踪体系的入口。
type Tracer struct {
	rec *Recorder
	hub *Hub
}

// NewTracer 创建 Tracer，maxTraces 为内存中保留的最近 trace 数量。
func NewTracer(maxTraces int) *Tracer {
	return &Tracer{rec: NewRecorder(maxTraces), hub: NewHub()}
}

func (t *Tracer) Recorder() *Recorder { return t.rec }
func (t *Tracer) Hub() *Hub           { return t.hub }

func newID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// StartTrace 开启一条新链路并返回其根 span。
// 返回的 context 已携带 tracer 与根 span，后续 Start 会自动挂为子节点。
func (t *Tracer) StartTrace(ctx context.Context, name string, live bool, attrs ...Attr) (context.Context, *Span) {
	tr := &Trace{
		id:     newID(8),
		name:   name,
		start:  time.Now(),
		status: StatusRunning,
		live:   live,
		tracer: t,
		attrs:  map[string]any{},
	}
	root := &Span{
		id:      newID(6),
		traceID: tr.id,
		name:    name,
		kind:    KindHTTP,
		start:   tr.start,
		status:  StatusRunning,
		attrs:   map[string]any{},
		tracer:  t,
		trace:   tr,
	}
	for _, a := range attrs {
		root.attrs[a.K] = a.V
		tr.attrs[a.K] = a.V
	}
	// 把 context 的剩余预算记下来，前端可以直观看到「这次请求有多少时间可花」。
	if dl, ok := ctx.Deadline(); ok {
		root.attrs["budgetMs"] = time.Until(dl).Milliseconds()
	}
	tr.rootID = root.id
	tr.spans = append(tr.spans, root)
	tr.pending = 1

	t.rec.Put(tr)
	ctx = context.WithValue(ctx, ctxKeyTracer, t)
	ctx = context.WithValue(ctx, ctxKeySpan, root)
	if live {
		t.hub.Publish(Event{Type: "span.start", TS: nowMS(), Span: ptr(root.View())})
	}
	return ctx, root
}

// Start 在当前 context 的 span 下开一个子 span。
// 若 context 中没有 tracer（例如单测直传 context.Background），返回一个空转 span，
// 保证业务代码永远不需要判空。
func Start(ctx context.Context, name string, kind Kind, attrs ...Attr) (context.Context, *Span) {
	parent, _ := ctx.Value(ctxKeySpan).(*Span)
	t, _ := ctx.Value(ctxKeyTracer).(*Tracer)
	if t == nil || parent == nil {
		return ctx, &Span{id: "noop", name: name, kind: kind, start: time.Now(), status: StatusRunning, attrs: map[string]any{}}
	}
	s := &Span{
		id:       newID(6),
		traceID:  parent.traceID,
		parentID: parent.id,
		name:     name,
		kind:     kind,
		start:    time.Now(),
		status:   StatusRunning,
		attrs:    map[string]any{},
		tracer:   t,
		trace:    parent.trace,
	}
	for _, a := range attrs {
		s.attrs[a.K] = a.V
	}
	tr := parent.trace
	tr.mu.Lock()
	tr.spans = append(tr.spans, s)
	tr.pending++
	live := tr.live
	tr.mu.Unlock()

	if live {
		t.hub.Publish(Event{Type: "span.start", TS: nowMS(), Span: ptr(s.View())})
	}
	return context.WithValue(ctx, ctxKeySpan, s), s
}

// SetAttr 追加标注。span 结束后再调用会被忽略。
func (s *Span) SetAttr(k string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = map[string]any{}
	}
	s.attrs[k] = v
}

// Mark 记录一个时间点里程碑，典型用途是 LLM 的 first_token。
func (s *Span) Mark(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, SpanEvent{Name: name, TS: nowMS()})
}

// End 结束 span。传入的 err 决定终态：
// nil→ok，context.Canceled→canceled，DeadlineExceeded→timeout，ErrRejected→rejected，其余→error。
func (s *Span) End(err error) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.end = time.Now()
	switch {
	case err == nil:
		s.status = StatusOK
	case errors.Is(err, ErrRejected):
		s.status = StatusRejected
		s.errMsg = err.Error()
	case errors.Is(err, context.Canceled):
		s.status = StatusCanceled
		s.errMsg = "上游取消"
	case errors.Is(err, context.DeadlineExceeded):
		s.status = StatusTimeout
		s.errMsg = "超出 context 预算"
	default:
		s.status = StatusError
		s.errMsg = err.Error()
	}
	view := s.viewLocked()
	t, tr := s.tracer, s.trace
	s.mu.Unlock()

	if t == nil || tr == nil {
		return
	}
	tr.mu.Lock()
	tr.pending--
	isRoot := tr.rootID == view.ID
	live := tr.live
	tr.mu.Unlock()

	if live {
		t.hub.Publish(Event{Type: "span.end", TS: nowMS(), Span: &view})
	}
	if isRoot {
		tr.finish(view.Status)
	}
}

// EndWithStatus 用于少数需要直接指定终态的场景（例如手动标记 rejected）。
func (s *Span) EndWithStatus(st Status, msg string) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.end = time.Now()
	s.status = st
	s.errMsg = msg
	view := s.viewLocked()
	t, tr := s.tracer, s.trace
	s.mu.Unlock()

	if t == nil || tr == nil {
		return
	}
	tr.mu.Lock()
	tr.pending--
	isRoot := tr.rootID == view.ID
	live := tr.live
	tr.mu.Unlock()

	if live {
		t.hub.Publish(Event{Type: "span.end", TS: nowMS(), Span: &view})
	}
	if isRoot {
		tr.finish(st)
	}
}

func (s *Span) TraceID() string { return s.traceID }
func (s *Span) ID() string      { return s.id }

// View 返回并发安全的快照。
func (s *Span) View() SpanView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewLocked()
}

func (s *Span) viewLocked() SpanView {
	v := SpanView{
		ID:       s.id,
		TraceID:  s.traceID,
		ParentID: s.parentID,
		Name:     s.name,
		Kind:     s.kind,
		StartMS:  s.start.UnixMilli(),
		Status:   s.status,
		Error:    s.errMsg,
	}
	if !s.end.IsZero() {
		v.EndMS = s.end.UnixMilli()
		v.DurationMS = float64(s.end.Sub(s.start).Microseconds()) / 1000
	} else {
		v.DurationMS = float64(time.Since(s.start).Microseconds()) / 1000
	}
	if len(s.attrs) > 0 {
		v.Attrs = make(map[string]any, len(s.attrs))
		for k, val := range s.attrs {
			v.Attrs[k] = val
		}
	}
	if len(s.events) > 0 {
		v.Events = append([]SpanEvent(nil), s.events...)
	}
	return v
}

// AddTokens 累加本次链路产出的 token 数，用于统计面板。
func (t *Trace) AddTokens(n int) {
	t.mu.Lock()
	t.tokens += n
	t.mu.Unlock()
}

// TraceOf 取出 span 所属的 trace，便于业务侧累加 token 等聚合信息。
func TraceOf(ctx context.Context) *Trace {
	s, _ := ctx.Value(ctxKeySpan).(*Span)
	if s == nil {
		return nil
	}
	return s.trace
}

// TraceIDFromContext 取当前链路 ID，日志里带上它就能和前端瀑布图对上。
func TraceIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(ctxKeySpan).(*Span)
	if s == nil {
		return ""
	}
	return s.traceID
}

func (t *Trace) finish(st Status) {
	t.mu.Lock()
	if t.ended {
		t.mu.Unlock()
		return
	}
	t.ended = true
	t.end = time.Now()
	t.status = st
	tracer := t.tracer
	t.mu.Unlock()

	if tracer != nil {
		v := t.View(false)
		tracer.hub.Publish(Event{Type: "trace.end", TS: nowMS(), Trace: &v})
	}
}

// View 返回 trace 快照，withSpans 为 true 时附带全部 span（详情页用）。
func (t *Trace) View(withSpans bool) TraceView {
	t.mu.Lock()
	spans := append([]*Span(nil), t.spans...)
	v := TraceView{
		ID:        t.id,
		Name:      t.name,
		StartMS:   t.start.UnixMilli(),
		Status:    t.status,
		SpanCount: len(spans),
		Tokens:    t.tokens,
	}
	if len(t.attrs) > 0 {
		v.Attrs = make(map[string]any, len(t.attrs))
		for k, val := range t.attrs {
			v.Attrs[k] = val
		}
	}
	if !t.end.IsZero() {
		v.EndMS = t.end.UnixMilli()
		v.DurationMS = float64(t.end.Sub(t.start).Microseconds()) / 1000
	} else {
		v.DurationMS = float64(time.Since(t.start).Microseconds()) / 1000
	}
	t.mu.Unlock()

	if withSpans {
		v.Spans = make([]SpanView, 0, len(spans))
		for _, s := range spans {
			v.Spans = append(v.Spans, s.View())
		}
	}
	return v
}

func (t *Trace) ID() string { return t.id }

func nowMS() int64 { return time.Now().UnixMilli() }

func ptr[T any](v T) *T { return &v }
