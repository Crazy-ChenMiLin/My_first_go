// Package bench 是一个内置的压测发生器。
//
// 它存在的意义不是「测出这台机器能跑多少 QPS」，而是让限流、熔断、队列积压、
// 请求合并这些平时看不见的保护机制，在图表上真的动起来。
// 所以它刻意支持超过系统承载能力的压力 —— 被拒绝的请求同样是有价值的观测数据。
package bench

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Options 是一次压测的参数。
type Options struct {
	QPS         float64  `json:"qps"`         // 目标发压速率
	DurationSec int      `json:"durationSec"` // 持续时间
	Concurrency int      `json:"concurrency"` // 发压端并发上限（防止发压侧自己先崩）
	NoCache     bool     `json:"noCache"`     // 每条请求都打到底层，用来看真实链路压力
	Queries     []string `json:"queries"`     // 问题池，随机取
}

func (o *Options) normalize() {
	if o.QPS <= 0 {
		o.QPS = 20
	}
	if o.QPS > 5000 {
		o.QPS = 5000
	}
	if o.DurationSec <= 0 {
		o.DurationSec = 15
	}
	if o.DurationSec > 300 {
		o.DurationSec = 300
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 256
	}
	if o.Concurrency > 4000 {
		o.Concurrency = 4000
	}
	if len(o.Queries) == 0 {
		o.Queries = defaultQueries
	}
}

var defaultQueries = []string{
	"Go 的 context 在高并发链路里应该怎么传？",
	"令牌桶和漏桶的区别是什么？",
	"熔断器的半开状态是干什么用的？",
	"为什么流式响应不能在首 token 之后重试？",
	"worker pool 的队列该设多大？",
	"singleflight 适合哪些场景？",
	"P99 延迟高但 P50 正常，先查什么？",
	"如何给一条 AI 请求做超时预算切分？",
}

// Outcome 是单次请求的分类结果，由调用方（server）判定后回传。
type Outcome string

const (
	OK       Outcome = "ok"
	Failed   Outcome = "failed"
	Rejected Outcome = "rejected"
	Timeout  Outcome = "timeout"
	Canceled Outcome = "canceled"
)

// RunFunc 由调用方注入，负责真正执行一次请求。
type RunFunc func(ctx context.Context, query string, noCache bool) (Outcome, float64)

// Status 是压测的实时状态，直接 JSON 给前端。
type Status struct {
	Running     bool    `json:"running"`
	StartedAt   int64   `json:"startedAt"`
	ElapsedSec  float64 `json:"elapsedSec"`
	DurationSec int     `json:"durationSec"`
	TargetQPS   float64 `json:"targetQps"`
	Sent        int64   `json:"sent"`
	Done        int64   `json:"done"`
	OK          int64   `json:"ok"`
	Failed      int64   `json:"failed"`
	Rejected    int64   `json:"rejected"`
	Timeout     int64   `json:"timeout"`
	Canceled    int64   `json:"canceled"`
	Inflight    int64   `json:"inflight"`
	Dropped     int64   `json:"dropped"` // 发压端并发打满，来不及发出的
	ActualQPS   float64 `json:"actualQps"`
	SuccessPct  float64 `json:"successPct"`
	AvgMS       float64 `json:"avgMs"`
	P50MS       float64 `json:"p50Ms"`
	P95MS       float64 `json:"p95Ms"`
	P99MS       float64 `json:"p99Ms"`
	MaxMS       float64 `json:"maxMs"`
}

// Runner 同一时间只允许跑一场压测。
type Runner struct {
	run RunFunc

	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	opts    Options
	started time.Time
	stopped time.Time

	sent, done           atomic.Int64
	ok, failed, rejected atomic.Int64
	timeout, canceled    atomic.Int64
	inflight, dropped    atomic.Int64
	latMu                sync.Mutex
	latencies            []float64
	latSum               float64
	latMax               float64
}

func NewRunner(run RunFunc) *Runner { return &Runner{run: run} }

// Start 启动一场压测。已在跑时返回 false。
func (r *Runner) Start(opts Options) bool {
	opts.normalize()

	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.running = true
	r.cancel = cancel
	r.opts = opts
	r.started = time.Now()
	r.stopped = time.Time{}
	r.mu.Unlock()

	r.reset()
	go r.loop(ctx, opts)
	return true
}

func (r *Runner) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *Runner) reset() {
	r.sent.Store(0)
	r.done.Store(0)
	r.ok.Store(0)
	r.failed.Store(0)
	r.rejected.Store(0)
	r.timeout.Store(0)
	r.canceled.Store(0)
	r.dropped.Store(0)
	r.latMu.Lock()
	r.latencies = r.latencies[:0]
	r.latSum = 0
	r.latMax = 0
	r.latMu.Unlock()
}

// loop 是发压主循环。
//
// 这里用「按间隔 tick」而不是「每秒批量发 N 个」，是为了让压力平滑。
// 批量发压会造成人为的尖峰，令牌桶瞬间被打空，测出来的拒绝率毫无参考价值。
func (r *Runner) loop(ctx context.Context, opts Options) {
	defer func() {
		r.mu.Lock()
		r.running = false
		r.cancel = nil
		r.stopped = time.Now()
		r.mu.Unlock()
	}()

	interval := time.Duration(float64(time.Second) / opts.QPS)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	deadline := time.After(time.Duration(opts.DurationSec) * time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sem := make(chan struct{}, opts.Concurrency)
	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-deadline:
			wg.Wait()
			return
		case <-ticker.C:
			query := opts.Queries[rng.Intn(len(opts.Queries))]
			select {
			case sem <- struct{}{}:
			default:
				// 发压端自己饱和了。记为 dropped 而不是失败 ——
				// 这是压测工具的瓶颈，不是被测系统的问题，混在一起会得出错误结论。
				r.dropped.Add(1)
				continue
			}
			wg.Add(1)
			r.sent.Add(1)
			r.inflight.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				defer r.inflight.Add(-1)

				outcome, ms := r.run(ctx, query, opts.NoCache)
				r.record(outcome, ms)
			}()
		}
	}
}

func (r *Runner) record(o Outcome, ms float64) {
	r.done.Add(1)
	switch o {
	case OK:
		r.ok.Add(1)
	case Rejected:
		r.rejected.Add(1)
	case Timeout:
		r.timeout.Add(1)
	case Canceled:
		r.canceled.Add(1)
	default:
		r.failed.Add(1)
	}

	r.latMu.Lock()
	// 只保留最近 20000 个样本，避免长时间压测把内存吃干净
	if len(r.latencies) >= 20000 {
		r.latencies = r.latencies[1:]
	}
	r.latencies = append(r.latencies, ms)
	r.latSum += ms
	if ms > r.latMax {
		r.latMax = ms
	}
	r.latMu.Unlock()
}

// Status 返回实时状态快照。
func (r *Runner) Status() Status {
	r.mu.Lock()
	running := r.running
	opts := r.opts
	started := r.started
	stopped := r.stopped
	r.mu.Unlock()

	s := Status{
		Running:     running,
		DurationSec: opts.DurationSec,
		TargetQPS:   opts.QPS,
		Sent:        r.sent.Load(),
		Done:        r.done.Load(),
		OK:          r.ok.Load(),
		Failed:      r.failed.Load(),
		Rejected:    r.rejected.Load(),
		Timeout:     r.timeout.Load(),
		Canceled:    r.canceled.Load(),
		Inflight:    r.inflight.Load(),
		Dropped:     r.dropped.Load(),
	}
	if started.IsZero() {
		return s
	}
	s.StartedAt = started.UnixMilli()

	end := stopped
	if running || end.IsZero() {
		end = time.Now()
	}
	s.ElapsedSec = end.Sub(started).Seconds()
	if s.ElapsedSec > 0 {
		s.ActualQPS = float64(s.Done) / s.ElapsedSec
	}
	if s.Done > 0 {
		s.SuccessPct = float64(s.OK) / float64(s.Done) * 100
	}

	r.latMu.Lock()
	n := len(r.latencies)
	if n > 0 {
		s.AvgMS = r.latSum / float64(n)
		s.MaxMS = r.latMax
		sorted := make([]float64, n)
		copy(sorted, r.latencies)
		r.latMu.Unlock()
		sort.Float64s(sorted)
		s.P50MS = pick(sorted, 0.50)
		s.P95MS = pick(sorted, 0.95)
		s.P99MS = pick(sorted, 0.99)
	} else {
		r.latMu.Unlock()
	}
	return s
}

func pick(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1)*q + 0.5)
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
