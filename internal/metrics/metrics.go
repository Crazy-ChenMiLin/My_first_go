// Package metrics 是零依赖的进程内指标采集。
//
// 只做三件真正有用的事：
//  1. 分类计数（成功 / 失败 / 拒绝 / 取消 / 超时）—— 拒绝和失败必须分开看；
//  2. 延迟分位数 —— 平均值会骗人，P95/P99 才反映真实体验；
//  3. 按秒滚动窗口 —— 压测时能看到曲线，而不是一个静态总数。
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// 固定桶边界（毫秒）。固定桶的好处是内存恒定、写入 O(log n)，
// 代价是分位数为近似值 —— 对服务观测来说完全够用。
var buckets = []float64{1, 2, 5, 10, 20, 35, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000, 30000}

type histogram struct {
	mu     sync.Mutex
	counts []int64
	over   int64
	sum    float64
	n      int64
	maxVal float64
}

func newHistogram() *histogram { return &histogram{counts: make([]int64, len(buckets))} }

func (h *histogram) observe(ms float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
	h.sum += ms
	if ms > h.maxVal {
		h.maxVal = ms
	}
	for i, b := range buckets {
		if ms <= b {
			h.counts[i]++
			return
		}
	}
	h.over++
}

// quantile 用桶内线性插值估算分位数。
func (h *histogram) quantile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == 0 {
		return 0
	}
	target := q * float64(h.n)
	var cum float64
	prev := 0.0
	for i, b := range buckets {
		next := cum + float64(h.counts[i])
		if next >= target && h.counts[i] > 0 {
			frac := (target - cum) / float64(h.counts[i])
			return prev + (b-prev)*frac
		}
		cum = next
		prev = b
	}
	return h.maxVal
}

func (h *histogram) avg() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.n == 0 {
		return 0
	}
	return h.sum / float64(h.n)
}

const windowSize = 90 // 保留最近 90 秒

type slot struct {
	sec   int64
	ok    int64
	fail  int64
	rej   int64
	ms    float64
	count int64
}

// Metrics 是全局指标聚合器。
type Metrics struct {
	total    atomic.Int64
	success  atomic.Int64
	failed   atomic.Int64
	rejected atomic.Int64
	canceled atomic.Int64
	timeout  atomic.Int64
	tokens   atomic.Int64
	inflight atomic.Int64
	sfHits   atomic.Int64 // singleflight 合并命中
	cacheHit atomic.Int64
	retries  atomic.Int64

	lat  *histogram // 端到端延迟
	ttft *histogram // 首 token 延迟

	mu    sync.Mutex
	slots [windowSize]slot
	start time.Time
}

func New() *Metrics {
	return &Metrics{lat: newHistogram(), ttft: newHistogram(), start: time.Now()}
}

// Outcome 是一次请求的结果分类。
type Outcome string

const (
	OutcomeOK       Outcome = "ok"
	OutcomeFailed   Outcome = "failed"
	OutcomeRejected Outcome = "rejected"
	OutcomeCanceled Outcome = "canceled"
	OutcomeTimeout  Outcome = "timeout"
)

func (m *Metrics) IncInflight() { m.inflight.Add(1) }
func (m *Metrics) DecInflight() { m.inflight.Add(-1) }
func (m *Metrics) AddTokens(n int) {
	if n > 0 {
		m.tokens.Add(int64(n))
	}
}
func (m *Metrics) IncSingleflightHit() { m.sfHits.Add(1) }
func (m *Metrics) IncCacheHit()        { m.cacheHit.Add(1) }
func (m *Metrics) IncRetry()           { m.retries.Add(1) }

// ObserveTTFT 记录首 token 延迟，这是流式体验最关键的指标。
func (m *Metrics) ObserveTTFT(ms float64) { m.ttft.observe(ms) }

// Observe 记录一次完整请求。
func (m *Metrics) Observe(o Outcome, latencyMS float64) {
	m.total.Add(1)
	switch o {
	case OutcomeOK:
		m.success.Add(1)
	case OutcomeRejected:
		m.rejected.Add(1)
	case OutcomeCanceled:
		m.canceled.Add(1)
	case OutcomeTimeout:
		m.timeout.Add(1)
	default:
		m.failed.Add(1)
	}
	m.lat.observe(latencyMS)

	sec := time.Now().Unix()
	idx := int(sec % windowSize)
	m.mu.Lock()
	if m.slots[idx].sec != sec {
		m.slots[idx] = slot{sec: sec} // 时间轮转一圈，覆盖旧数据
	}
	switch o {
	case OutcomeOK:
		m.slots[idx].ok++
	case OutcomeRejected:
		m.slots[idx].rej++
	default:
		m.slots[idx].fail++
	}
	m.slots[idx].ms += latencyMS
	m.slots[idx].count++
	m.mu.Unlock()
}

// Point 是一秒的聚合值。
type Point struct {
	Sec   int64   `json:"sec"`
	OK    int64   `json:"ok"`
	Fail  int64   `json:"fail"`
	Rej   int64   `json:"rej"`
	AvgMS float64 `json:"avgMs"`
}

// Snapshot 是给前端的完整指标快照。
type Snapshot struct {
	Total      int64   `json:"total"`
	Success    int64   `json:"success"`
	Failed     int64   `json:"failed"`
	Rejected   int64   `json:"rejected"`
	Canceled   int64   `json:"canceled"`
	Timeout    int64   `json:"timeout"`
	Inflight   int64   `json:"inflight"`
	Tokens     int64   `json:"tokens"`
	Merged     int64   `json:"merged"`
	CacheHits  int64   `json:"cacheHits"`
	Retries    int64   `json:"retries"`
	QPS        float64 `json:"qps"`
	AvgMS      float64 `json:"avgMs"`
	P50MS      float64 `json:"p50Ms"`
	P95MS      float64 `json:"p95Ms"`
	P99MS      float64 `json:"p99Ms"`
	TTFTP50    float64 `json:"ttftP50"`
	TTFTP95    float64 `json:"ttftP95"`
	SuccessPct float64 `json:"successPct"`
	UptimeSec  float64 `json:"uptimeSec"`
	Series     []Point `json:"series"`
}

// Snapshot 生成快照。窗口序列按时间升序返回，最后一个是当前秒。
func (m *Metrics) Snapshot(seriesLen int) Snapshot {
	if seriesLen <= 0 || seriesLen > windowSize {
		seriesLen = 60
	}
	now := time.Now().Unix()

	m.mu.Lock()
	series := make([]Point, 0, seriesLen)
	var recentReq int64
	for i := seriesLen - 1; i >= 0; i-- {
		sec := now - int64(i)
		s := m.slots[int(sec%windowSize)]
		p := Point{Sec: sec}
		if s.sec == sec {
			p.OK, p.Fail, p.Rej = s.ok, s.fail, s.rej
			if s.count > 0 {
				p.AvgMS = s.ms / float64(s.count)
			}
		}
		// 最近 5 秒（跳过当前未完成的这一秒）用于算实时 QPS
		if i >= 1 && i <= 5 {
			recentReq += p.OK + p.Fail + p.Rej
		}
		series = append(series, p)
	}
	m.mu.Unlock()

	total := m.total.Load()
	succ := m.success.Load()
	pct := 0.0
	if total > 0 {
		pct = float64(succ) / float64(total) * 100
	}
	return Snapshot{
		Total:      total,
		Success:    succ,
		Failed:     m.failed.Load(),
		Rejected:   m.rejected.Load(),
		Canceled:   m.canceled.Load(),
		Timeout:    m.timeout.Load(),
		Inflight:   m.inflight.Load(),
		Tokens:     m.tokens.Load(),
		Merged:     m.sfHits.Load(),
		CacheHits:  m.cacheHit.Load(),
		Retries:    m.retries.Load(),
		QPS:        float64(recentReq) / 5.0,
		AvgMS:      m.lat.avg(),
		P50MS:      m.lat.quantile(0.50),
		P95MS:      m.lat.quantile(0.95),
		P99MS:      m.lat.quantile(0.99),
		TTFTP50:    m.ttft.quantile(0.50),
		TTFTP95:    m.ttft.quantile(0.95),
		SuccessPct: pct,
		UptimeSec:  time.Since(m.start).Seconds(),
		Series:     series,
	}
}

// Reset 清零所有指标，压测前点一下更干净。
func (m *Metrics) Reset() {
	m.total.Store(0)
	m.success.Store(0)
	m.failed.Store(0)
	m.rejected.Store(0)
	m.canceled.Store(0)
	m.timeout.Store(0)
	m.tokens.Store(0)
	m.sfHits.Store(0)
	m.cacheHit.Store(0)
	m.retries.Store(0)
	m.lat = newHistogram()
	m.ttft = newHistogram()
	m.mu.Lock()
	m.slots = [windowSize]slot{}
	m.start = time.Now()
	m.mu.Unlock()
}
