// Package pool 提供有界队列的固定 worker 池。
//
// 为什么不直接 go func()：
// Go 的 goroutine 很便宜，但「便宜」不等于「免费」。
// AI 请求每个都要占内存、连接和下游配额，突发 10 万请求时无脑开 goroutine
// 会让内存暴涨、下游被打穿，最终整体雪崩。
//
// worker 池把并发度钉死在一个已知值上，超出的请求排队；
// 队列也满了就快速失败 —— 这比让所有人一起慢慢等要好得多。
package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"aictx/internal/trace"
)

type job struct {
	ctx      context.Context
	fn       func(context.Context) error
	res      chan error
	enqueued time.Time
}

// Pool 是 worker 池。
type Pool struct {
	q      chan *job
	shrink chan struct{}
	quit   chan struct{}

	mu      sync.Mutex
	workers int
	closed  bool

	queueTimeout atomic.Int64 // 纳秒，支持热更新

	inflight  atomic.Int64
	completed atomic.Int64
	rejected  atomic.Int64
	expired   atomic.Int64
	waitNanos atomic.Int64
	waitCount atomic.Int64
	wg        sync.WaitGroup
}

func (p *Pool) qTimeout() time.Duration { return time.Duration(p.queueTimeout.Load()) }

// SetQueueTimeout 热更新排队上限。
func (p *Pool) SetQueueTimeout(d time.Duration) {
	if d <= 0 {
		d = 2 * time.Second
	}
	p.queueTimeout.Store(int64(d))
}

// New 创建 worker 池。queueSize 是排队上限，queueTimeout 是排队等待上限。
func New(workers, queueSize int, queueTimeout time.Duration) *Pool {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	if queueTimeout <= 0 {
		queueTimeout = 2 * time.Second
	}
	p := &Pool{
		q:      make(chan *job, queueSize),
		shrink: make(chan struct{}),
		quit:   make(chan struct{}),
	}
	p.queueTimeout.Store(int64(queueTimeout))
	for range workers {
		p.spawn()
	}
	return p
}

func (p *Pool) spawn() {
	p.mu.Lock()
	p.workers++
	p.mu.Unlock()
	p.wg.Add(1)
	go p.loop()
}

func (p *Pool) loop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.quit:
			return
		case <-p.shrink:
			p.mu.Lock()
			p.workers--
			p.mu.Unlock()
			return
		case j := <-p.q:
			p.run(j)
		}
	}
}

func (p *Pool) run(j *job) {
	waited := time.Since(j.enqueued)
	p.waitNanos.Add(int64(waited))
	p.waitCount.Add(1)

	// 任务在队列里躺着的这段时间，调用方可能早就断开了。
	// 先检查一次 ctx，别做无用功 —— 这是排队系统最容易漏掉的一步。
	if err := j.ctx.Err(); err != nil {
		j.res <- err
		return
	}

	// 按「排队年龄」主动丢弃。
	//
	// 只在入队时限时是不够的：队列没满时任务瞬间入队，然后可以在里面躺任意久。
	// 突发过去后，队列里堆的全是调用方已经放弃的陈旧请求，
	// 系统却还在老老实实执行它们，于是永远追不上进度 —— 这就是 bufferbloat。
	// 与其做完了没人要，不如立刻丢掉，把算力让给还有人等的新请求。
	if to := p.qTimeout(); waited > to {
		p.expired.Add(1)
		j.res <- fmt.Errorf("%w: 已在队列中等待 %s，超过上限 %s，主动丢弃", trace.ErrRejected, waited.Round(time.Millisecond), to)
		return
	}
	p.inflight.Add(1)
	defer func() {
		p.inflight.Add(-1)
		p.completed.Add(1)
	}()
	j.res <- j.fn(j.ctx)
}

// Run 提交任务并等待其完成。
//
// 入队等待用的是「父 ctx 再套一层更短的超时」：
// 排队最多花 queueTimeout，剩下的预算全部留给真正的执行。
// 这就是 context 超时逐层递减的典型用法。
func (p *Pool) Run(ctx context.Context, fn func(context.Context) error) error {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return fmt.Errorf("%w: 服务正在关闭", trace.ErrRejected)
	}

	j := &job{ctx: ctx, fn: fn, res: make(chan error, 1), enqueued: time.Now()}

	timeout := p.qTimeout()
	select {
	case p.q <- j: // 队列有空位，秒入
	default:
		enqueueCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		select {
		case p.q <- j:
		case <-enqueueCtx.Done():
			p.rejected.Add(1)
			if ctx.Err() != nil {
				return ctx.Err() // 父 ctx 先结束，是取消/超时
			}
			return fmt.Errorf("%w: 排队超过 %s，队列已满", trace.ErrRejected, timeout)
		}
	}

	select {
	case err := <-j.res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetWorkers 热调整 worker 数量。
func (p *Pool) SetWorkers(n int) {
	if n < 1 {
		n = 1
	}
	p.mu.Lock()
	cur := p.workers
	p.mu.Unlock()

	for range max(0, n-cur) {
		p.spawn()
	}
	for range max(0, cur-n) {
		select {
		case p.shrink <- struct{}{}:
		default:
		}
	}
}

// Stats 是面板展示用的池状态。
type Stats struct {
	Workers     int     `json:"workers"`
	Queued      int     `json:"queued"`
	QueueCap    int     `json:"queueCap"`
	Inflight    int64   `json:"inflight"`
	Completed   int64   `json:"completed"`
	Rejected    int64   `json:"rejected"` // 队列满，压根没入队
	Expired     int64   `json:"expired"`  // 入队了但排太久，执行前被丢弃
	AvgWaitMS   float64 `json:"avgWaitMs"`
	QueueTimeMS int64   `json:"queueTimeoutMs"`
}

func (p *Pool) Stats() Stats {
	p.mu.Lock()
	w := p.workers
	p.mu.Unlock()
	done := p.completed.Load()
	avg := 0.0
	if n := p.waitCount.Load(); n > 0 {
		avg = float64(p.waitNanos.Load()) / float64(n) / 1e6
	}
	return Stats{
		Workers:     w,
		Queued:      len(p.q),
		QueueCap:    cap(p.q),
		Inflight:    p.inflight.Load(),
		Completed:   done,
		Rejected:    p.rejected.Load(),
		Expired:     p.expired.Load(),
		AvgWaitMS:   avg,
		QueueTimeMS: p.qTimeout().Milliseconds(),
	}
}

// Close 停止所有 worker，等待在跑的任务结束。
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	close(p.quit)
	p.wg.Wait()
}
