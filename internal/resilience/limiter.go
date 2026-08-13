// Package resilience 提供高并发下保护后端的四件套：
// 令牌桶限流、并发信号量、熔断器、退避重试。
//
// 共同约定：所有可能阻塞的方法都接收 context.Context，
// 并在 ctx 结束时立刻返回 —— 这是「链路可取消」的基础。
package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"

	"aictx/internal/trace"
)

// TokenBucket 是令牌桶限流器，控制单位时间的请求数（QPS）。
//
// 不用 time.Ticker 定时投放令牌，而是「按需惰性补发」：
// 每次取令牌时根据距上次的时间差算出应补多少。
// 好处是零后台 goroutine，限流器数量再多也不占调度资源。
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // 每秒补充的令牌数
	burst  float64 // 桶容量，即允许的瞬时突发量
	tokens float64
	last   time.Time
}

func NewTokenBucket(rate, burst float64) *TokenBucket {
	if burst < 1 {
		burst = 1
	}
	return &TokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// SetLimit 运行时热更新限流参数，前端调参面板会用到。
func (b *TokenBucket) SetLimit(rate, burst float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	b.rate = rate
	if burst < 1 {
		burst = 1
	}
	b.burst = burst
	if b.tokens > burst {
		b.tokens = burst
	}
}

func (b *TokenBucket) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

// Allow 非阻塞判定：有令牌就消耗一个返回 true，否则立即 false（快速失败）。
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Wait 阻塞直到拿到令牌，或 ctx 结束。
// 返回的 error 直接就是 ctx.Err()，能被上层 span 识别成 timeout / canceled。
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		b.refillLocked()
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		// 算出距离下一个令牌还差多久，精确睡眠，避免忙轮询烧 CPU
		need := (1 - b.tokens) / b.rate
		b.mu.Unlock()

		wait := time.Duration(need * float64(time.Second))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Tokens 返回当前可用令牌数，供监控面板展示。
func (b *TokenBucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	return b.tokens
}

// Semaphore 是可动态调整容量的计数信号量，控制「同时进行」的请求数。
//
// 与限流器的区别常被混淆：
//   - 令牌桶管的是「每秒进来多少」（速率）
//   - 信号量管的是「同时有多少在跑」（并发度）
//
// 调用下游 LLM 时真正需要护住的是后者：模型服务的连接数是有限的。
type Semaphore struct {
	mu       sync.Mutex
	capacity int
	inflight int
	waiters  []chan struct{}
	maxWait  int
}

func NewSemaphore(capacity int) *Semaphore {
	if capacity < 1 {
		capacity = 1
	}
	return &Semaphore{capacity: capacity, maxWait: 4096}
}

// Acquire 获取一个名额。ctx 结束则放弃排队并返回 ctx.Err()。
func (s *Semaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()
	if s.inflight < s.capacity {
		s.inflight++
		s.mu.Unlock()
		return nil
	}
	if len(s.waiters) >= s.maxWait {
		s.mu.Unlock()
		return fmt.Errorf("%w: 等待队列已满", trace.ErrRejected)
	}
	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()

	select {
	case <-ch:
		// 被 Release 唤醒，名额已由唤醒方转交，无需再自增
		return nil
	case <-ctx.Done():
		// 关键细节：超时退出时必须把自己从等待队列摘掉，
		// 否则 Release 会把名额交给一个已经没人接的 channel，导致名额泄漏。
		s.mu.Lock()
		for i, w := range s.waiters {
			if w == ch {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				s.mu.Unlock()
				return ctx.Err()
			}
		}
		// 没找到说明已被唤醒，名额已经属于我们了，必须归还，否则同样泄漏
		s.mu.Unlock()
		s.Release()
		return ctx.Err()
	}
}

// TryAcquire 非阻塞获取，拿不到立刻返回 false。
func (s *Semaphore) TryAcquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight < s.capacity {
		s.inflight++
		return true
	}
	return false
}

// Release 归还名额，优先转交给排队者。
func (s *Semaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiters) > 0 {
		ch := s.waiters[0]
		s.waiters = s.waiters[1:]
		close(ch) // 名额直接转交，inflight 不变
		return
	}
	if s.inflight > 0 {
		s.inflight--
	}
}

// SetCapacity 热更新并发上限。扩容时立即唤醒等待者。
func (s *Semaphore) SetCapacity(n int) {
	if n < 1 {
		n = 1
	}
	s.mu.Lock()
	s.capacity = n
	for s.inflight < s.capacity && len(s.waiters) > 0 {
		ch := s.waiters[0]
		s.waiters = s.waiters[1:]
		s.inflight++
		close(ch)
	}
	s.mu.Unlock()
}

// Stats 返回当前占用情况。
func (s *Semaphore) Stats() (inflight, capacity, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight, s.capacity, len(s.waiters)
}
