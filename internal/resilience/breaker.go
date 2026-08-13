package resilience

import (
	"fmt"
	"sync"
	"time"

	"aictx/internal/trace"
)

// BreakerState 是熔断器三态。
type BreakerState string

const (
	StateClosed   BreakerState = "closed"    // 正常放行
	StateOpen     BreakerState = "open"      // 全部拒绝，快速失败
	StateHalfOpen BreakerState = "half_open" // 放少量探针试探恢复
)

// Breaker 是滑动窗口熔断器。
//
// 为什么 AI 网关特别需要熔断：模型服务一旦抖动，单次调用可能要挂 30 秒才超时。
// 如果不熔断，请求会持续堆积、占满连接与内存，把「下游慢」放大成「自己崩」。
// 熔断的本质是：宁可立刻失败，也不要慢慢死。
type Breaker struct {
	mu sync.Mutex

	window       time.Duration // 统计窗口
	minRequests  int           // 窗口内低于该请求数不做判定，避免小样本误判
	failureRatio float64       // 触发熔断的失败率阈值
	openFor      time.Duration // open 状态持续多久后进入 half-open
	halfOpenMax  int           // half-open 允许的探针并发数

	state       BreakerState
	winStart    time.Time
	successes   int
	failures    int
	openedAt    time.Time
	probes      int // half-open 中已放行的探针数
	probeOK     int
	transitions int
}

type BreakerConfig struct {
	Window       time.Duration
	MinRequests  int
	FailureRatio float64
	OpenFor      time.Duration
	HalfOpenMax  int
}

func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = 20
	}
	if cfg.FailureRatio <= 0 {
		cfg.FailureRatio = 0.5
	}
	if cfg.OpenFor <= 0 {
		cfg.OpenFor = 5 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 3
	}
	return &Breaker{
		window:       cfg.Window,
		minRequests:  cfg.MinRequests,
		failureRatio: cfg.FailureRatio,
		openFor:      cfg.OpenFor,
		halfOpenMax:  cfg.HalfOpenMax,
		state:        StateClosed,
		winStart:     time.Now(),
	}
}

// Allow 判断是否放行。返回的 error 包装了 trace.ErrRejected，
// 上层 span 会被标成 rejected 而不是 error —— 这次调用根本没打到下游。
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()

	switch b.state {
	case StateOpen:
		if time.Since(b.openedAt) >= b.openFor {
			b.toHalfOpenLocked()
			b.probes = 1
			return nil
		}
		return fmt.Errorf("%w: 熔断器打开，剩余 %.0fms", trace.ErrRejected,
			float64((b.openFor - time.Since(b.openedAt)).Milliseconds()))
	case StateHalfOpen:
		if b.probes >= b.halfOpenMax {
			return fmt.Errorf("%w: 熔断器半开，探针名额已用尽", trace.ErrRejected)
		}
		b.probes++
		return nil
	default:
		return nil
	}
}

// Report 上报一次调用结果，驱动状态机流转。
func (b *Breaker) Report(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()

	if b.state == StateHalfOpen {
		if success {
			b.probeOK++
			// 探针全部成功才认为下游恢复，回到 closed 并清空窗口
			if b.probeOK >= b.halfOpenMax {
				b.state = StateClosed
				b.transitions++
				b.resetWindowLocked()
			}
			return
		}
		// 半开期间只要失败一次，立刻退回 open，重新计时
		b.toOpenLocked()
		return
	}

	if success {
		b.successes++
	} else {
		b.failures++
	}
	total := b.successes + b.failures
	if total >= b.minRequests {
		if float64(b.failures)/float64(total) >= b.failureRatio {
			b.toOpenLocked()
		}
	}
}

func (b *Breaker) rollLocked() {
	if time.Since(b.winStart) >= b.window {
		b.resetWindowLocked()
	}
}

func (b *Breaker) resetWindowLocked() {
	b.winStart = time.Now()
	b.successes = 0
	b.failures = 0
}

func (b *Breaker) toOpenLocked() {
	b.state = StateOpen
	b.openedAt = time.Now()
	b.probes = 0
	b.probeOK = 0
	b.transitions++
	b.resetWindowLocked()
}

func (b *Breaker) toHalfOpenLocked() {
	b.state = StateHalfOpen
	b.probes = 0
	b.probeOK = 0
	b.transitions++
}

// State 返回当前状态。
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()
	if b.state == StateOpen && time.Since(b.openedAt) >= b.openFor {
		return StateHalfOpen
	}
	return b.state
}

// BreakerStats 是面板展示用的快照。
type BreakerStats struct {
	State        BreakerState `json:"state"`
	Successes    int          `json:"successes"`
	Failures     int          `json:"failures"`
	FailureRatio float64      `json:"failureRatio"`
	Threshold    float64      `json:"threshold"`
	Transitions  int          `json:"transitions"`
	CooldownMS   int64        `json:"cooldownMs"`
}

func (b *Breaker) Stats() BreakerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()
	total := b.successes + b.failures
	ratio := 0.0
	if total > 0 {
		ratio = float64(b.failures) / float64(total)
	}
	var cooldown int64
	if b.state == StateOpen {
		if left := b.openFor - time.Since(b.openedAt); left > 0 {
			cooldown = left.Milliseconds()
		}
	}
	return BreakerStats{
		State:        b.state,
		Successes:    b.successes,
		Failures:     b.failures,
		FailureRatio: ratio,
		Threshold:    b.failureRatio,
		Transitions:  b.transitions,
		CooldownMS:   cooldown,
	}
}

// Reset 手动复位，前端面板提供一个「恢复」按钮。
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateClosed
	b.probes = 0
	b.probeOK = 0
	b.resetWindowLocked()
}
