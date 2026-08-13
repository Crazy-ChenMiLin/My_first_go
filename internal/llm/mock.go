package llm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// MockConfig 是模拟模型的行为参数，可在运行时热更新。
// 压测和演示故障时，就靠调这几个数把各种糟糕情况复现出来。
type MockConfig struct {
	TTFTMinMS  int     `json:"ttftMinMs"`  // 首 token 最短耗时
	TTFTMaxMS  int     `json:"ttftMaxMs"`  // 首 token 最长耗时
	TokenMinMS int     `json:"tokenMinMs"` // token 间隔下限
	TokenMaxMS int     `json:"tokenMaxMs"` // token 间隔上限
	ErrorRate  float64 `json:"errorRate"`  // 调用直接报错的概率
	StallRate  float64 `json:"stallRate"`  // 生成中途卡死的概率（用来验证超时与取消）
	StallMS    int     `json:"stallMs"`    // 卡死时长
	MaxTokens  int     `json:"maxTokens"`  // 最多生成多少段
	ModelName  string  `json:"modelName"`
}

func DefaultMockConfig() MockConfig {
	return MockConfig{
		TTFTMinMS: 120, TTFTMaxMS: 420,
		TokenMinMS: 12, TokenMaxMS: 45,
		ErrorRate: 0.03, StallRate: 0.02, StallMS: 8000,
		MaxTokens: 220, ModelName: "mock-reasoner-v1",
	}
}

// MockProvider 在本地模拟一个流式模型。
type MockProvider struct {
	mu  sync.RWMutex
	cfg MockConfig
}

func NewMock(cfg MockConfig) *MockProvider { return &MockProvider{cfg: cfg} }

func (m *MockProvider) Name() string { return "mock" }

func (m *MockProvider) Model() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.ModelName
}

func (m *MockProvider) Config() MockConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

func (m *MockProvider) SetConfig(cfg MockConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

// ErrUpstream 模拟下游模型服务异常，属于可重试错误。
var ErrUpstream = errors.New("上游模型服务异常")

func (m *MockProvider) Stream(ctx context.Context, req Request) (<-chan Chunk, error) {
	cfg := m.Config()
	out := make(chan Chunk, 32)

	go func() {
		defer close(out)

		// 阶段一：首 token 延迟。用 select 而不是 time.Sleep，
		// 否则 ctx 取消后这个 goroutine 还要傻等，白白占着资源。
		ttft := randRange(cfg.TTFTMinMS, cfg.TTFTMaxMS)
		if !sleepCtx(ctx, time.Duration(ttft)*time.Millisecond) {
			out <- Chunk{Err: ctx.Err()}
			return
		}
		if rand.Float64() < cfg.ErrorRate {
			out <- Chunk{Err: fmt.Errorf("%w: 503 upstream overloaded", ErrUpstream)}
			return
		}

		text := composeAnswer(req)
		segs := segment(text)
		if cfg.MaxTokens > 0 && len(segs) > cfg.MaxTokens {
			segs = segs[:cfg.MaxTokens]
		}
		stallAt := -1
		if rand.Float64() < cfg.StallRate && len(segs) > 3 {
			stallAt = rand.Intn(len(segs)-2) + 1
		}

		for i, s := range segs {
			if i == stallAt {
				// 生成中途卡死：真实世界里这种「半截不动」比直接报错更难缠，
				// 只有靠 context 超时才能兜住。
				if !sleepCtx(ctx, time.Duration(cfg.StallMS)*time.Millisecond) {
					out <- Chunk{Err: ctx.Err()}
					return
				}
			}
			gap := randRange(cfg.TokenMinMS, cfg.TokenMaxMS)
			if !sleepCtx(ctx, time.Duration(gap)*time.Millisecond) {
				out <- Chunk{Err: ctx.Err()}
				return
			}
			select {
			case out <- Chunk{Text: s}:
			case <-ctx.Done():
				out <- Chunk{Err: ctx.Err()}
				return
			}
		}
		out <- Chunk{Done: true}
	}()

	return out, nil
}

// sleepCtx 返回 false 表示被 ctx 打断。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func randRange(lo, hi int) int {
	if hi <= lo {
		return max(lo, 0)
	}
	return lo + rand.Intn(hi-lo)
}

// segment 把文本切成「像 token 一样」的小片段：中文 1-2 字，英文按词。
func segment(s string) []string {
	var out []string
	runes := []rune(s)
	for i := 0; i < len(runes); {
		step := 1
		if runes[i] < 128 {
			// 英文/符号：吃到下一个空格
			j := i
			for j < len(runes) && runes[j] != ' ' {
				j++
			}
			if j < len(runes) {
				j++
			}
			step = max(j-i, 1)
		} else if i+1 < len(runes) && runes[i+1] >= 128 && rand.Intn(2) == 0 {
			step = 2
		}
		out = append(out, string(runes[i:min(i+step, len(runes))]))
		i += step
	}
	return out
}

var openers = []string{
	"先给结论：",
	"简单说，",
	"这个问题的关键在于：",
	"拆开看有三层：",
}

var bodies = []string{
	"在高并发场景下，context 不只是用来传值的，它真正的价值是把「取消」这件事沿调用链一路传下去。上游一断，下游立刻收手，资源不会被拖住。",
	"超时预算应该逐层递减：入口给 30 秒，排队最多花 2 秒，检索留 800 毫秒，剩下的全部交给模型生成。每一层都只能花掉自己那一份。",
	"限流控制的是速率，信号量控制的是并发度，两者解决的问题完全不同，缺一不可。熔断则是最后一道闸：下游已经不行了，就别再往里灌。",
	"流式返回的体验核心是首 token 延迟。哪怕总耗时更长，只要 200 毫秒内出第一个字，用户就觉得快。",
	"扇出检索要用 errgroup 这种模式：任一分支失败就取消其余分支，避免为一个已经注定失败的请求继续烧配额。",
}

var closers = []string{
	"落到代码上，就是每个函数第一个参数都老老实实收 ctx，并且真的去 select 它。",
	"这套东西上线后最直观的变化是：下游抖动时，服务的错误率会上升，但延迟不会失控。",
	"可以在右侧的链路瀑布图里点开对应的 span，看看每一段实际花了多少时间。",
}

func composeAnswer(req Request) string {
	q := ""
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			q = req.Messages[i].Content
			break
		}
	}
	q = strings.TrimSpace(q)
	if len([]rune(q)) > 40 {
		q = string([]rune(q)[:40]) + "…"
	}

	var b strings.Builder
	b.WriteString(openers[rand.Intn(len(openers))])
	if q != "" {
		b.WriteString("关于「" + q + "」，")
	}
	b.WriteString(bodies[rand.Intn(len(bodies))])
	b.WriteString("\n\n")
	b.WriteString(bodies[rand.Intn(len(bodies))])
	b.WriteString("\n\n")
	b.WriteString(closers[rand.Intn(len(closers))])
	return b.String()
}
