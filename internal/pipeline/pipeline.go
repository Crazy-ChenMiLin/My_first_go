// Package pipeline 把一次 AI 请求的完整链路编排起来：
//
//	guard → 准入(限流/熔断) → 排队 → 缓存/请求合并 → 检索扇出 → 拼 prompt → 流式生成 → 后处理
//
// 每一步都开一个 span，每一步都吃同一条 context 的预算。
// 这一个文件就是整个项目的主干，其余包都是为它服务的零件。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"aictx/internal/config"
	"aictx/internal/llm"
	"aictx/internal/metrics"
	"aictx/internal/pool"
	"aictx/internal/resilience"
	"aictx/internal/trace"
)

// Input 是一次请求的输入。
type Input struct {
	Name    string        // 链路名，通常是 HTTP 路由
	Query   string        // 用户问题
	History []llm.Message // 历史消息
	Live    bool          // 是否逐 span 推送事件（压测流量关掉，避免事件风暴）
	NoCache bool          // 跳过缓存与请求合并
	OnDelta func(string)  // 增量 token 回调，nil 表示不需要流式
}

// Result 是一次请求的产出。
type Result struct {
	TraceID  string  `json:"traceId"`
	Text     string  `json:"text"`
	Tokens   int     `json:"tokens"`
	TTFTMS   float64 `json:"ttftMs"`
	TotalMS  float64 `json:"totalMs"`
	Cached   bool    `json:"cached"`
	Merged   bool    `json:"merged"`
	Attempts int     `json:"attempts"`
	Provider string  `json:"provider"`
	Model    string  `json:"model"`
}

// Pipeline 持有全部共享组件。它是并发安全的，全进程一个实例。
type Pipeline struct {
	tracer   *trace.Tracer
	metrics  *metrics.Metrics
	provider llm.Provider

	pool    *pool.Pool
	limiter *resilience.TokenBucket
	sem     *resilience.Semaphore
	breaker *resilience.Breaker
	sf      *resilience.Group
	cache   *ttlCache

	mu sync.RWMutex
	rt config.Runtime
}

func New(tracer *trace.Tracer, m *metrics.Metrics, provider llm.Provider, rt config.Runtime) *Pipeline {
	return &Pipeline{
		tracer:   tracer,
		metrics:  m,
		provider: provider,
		pool:     pool.New(rt.Workers, rt.QueueSize, msDur(rt.QueueTimeoutMS)),
		limiter:  resilience.NewTokenBucket(rt.RateLimit, rt.Burst),
		sem:      resilience.NewSemaphore(rt.MaxConcurrentLLM),
		breaker: resilience.NewBreaker(resilience.BreakerConfig{
			Window:       10 * time.Second,
			MinRequests:  rt.BreakerMinReq,
			FailureRatio: rt.BreakerRatio,
			OpenFor:      msDur(rt.BreakerOpenMS),
		}),
		sf:    resilience.NewGroup(),
		cache: newCache(2000),
		rt:    rt,
	}
}

func (p *Pipeline) Runtime() config.Runtime {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.rt
}

// SetRuntime 热更新参数并同步到各组件，无需重启。
func (p *Pipeline) SetRuntime(rt config.Runtime) {
	p.mu.Lock()
	rt.QueueSize = p.rt.QueueSize // 队列容量在池创建时固定，不可热改
	p.rt = rt
	p.mu.Unlock()

	p.limiter.SetLimit(rt.RateLimit, rt.Burst)
	p.sem.SetCapacity(rt.MaxConcurrentLLM)
	p.pool.SetWorkers(rt.Workers)
	p.pool.SetQueueTimeout(msDur(rt.QueueTimeoutMS))
}

func (p *Pipeline) Tracer() *trace.Tracer            { return p.tracer }
func (p *Pipeline) Metrics() *metrics.Metrics        { return p.metrics }
func (p *Pipeline) Pool() *pool.Pool                 { return p.pool }
func (p *Pipeline) Breaker() *resilience.Breaker     { return p.breaker }
func (p *Pipeline) Semaphore() *resilience.Semaphore { return p.sem }
func (p *Pipeline) Limiter() *resilience.TokenBucket { return p.limiter }
func (p *Pipeline) Provider() llm.Provider           { return p.provider }
func (p *Pipeline) CacheSize() int                   { return p.cache.Len() }
func (p *Pipeline) ClearCache()                      { p.cache.Clear() }
func (p *Pipeline) Close()                           { p.pool.Close() }

// Execute 跑完整条链路。
//
// 注意 context 的三层收紧：
//
//	parent(客户端连接) → 总预算 RequestTimeout → 排队 QueueTimeout → 单次生成 LLMTimeout
//
// 任何一层先到期，下面所有层立刻收到 Done，整条链路一起收手。
func (p *Pipeline) Execute(parent context.Context, in Input) (Result, error) {
	rt := p.Runtime()
	start := time.Now()

	// 第一层：给整条链路一个总预算
	ctx, cancel := context.WithTimeout(parent, msDur(rt.RequestTimeoutMS))
	defer cancel()

	name := in.Name
	if name == "" {
		name = "chat"
	}
	ctx, root := p.tracer.StartTrace(ctx, name, in.Live,
		trace.A("query", truncate(in.Query, 80)),
		trace.A("provider", p.provider.Name()),
		trace.A("model", p.provider.Model()),
	)

	res := Result{
		TraceID:  root.TraceID(),
		Provider: p.provider.Name(),
		Model:    p.provider.Model(),
	}

	p.metrics.IncInflight()
	err := p.run(ctx, in, &res, rt)
	p.metrics.DecInflight()

	res.TotalMS = float64(time.Since(start).Microseconds()) / 1000
	root.SetAttr("tokens", res.Tokens)
	root.SetAttr("cached", res.Cached)
	root.SetAttr("merged", res.Merged)
	if res.TTFTMS > 0 {
		root.SetAttr("ttftMs", round1(res.TTFTMS))
	}
	root.End(err)

	if tr := trace.TraceOf(ctx); tr != nil {
		tr.AddTokens(res.Tokens)
	}
	p.metrics.Observe(Outcome(err), res.TotalMS)
	p.metrics.AddTokens(res.Tokens)
	return res, err
}

func (p *Pipeline) run(ctx context.Context, in Input, res *Result, rt config.Runtime) error {
	if err := p.guard(ctx, in.Query); err != nil {
		return err
	}
	if err := p.admit(ctx); err != nil {
		return err
	}

	// 第二层：排队。queue span 与后续 span 都挂在 root 下，
	// 所以这里用 _ 丢弃派生 context，避免瀑布图出现虚假的父子嵌套。
	_, qspan := trace.Start(ctx, "queue.wait", trace.KindQueue)
	err := p.pool.Run(ctx, func(rctx context.Context) error {
		qspan.End(nil) // 拿到 worker，排队阶段到此为止
		return p.core(rctx, in, res, rt)
	})
	qspan.End(err) // 若压根没排上队，这次 End 才会生效
	return err
}

// guard 是入口校验。真实系统里这里会接内容安全服务，
// 放在最前面是因为它最便宜 —— 该拒的请求不要浪费后面任何一份资源。
func (p *Pipeline) guard(ctx context.Context, query string) error {
	_, span := trace.Start(ctx, "guard.validate", trace.KindGuard)
	var err error
	switch {
	case strings.TrimSpace(query) == "":
		err = fmt.Errorf("%w: 问题不能为空", trace.ErrRejected)
	case len([]rune(query)) > 2000:
		err = fmt.Errorf("%w: 问题超过 2000 字", trace.ErrRejected)
	}
	span.SetAttr("chars", len([]rune(query)))
	span.End(err)
	return err
}

// admit 是准入控制：限流 + 熔断。
// 两者都是「快速失败」语义 —— 与其让请求排队等死，不如立刻告诉调用方现在不行。
func (p *Pipeline) admit(ctx context.Context) error {
	_, span := trace.Start(ctx, "admit.control", trace.KindLimiter)

	if !p.limiter.Allow() {
		err := fmt.Errorf("%w: 触发限流，当前令牌不足", trace.ErrRejected)
		span.SetAttr("reason", "rate_limit")
		span.End(err)
		return err
	}
	if err := p.breaker.Allow(); err != nil {
		span.SetAttr("reason", "circuit_breaker")
		span.SetAttr("breaker", string(p.breaker.State()))
		span.End(err)
		return err
	}
	span.SetAttr("tokensLeft", round1(p.limiter.Tokens()))
	span.SetAttr("breaker", string(p.breaker.State()))
	span.End(nil)
	return nil
}

// core 在 worker 内执行，包含缓存、请求合并与真正的生成。
func (p *Pipeline) core(ctx context.Context, in Input, res *Result, rt config.Runtime) error {
	key := cacheKey(in.Query, p.provider.Model())
	useCache := rt.CacheEnabled && !in.NoCache

	if useCache {
		_, cs := trace.Start(ctx, "cache.lookup", trace.KindCache)
		text, hit := p.cache.Get(key)
		cs.SetAttr("hit", hit)
		cs.End(nil)
		if hit {
			p.metrics.IncCacheHit()
			res.Cached = true
			res.Text = text
			res.Tokens = llm.EstimateTokens(text)
			if in.OnDelta != nil {
				in.OnDelta(text) // 缓存命中没有增量过程，一次性给出
			}
			return nil
		}
	}

	gen := func(c context.Context) (any, error) {
		return p.generate(c, in, res, rt)
	}

	if rt.SingleflightOn && !in.NoCache {
		_, ss := trace.Start(ctx, "singleflight.join", trace.KindCache)
		r := p.sf.Do(ctx, key, gen)
		ss.SetAttr("shared", r.Shared)
		ss.SetAttr("waiters", r.Waiters)
		ss.End(r.Err)

		if r.Err != nil {
			return r.Err
		}
		text, _ := r.Val.(string)
		if r.Shared {
			// 搭了别人的便车：没有增量过程，拿到完整结果后一次性回放
			p.metrics.IncSingleflightHit()
			res.Merged = true
			res.Text = text
			res.Tokens = llm.EstimateTokens(text)
			if in.OnDelta != nil {
				in.OnDelta(text)
			}
		}
		if useCache && text != "" {
			p.cache.Set(key, text, time.Duration(rt.CacheTTLSec)*time.Second)
		}
		return nil
	}

	v, err := gen(ctx)
	if err != nil {
		return err
	}
	if text, ok := v.(string); ok && useCache && text != "" {
		p.cache.Set(key, text, time.Duration(rt.CacheTTLSec)*time.Second)
	}
	return nil
}

// generate 是「检索 → 拼 prompt → 生成 → 后处理」四步。
func (p *Pipeline) generate(ctx context.Context, in Input, res *Result, rt config.Runtime) (string, error) {
	docs, err := p.retrieve(ctx, in.Query, msDur(rt.RetrievalTimeoutMS))
	if err != nil {
		return "", err
	}

	msgs := p.buildPrompt(ctx, in, docs)

	text, err := p.callLLM(ctx, msgs, in, res, rt)
	if err != nil {
		return "", err
	}

	_, ps := trace.Start(ctx, "post.process", trace.KindPost)
	text = strings.TrimSpace(text)
	ps.SetAttr("chars", len([]rune(text)))
	ps.End(nil)

	res.Text = text
	res.Tokens = llm.EstimateTokens(text)
	return text, nil
}

func (p *Pipeline) buildPrompt(ctx context.Context, in Input, docs []Doc) []llm.Message {
	_, span := trace.Start(ctx, "prompt.build", trace.KindPrompt)
	defer span.End(nil)

	var sb strings.Builder
	sb.WriteString("你是一个后端架构助手，回答要具体、克制、不说套话。\n")
	if len(docs) > 0 {
		sb.WriteString("\n可参考的检索片段：\n")
		for i, d := range docs {
			fmt.Fprintf(&sb, "%d. [%s] %s\n", i+1, d.Source, d.Snippet)
		}
	}

	msgs := make([]llm.Message, 0, len(in.History)+2)
	msgs = append(msgs, llm.Message{Role: "system", Content: sb.String()})
	msgs = append(msgs, in.History...)
	msgs = append(msgs, llm.Message{Role: "user", Content: in.Query})

	span.SetAttr("docs", len(docs))
	span.SetAttr("promptChars", len([]rune(sb.String())))
	span.SetAttr("messages", len(msgs))
	return msgs
}

// callLLM 是最需要小心的一段：信号量、熔断上报、重试、流式取消都在这里。
func (p *Pipeline) callLLM(ctx context.Context, msgs []llm.Message, in Input, res *Result, rt config.Runtime) (string, error) {
	ctx, span := trace.Start(ctx, "llm.generate", trace.KindLLM,
		trace.A("model", p.provider.Model()),
		trace.A("provider", p.provider.Name()))

	// 并发闸门：控制同时打到模型的请求数
	_, semSpan := trace.Start(ctx, "llm.semaphore", trace.KindLimiter)
	if err := p.sem.Acquire(ctx); err != nil {
		inflight, capacity, queued := p.sem.Stats()
		semSpan.SetAttr("inflight", inflight)
		semSpan.SetAttr("capacity", capacity)
		semSpan.SetAttr("queued", queued)
		semSpan.End(err)
		span.End(err)
		return "", err
	}
	inflight, capacity, queued := p.sem.Stats()
	semSpan.SetAttr("inflight", inflight)
	semSpan.SetAttr("capacity", capacity)
	semSpan.SetAttr("queued", queued)
	semSpan.End(nil)
	defer p.sem.Release()

	var full strings.Builder
	var ttft float64
	callStart := time.Now()

	retryCfg := resilience.DefaultRetry()
	retryCfg.MaxAttempts = max(rt.RetryAttempts, 1)

	err := resilience.Do(ctx, retryCfg, func(c context.Context, attempt int) error {
		res.Attempts = attempt
		if attempt > 1 {
			p.metrics.IncRetry()
		}

		// 第三层：单次生成的预算。重试的每一次都重新计时，
		// 但父 ctx 的总预算不会因此变多 —— 这就是「预算只能越切越小」。
		actx, cancel := context.WithTimeout(c, msDur(rt.LLMTimeoutMS))
		defer cancel()

		actx, aspan := trace.Start(actx, fmt.Sprintf("llm.attempt.%d", attempt), trace.KindLLM,
			trace.A("attempt", attempt))

		ch, err := p.provider.Stream(actx, llm.Request{
			Messages:    msgs,
			MaxTokens:   1024,
			Temperature: 0.7,
		})
		if err != nil {
			aspan.End(err)
			return err
		}

		var got int
		var local strings.Builder
		for {
			select {
			case <-actx.Done():
				aspan.SetAttr("tokens", got)
				aspan.End(actx.Err())
				return actx.Err()

			case chunk, ok := <-ch:
				if !ok {
					aspan.SetAttr("tokens", got)
					aspan.End(nil)
					full.WriteString(local.String())
					return nil
				}
				if chunk.Err != nil {
					aspan.SetAttr("tokens", got)
					aspan.End(chunk.Err)
					// 已经吐出内容就不能重试了，否则用户会看到重复的半截回答。
					// 这是流式接口最容易踩的坑：重试必须在「首 token 之前」。
					if got > 0 {
						return errors.New(unwrapMsg(chunk.Err))
					}
					return chunk.Err
				}
				if chunk.Done {
					aspan.SetAttr("tokens", got)
					aspan.End(nil)
					full.WriteString(local.String())
					return nil
				}
				if got == 0 {
					ttft = float64(time.Since(callStart).Microseconds()) / 1000
					span.Mark("first_token")
					span.SetAttr("ttftMs", round1(ttft))
					p.metrics.ObserveTTFT(ttft)
				}
				got++
				local.WriteString(chunk.Text)
				if in.OnDelta != nil {
					in.OnDelta(chunk.Text)
				}
			}
		}
	})

	// 熔断只统计「真正打到下游」的结果。
	// 客户端主动取消不是下游的锅，算进去会误伤，让熔断器无端跳闸。
	if !errors.Is(err, context.Canceled) {
		p.breaker.Report(err == nil)
	}

	res.TTFTMS = ttft
	span.SetAttr("attempts", res.Attempts)
	span.SetAttr("chars", len([]rune(full.String())))
	span.End(err)
	if err != nil {
		return "", err
	}
	return full.String(), nil
}

// Outcome 把错误映射成指标分类。
// 把 rejected / canceled / timeout 从 failed 里拆出来，是排障效率的关键：
// 「拒绝」说明保护机制在起作用，「失败」才是真的出事了。
func Outcome(err error) metrics.Outcome {
	switch {
	case err == nil:
		return metrics.OutcomeOK
	case errors.Is(err, trace.ErrRejected):
		return metrics.OutcomeRejected
	case errors.Is(err, context.Canceled):
		return metrics.OutcomeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return metrics.OutcomeTimeout
	default:
		return metrics.OutcomeFailed
	}
}

func msDur(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }

func round1(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func unwrapMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
