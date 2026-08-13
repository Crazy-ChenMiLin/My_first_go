package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"aictx/internal/resilience"
	"aictx/internal/trace"
)

// Doc 是一条召回结果。
type Doc struct {
	Source  string  `json:"source"`
	Title   string  `json:"title"`
	Score   float64 `json:"score"`
	Snippet string  `json:"snippet"`
}

var corpus = []string{
	"context.Context 的三大能力：传递取消信号、传递截止时间、传递请求域数据。前两者是高并发服务的命脉。",
	"WithTimeout 派生出的子 context 永远不会比父 context 活得久，这保证了超时预算只会逐层收紧。",
	"HTTP 服务中 r.Context() 会在客户端断开时自动取消，把它一路传下去，就能让下游立即停止无用功。",
	"令牌桶限制速率，信号量限制并发，队列吸收突发，熔断切断雪崩，四者组合才是完整的过载保护。",
	"流式响应下重试要格外小心：一旦已经吐出 token 就不能重来，否则用户会看到重复内容。",
	"扇出检索用 errgroup：任一必需分支失败立即取消其余分支，省下的配额和延迟都是实打实的。",
}

// retrieve 执行检索扇出。
//
// 这里刻意区分了两类分支：
//   - 必需分支（向量库）：失败即整体失败，并通过 group 取消其余分支
//   - 可选分支（联网、长期记忆）：失败只降级，不影响主流程
//
// 这个区分是 RAG 服务能不能扛住抖动的分水岭。
func (p *Pipeline) retrieve(ctx context.Context, query string, budget time.Duration) ([]Doc, error) {
	ctx, span := trace.Start(ctx, "retrieve.fanout", trace.KindRetrieval,
		trace.A("budgetMs", budget.Milliseconds()))

	// 给检索单独切一份预算：检索再慢也不能吃掉留给生成的时间
	rctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	g, gctx := withGroup(rctx)
	var mu sync.Mutex
	var docs []Doc

	collect := func(d []Doc) {
		mu.Lock()
		docs = append(docs, d...)
		mu.Unlock()
	}

	// 必需分支
	g.Go(func() error {
		return searchSpan(gctx, "retrieve.vector", "向量库", 40, 180, 0.04, collect)
	})
	// 可选分支：挂了就少一路召回
	g.GoOptional(func() error {
		return searchSpan(gctx, "retrieve.keyword", "关键词", 20, 90, 0.06, collect)
	})
	g.GoOptional(func() error {
		return searchSpan(gctx, "retrieve.memory", "长期记忆", 60, 260, 0.10, collect)
	})

	err := g.Wait()
	if err == nil && rctx.Err() != nil {
		err = rctx.Err()
	}
	span.SetAttr("docs", len(docs))
	span.SetAttr("sources", 3)
	span.End(err)
	if err != nil {
		return nil, err
	}
	return docs, nil
}

func searchSpan(ctx context.Context, name, source string, minMS, maxMS int, failRate float64, collect func([]Doc)) error {
	_, span := trace.Start(ctx, name, trace.KindRetrieval, trace.A("source", source))
	docs, err := fakeSearch(ctx, source, minMS, maxMS, failRate)
	span.SetAttr("hits", len(docs))
	span.End(err)
	if err != nil {
		return err
	}
	collect(docs)
	return nil
}

// fakeSearch 模拟一次带延迟与失败率的检索。
// 注意它用 select 等待而不是 time.Sleep：ctx 取消时必须立刻返回。
func fakeSearch(ctx context.Context, source string, minMS, maxMS int, failRate float64) ([]Doc, error) {
	d := time.Duration(minMS+rand.Intn(max(maxMS-minMS, 1))) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
	}
	if rand.Float64() < failRate {
		return nil, resilience.MarkRetryable(fmt.Errorf("%s 检索失败", source))
	}
	n := 1 + rand.Intn(3)
	out := make([]Doc, 0, n)
	for i := range n {
		out = append(out, Doc{
			Source:  source,
			Title:   fmt.Sprintf("%s#%d", source, i+1),
			Score:   0.6 + rand.Float64()*0.4,
			Snippet: corpus[rand.Intn(len(corpus))],
		})
	}
	return out, nil
}

var errNoDocs = errors.New("全部检索源均不可用")
