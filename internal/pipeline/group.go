package pipeline

import (
	"context"
	"sync"
)

// group 是 errgroup 的极简实现（零依赖版）。
//
// 与 golang.org/x/sync/errgroup 的关键差异：这里用 context.WithCancelCause，
// 取消时能把「究竟是谁导致的取消」带下去。
// 排障时 "context canceled" 和 "context canceled: 向量检索超时" 的价值天差地别。
type group struct {
	wg     sync.WaitGroup
	once   sync.Once
	err    error
	cancel context.CancelCauseFunc
}

// withGroup 返回 group 与派生 context。任一子任务失败即取消该 context。
func withGroup(parent context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancelCause(parent)
	return &group{cancel: cancel}, ctx
}

// Go 启动一个子任务。返回 error 会触发整组取消（fail-fast）。
func (g *group) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := f(); err != nil {
			g.once.Do(func() {
				g.err = err
				g.cancel(err)
			})
		}
	}()
}

// GoOptional 启动一个「可失败」的子任务：它出错不会拖垮整组，只做降级。
// 检索里的联网搜索就属于这类 —— 挂了就少一路召回，不该让整个回答失败。
func (g *group) GoOptional(f func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		_ = f()
	}()
}

// Wait 等待全部子任务结束，返回第一个致命错误。
func (g *group) Wait() error {
	g.wg.Wait()
	g.cancel(nil)
	return g.err
}
