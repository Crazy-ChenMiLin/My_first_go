package resilience

import (
	"context"
	"sync"
)

// Group 是请求合并器（singleflight）。
//
// 场景：热点问题被 200 个用户同时问到，没有合并的话就是 200 次模型调用、200 份账单。
// 合并之后只有第一个请求真正执行，其余全部挂在同一个结果上等待。
//
// 与标准库 x/sync/singleflight 的差别：这里的等待方支持 context 取消，
// 某个用户断开连接不会拖住其他人，也不会让自己一直挂着。
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	done   chan struct{}
	val    any
	err    error
	shared int // 搭便车的等待者数量
}

func NewGroup() *Group { return &Group{m: make(map[string]*call)} }

// Result 描述一次 Do 的结果来源。
type Result struct {
	Val     any
	Err     error
	Shared  bool // true 表示复用了别人的执行结果
	Waiters int
}

// Do 对同一 key 的并发调用只执行一次 fn。
func (g *Group) Do(ctx context.Context, key string, fn func(ctx context.Context) (any, error)) Result {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		c.shared++
		waiters := c.shared
		g.mu.Unlock()
		select {
		case <-c.done:
			return Result{Val: c.val, Err: c.err, Shared: true, Waiters: waiters}
		case <-ctx.Done():
			// 自己不等了，但不影响正在执行的那个请求
			return Result{Err: ctx.Err(), Shared: true, Waiters: waiters}
		}
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn(ctx)

	g.mu.Lock()
	delete(g.m, key)
	waiters := c.shared
	g.mu.Unlock()
	close(c.done)

	return Result{Val: c.val, Err: c.err, Shared: false, Waiters: waiters}
}

// Inflight 返回当前正在执行的 key 数量。
func (g *Group) Inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.m)
}
