// Package llm 定义模型调用的统一抽象，并提供两种实现：
// mock（本地模拟，默认，零成本零网络）与 openai（任何 OpenAI 兼容端点）。
package llm

import "context"

// Message 是一条对话消息。
type Message struct {
	Role    string `json:"role"` // system | user | assistant
	Content string `json:"content"`
}

// Request 是一次模型调用请求。
type Request struct {
	Model       string
	Messages    []Message
	MaxTokens   int
	Temperature float64
}

// Chunk 是流式返回的一个增量片段。
type Chunk struct {
	Text string // 增量文本
	Done bool   // 流是否结束
	Err  error  // 流中途出错
}

// Provider 是模型提供方抽象。
//
// 返回 channel 而不是 callback，是为了让调用方能用 select 同时等待
// 「下一个 chunk」和「ctx.Done()」—— 这是流式调用可被取消的关键。
type Provider interface {
	Name() string
	Model() string
	Stream(ctx context.Context, req Request) (<-chan Chunk, error)
}

// EstimateTokens 粗略估算 token 数：中文按字，英文按 4 字符。
// 用于统计展示，不追求与官方分词器一致。
func EstimateTokens(s string) int {
	n := 0
	ascii := 0
	for _, r := range s {
		if r < 128 {
			ascii++
		} else {
			n++
		}
	}
	return n + ascii/4
}
