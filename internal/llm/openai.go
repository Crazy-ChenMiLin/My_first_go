package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"aictx/internal/resilience"
)

// OpenAIConfig 指向任意 OpenAI 兼容端点。
// 实测可直接用于 DeepSeek、通义、NVIDIA NIM、小米 MiMo、vLLM、Ollama 等。
type OpenAIConfig struct {
	BaseURL string // 例如 https://api.deepseek.com/v1
	APIKey  string
	Model   string
	Timeout time.Duration
}

// OpenAIProvider 是 OpenAI 兼容协议的流式实现。
type OpenAIProvider struct {
	cfg    OpenAIConfig
	client *http.Client
}

// NewOpenAI 构造 provider。
//
// http.Client 是复用的，这点非常关键：
// 每次请求新建 Client 会导致连接无法复用，高并发下瞬间打满本地端口（TIME_WAIT 堆积）。
// 同时必须调高 MaxIdleConnsPerHost —— 默认值只有 2，
// 在只打一个模型域名的网关场景下，它会成为最隐蔽的性能瓶颈。
func NewOpenAI(cfg OpenAIConfig) *OpenAIProvider {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 120 * time.Second
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   8 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   8 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &OpenAIProvider{
		cfg: cfg,
		// 注意这里不设 Client.Timeout：流式响应可能持续几分钟，
		// 整体时限交给 context 控制，粒度更细也更符合链路语义。
		client: &http.Client{Transport: tr},
	}
}

func (p *OpenAIProvider) Name() string  { return "openai-compatible" }
func (p *OpenAIProvider) Model() string { return p.cfg.Model }

type chatReq struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Temperature float64   `json:"temperature,omitempty"`
}

type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (p *OpenAIProvider) Stream(ctx context.Context, req Request) (<-chan Chunk, error) {
	model := req.Model
	if model == "" {
		model = p.cfg.Model
	}
	body, err := json.Marshal(chatReq{
		Model:       model,
		Messages:    req.Messages,
		Stream:      true,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	})
	if err != nil {
		return nil, err
	}

	url := strings.TrimRight(p.cfg.BaseURL, "/") + "/chat/completions"
	// NewRequestWithContext 是整条取消链路能打通到 TCP 层的前提：
	// ctx 一旦 Done，底层连接会被立刻关闭，不会有孤儿请求继续跑。
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if p.cfg.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		// 网络类错误标记为可重试
		return nil, resilience.MarkRetryable(fmt.Errorf("请求上游失败: %w", err))
	}
	if resp.StatusCode >= 400 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		e := fmt.Errorf("上游返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
		// 只有 429 与 5xx 值得重试；4xx 是自己的问题，重试没有意义
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return nil, resilience.MarkRetryable(e)
		}
		return nil, e
	}

	out := make(chan Chunk, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 单条 SSE 可能很长，放大缓冲

		for sc.Scan() {
			if ctx.Err() != nil {
				out <- Chunk{Err: ctx.Err()}
				return
			}
			line := strings.TrimSpace(sc.Text())
			if line == "" || !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				out <- Chunk{Done: true}
				return
			}
			var c chatChunk
			if err := json.Unmarshal([]byte(data), &c); err != nil {
				continue // 容忍个别畸形分片，不因此中断整条流
			}
			if c.Error != nil {
				out <- Chunk{Err: fmt.Errorf("上游错误: %s", c.Error.Message)}
				return
			}
			if len(c.Choices) == 0 {
				continue
			}
			d := c.Choices[0].Delta
			text := d.Content
			// 部分推理模型（如 NIM 上的 Nemotron / Qwen）会把内容放进 reasoning_content，
			// content 反而为空。这里做兼容，避免前端看到一片空白。
			if text == "" && d.ReasoningContent != "" {
				text = d.ReasoningContent
			}
			if text != "" {
				select {
				case out <- Chunk{Text: text}:
				case <-ctx.Done():
					out <- Chunk{Err: ctx.Err()}
					return
				}
			}
			if c.Choices[0].FinishReason != nil {
				out <- Chunk{Done: true}
				return
			}
		}
		if err := sc.Err(); err != nil {
			out <- Chunk{Err: resilience.MarkRetryable(err)}
			return
		}
		out <- Chunk{Done: true}
	}()

	return out, nil
}
