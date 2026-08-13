// Package config 负责启动配置（环境变量）与运行时可热调参数。
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是启动期配置，来自环境变量。
type Config struct {
	Port      string
	MaxTraces int

	// Provider 选择：mock（默认，本地模拟）或 openai（任意 OpenAI 兼容端点）
	Provider      string
	OpenAIBase    string
	OpenAIKey     string
	OpenAIModel   string
	OpenAITimeout time.Duration
}

func Load() Config {
	c := Config{
		Port:          env("PORT", "8080"),
		MaxTraces:     envInt("MAX_TRACES", 500),
		Provider:      strings.ToLower(env("LLM_PROVIDER", "mock")),
		OpenAIBase:    env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		OpenAIKey:     env("OPENAI_API_KEY", ""),
		OpenAIModel:   env("OPENAI_MODEL", "gpt-4o-mini"),
		OpenAITimeout: time.Duration(envInt("OPENAI_TIMEOUT_SEC", 120)) * time.Second,
	}
	// 只填了 key 没改 provider 时，自动切到真实模型，少一步配置
	if c.Provider == "mock" && c.OpenAIKey != "" {
		c.Provider = "openai"
	}
	return c
}

// Runtime 是运行时可热调的参数集合，前端调参面板直接改它。
// 用读写锁而不是 atomic.Value，是因为要支持「部分字段更新」。
type Runtime struct {
	RequestTimeoutMS   int     `json:"requestTimeoutMs"`   // 整条链路总预算
	QueueTimeoutMS     int     `json:"queueTimeoutMs"`     // 排队等待上限
	RetrievalTimeoutMS int     `json:"retrievalTimeoutMs"` // 检索扇出预算
	LLMTimeoutMS       int     `json:"llmTimeoutMs"`       // 单次模型调用预算
	RateLimit          float64 `json:"rateLimit"`          // 令牌桶速率 QPS
	Burst              float64 `json:"burst"`              // 突发容量
	MaxConcurrentLLM   int     `json:"maxConcurrentLlm"`   // 模型并发上限
	Workers            int     `json:"workers"`            // worker 数
	QueueSize          int     `json:"queueSize"`          // 队列容量（只读展示）
	RetryAttempts      int     `json:"retryAttempts"`      // 含首次的总尝试次数
	CacheEnabled       bool    `json:"cacheEnabled"`
	CacheTTLSec        int     `json:"cacheTtlSec"`
	SingleflightOn     bool    `json:"singleflightOn"`
	BreakerRatio       float64 `json:"breakerRatio"`  // 熔断失败率阈值
	BreakerMinReq      int     `json:"breakerMinReq"` // 熔断最小样本
	BreakerOpenMS      int     `json:"breakerOpenMs"` // 熔断打开持续时间
}

func DefaultRuntime() Runtime {
	return Runtime{
		RequestTimeoutMS:   30000,
		QueueTimeoutMS:     2000,
		RetrievalTimeoutMS: 900,
		LLMTimeoutMS:       25000,
		RateLimit:          50,
		Burst:              80,
		MaxConcurrentLLM:   16,
		Workers:            32,
		QueueSize:          512,
		RetryAttempts:      3,
		CacheEnabled:       true,
		CacheTTLSec:        60,
		SingleflightOn:     true,
		BreakerRatio:       0.5,
		BreakerMinReq:      20,
		BreakerOpenMS:      5000,
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
