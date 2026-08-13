// Command server 启动整个 AI 高并发链路服务。
//
//	go run ./cmd/server                     # mock provider，零外部依赖
//	OPENAI_API_KEY=sk-xxx go run ./cmd/server   # 自动切到真实模型
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"aictx/internal/config"
	"aictx/internal/llm"
	"aictx/internal/metrics"
	"aictx/internal/pipeline"
	"aictx/internal/server"
	"aictx/internal/trace"
	"aictx/web"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := config.Load()
	rt := config.DefaultRuntime()

	provider, mock := buildProvider(cfg, log)

	tracer := trace.NewTracer(cfg.MaxTraces)
	m := metrics.New()
	pipe := pipeline.New(tracer, m, provider, rt)
	defer pipe.Close()

	srv := server.New(pipe, mock, web.Assets(), log)

	httpSrv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: srv.Handler(),
		// 关键：ReadHeaderTimeout 防慢速攻击，但 WriteTimeout 必须为 0。
		// SSE 是长连接，一旦设了写超时，流到点就会被服务器无情掐断。
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          nil,
	}

	go func() {
		log.Info("服务启动",
			"addr", "http://localhost:"+cfg.Port,
			"provider", provider.Name(),
			"model", provider.Model(),
			"workers", rt.Workers,
			"rateLimit", rt.RateLimit,
		)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("监听失败", "err", err)
			os.Exit(1)
		}
	}()

	// 优雅退出：先停止接收新连接，给在途请求 10 秒收尾。
	// 对流式服务来说这一步很重要 —— 直接 kill 会让所有正在生成的回答戛然而止。
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Info("收到退出信号，正在收尾…")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Error("优雅退出超时", "err", err)
	}
	log.Info("已退出")
}

func buildProvider(cfg config.Config, log *slog.Logger) (llm.Provider, *llm.MockProvider) {
	if cfg.Provider == "openai" && cfg.OpenAIKey != "" {
		log.Info("使用 OpenAI 兼容端点", "base", cfg.OpenAIBase, "model", cfg.OpenAIModel)
		return llm.NewOpenAI(llm.OpenAIConfig{
			BaseURL: cfg.OpenAIBase,
			APIKey:  cfg.OpenAIKey,
			Model:   cfg.OpenAIModel,
			Timeout: cfg.OpenAITimeout,
		}), nil
	}
	if cfg.Provider == "openai" {
		log.Warn("LLM_PROVIDER=openai 但未提供 OPENAI_API_KEY，回退到 mock")
	}
	m := llm.NewMock(llm.DefaultMockConfig())
	return m, m
}
