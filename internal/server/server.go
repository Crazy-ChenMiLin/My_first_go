// Package server 提供 HTTP 接口层：REST + SSE 流式 + 内嵌前端静态资源。
package server

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"aictx/internal/bench"
	"aictx/internal/config"
	"aictx/internal/llm"
	"aictx/internal/metrics"
	"aictx/internal/pipeline"
	"aictx/internal/pool"
	"aictx/internal/resilience"
	"aictx/internal/trace"
)

// Server 组装 pipeline、压测器与静态资源。
type Server struct {
	pipe   *pipeline.Pipeline
	tracer *trace.Tracer
	runner *bench.Runner
	mock   *llm.MockProvider // 非 mock provider 时为 nil
	static fs.FS
	log    *slog.Logger
	start  time.Time
}

func New(p *pipeline.Pipeline, mock *llm.MockProvider, static fs.FS, log *slog.Logger) *Server {
	s := &Server{
		pipe:   p,
		tracer: p.Tracer(),
		mock:   mock,
		static: static,
		log:    log,
		start:  time.Now(),
	}
	// 压测流量走同一条 pipeline，但关掉 live 事件推送：
	// 几千 QPS 下逐 span 广播会把 SSE 通道和浏览器一起冲垮。
	s.runner = bench.NewRunner(func(ctx context.Context, query string, noCache bool) (bench.Outcome, float64) {
		res, err := p.Execute(ctx, pipeline.Input{
			Name:    "bench",
			Query:   query,
			Live:    false,
			NoCache: noCache,
		})
		return toBenchOutcome(err), res.TotalMS
	})
	return s
}

func (s *Server) Runner() *bench.Runner { return s.runner }

func toBenchOutcome(err error) bench.Outcome {
	switch pipeline.Outcome(err) {
	case metrics.OutcomeOK:
		return bench.OK
	case metrics.OutcomeRejected:
		return bench.Rejected
	case metrics.OutcomeTimeout:
		return bench.Timeout
	case metrics.OutcomeCanceled:
		return bench.Canceled
	default:
		return bench.Failed
	}
}

// Handler 构建路由。用 Go 1.22 起支持的方法+路径模式，省掉一个路由库依赖。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/chat", s.handleChat)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /api/traces", s.handleTraces)
	mux.HandleFunc("GET /api/traces/{id}", s.handleTraceDetail)
	mux.HandleFunc("GET /api/config", s.handleGetConfig)
	mux.HandleFunc("PUT /api/config", s.handlePutConfig)
	mux.HandleFunc("POST /api/bench", s.handleBenchStart)
	mux.HandleFunc("DELETE /api/bench", s.handleBenchStop)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "uptimeSec": time.Since(s.start).Seconds()})
	})

	mux.Handle("/", s.staticHandler())

	return withRecover(withCORS(mux), s.log)
}

// staticHandler 服务内嵌的前端产物，并做 SPA 回退。
func (s *Server) staticHandler() http.Handler {
	if s.static == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeErr(w, http.StatusNotFound, "前端未构建：进入 web/ 执行 npm install && npm run build")
		})
	}
	files := http.FileServer(http.FS(s.static))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p == "" || p == "." {
			p = "index.html"
		}
		if _, err := fs.Stat(s.static, p); err != nil {
			// 前端是单页应用，未知路径一律交还 index.html 由前端路由处理。
			// 但 /api 前缀不能回退，否则拼错的接口会返回一坨 HTML，排查起来很痛苦。
			if strings.HasPrefix(r.URL.Path, "/api/") {
				writeErr(w, http.StatusNotFound, "接口不存在")
				return
			}
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		if strings.HasPrefix(p, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withRecover 兜底 panic。一个请求写崩了不该带走整个进程，
// 尤其是这个服务本来就在故意制造超时和取消。
func withRecover(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			// ErrAbortHandler 是 net/http 约定的「静默中断」信号，必须原样抛回去，
			// 否则会污染日志，还会让 http.Server 无法正确收尾连接。
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v)
			}
			log.Error("handler panic", "path", r.URL.Path, "err", v)
		}()
		next.ServeHTTP(w, r)
	})
}

// Snapshot 是给前端仪表盘的一次性全景快照。
type Snapshot struct {
	TS        int64                   `json:"ts"`
	UptimeSec float64                 `json:"uptimeSec"`
	Metrics   metrics.Snapshot        `json:"metrics"`
	Pool      pool.Stats              `json:"pool"`
	Breaker   resilience.BreakerStats `json:"breaker"`
	Sem       SemStats                `json:"sem"`
	Limiter   LimiterStats            `json:"limiter"`
	Bench     bench.Status            `json:"bench"`
	Runtime   config.Runtime          `json:"runtime"`
	Provider  ProviderInfo            `json:"provider"`
	Cache     int                     `json:"cacheSize"`
	Traces    int                     `json:"traceCount"`
	Watchers  int                     `json:"watchers"`
}

type SemStats struct {
	Inflight int `json:"inflight"`
	Capacity int `json:"capacity"`
	Queued   int `json:"queued"`
}

type LimiterStats struct {
	Tokens float64 `json:"tokens"`
	Rate   float64 `json:"rate"`
	Burst  float64 `json:"burst"`
}

type ProviderInfo struct {
	Name  string `json:"name"`
	Model string `json:"model"`
	Mock  bool   `json:"mock"`
}

func (s *Server) snapshot(seriesLen int) Snapshot {
	rt := s.pipe.Runtime()
	inflight, capacity, queued := s.pipe.Semaphore().Stats()
	return Snapshot{
		TS:        time.Now().UnixMilli(),
		UptimeSec: time.Since(s.start).Seconds(),
		Metrics:   s.pipe.Metrics().Snapshot(seriesLen),
		Pool:      s.pipe.Pool().Stats(),
		Breaker:   s.pipe.Breaker().Stats(),
		Sem:       SemStats{Inflight: inflight, Capacity: capacity, Queued: queued},
		Limiter:   LimiterStats{Tokens: s.pipe.Limiter().Tokens(), Rate: rt.RateLimit, Burst: rt.Burst},
		Bench:     s.runner.Status(),
		Runtime:   rt,
		Provider: ProviderInfo{
			Name:  s.pipe.Provider().Name(),
			Model: s.pipe.Provider().Model(),
			Mock:  s.mock != nil,
		},
		Cache:    s.pipe.CacheSize(),
		Traces:   s.tracer.Recorder().Len(),
		Watchers: s.tracer.Hub().Subscribers(),
	}
}
