package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"aictx/internal/bench"
	"aictx/internal/config"
	"aictx/internal/llm"
	"aictx/internal/pipeline"
)

// ---------- 聊天：SSE 流式 ----------

type chatReq struct {
	Query   string        `json:"query"`
	History []llm.Message `json:"history"`
	NoCache bool          `json:"noCache"`
}

type chatDone struct {
	pipeline.Result
	Error string `json:"error,omitempty"`
	Kind  string `json:"kind,omitempty"` // ok | rejected | timeout | canceled | failed
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}

	ctx := r.Context()
	sse := newSSE(w)

	// pipeline.Execute 会阻塞，而 OnDelta 由 worker goroutine 回调。
	// 直接在 OnDelta 里写 ResponseWriter 等于跨 goroutine 并发写，
	// 所以这里把执行放到后台，增量走 channel 回到本 goroutine 串行写出。
	deltas := make(chan string, 512)
	type outcome struct {
		res pipeline.Result
		err error
	}
	resCh := make(chan outcome, 1)

	go func() {
		res, err := s.pipe.Execute(ctx, pipeline.Input{
			Name:    "chat",
			Query:   req.Query,
			History: req.History,
			Live:    true,
			NoCache: req.NoCache,
			OnDelta: func(t string) {
				select {
				case deltas <- t:
				case <-ctx.Done(): // 客户端已断开，别把 worker 卡在这里
				}
			},
		})
		close(deltas)
		resCh <- outcome{res, err}
	}()

	// 心跳：客户端断开时 Flush 会报错，靠它及时发现死连接
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

	for open := true; open; {
		select {
		case d, ok := <-deltas:
			if !ok {
				open = false
				break
			}
			if err := sse.send("delta", map[string]string{"text": d}); err != nil {
				open = false
			}
		case <-tick.C:
			if err := sse.ping(); err != nil {
				open = false
			}
		case <-ctx.Done():
			open = false
		}
	}

	// 把剩余增量排干，避免后台 goroutine 卡在 send 上
	for range deltas {
	}
	out := <-resCh

	done := chatDone{Result: out.res, Kind: string(pipeline.Outcome(out.err))}
	if out.err != nil {
		done.Error = out.err.Error()
	}
	_ = sse.send("done", done)
}

// ---------- 全局事件流：链路 span + 指标快照 ----------

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, events := s.tracer.Hub().Subscribe(1024)
	defer s.tracer.Hub().Unsubscribe(id)

	sse := newSSE(w)
	if err := sse.send("snapshot", s.snapshot(60)); err != nil {
		return
	}

	// 指标 2Hz 推送。再快前端也画不过来，还会让 SSE 通道被指标挤满，
	// 把真正重要的 span 事件挤掉。
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	beat := time.NewTicker(20 * time.Second)
	defer beat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if err := sse.send(ev.Type, ev); err != nil {
				return
			}
		case <-tick.C:
			if err := sse.send("snapshot", s.snapshot(60)); err != nil {
				return
			}
		case <-beat.C:
			if err := sse.ping(); err != nil {
				return
			}
		}
	}
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	n := intQuery(r, "series", 60)
	writeJSON(w, http.StatusOK, s.snapshot(n))
}

// ---------- 链路查询 ----------

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	limit := intQuery(r, "limit", 60)
	if limit > 500 {
		limit = 500
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"traces": s.tracer.Recorder().Recent(limit),
		"total":  s.tracer.Recorder().Len(),
	})
}

func (s *Server) handleTraceDetail(w http.ResponseWriter, r *http.Request) {
	t, ok := s.tracer.Recorder().Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "trace 不存在或已被淘汰")
		return
	}
	writeJSON(w, http.StatusOK, t.View(true))
}

// ---------- 运行时调参 ----------

type configPayload struct {
	Runtime *config.Runtime `json:"runtime,omitempty"`
	Mock    *llm.MockConfig `json:"mock,omitempty"`
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	out := configPayload{}
	rt := s.pipe.Runtime()
	out.Runtime = &rt
	if s.mock != nil {
		mc := s.mock.Config()
		out.Mock = &mc
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePutConfig(w http.ResponseWriter, r *http.Request) {
	var in configPayload
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<18)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if in.Runtime != nil {
		s.pipe.SetRuntime(sanitize(*in.Runtime))
	}
	if in.Mock != nil && s.mock != nil {
		s.mock.SetConfig(*in.Mock)
	}
	s.handleGetConfig(w, r)
}

// sanitize 给热更新的参数兜底。
// 前端滑块理论上不会越界，但接口是公开的，
// 一个 workers=0 就能让整个服务静默卡死 —— 这种代价太大，必须在入口拦住。
func sanitize(rt config.Runtime) config.Runtime {
	rt.RequestTimeoutMS = clampInt(rt.RequestTimeoutMS, 200, 300000)
	rt.QueueTimeoutMS = clampInt(rt.QueueTimeoutMS, 10, 60000)
	rt.RetrievalTimeoutMS = clampInt(rt.RetrievalTimeoutMS, 10, 60000)
	rt.LLMTimeoutMS = clampInt(rt.LLMTimeoutMS, 100, 300000)
	rt.RateLimit = clampF(rt.RateLimit, 0.1, 100000)
	rt.Burst = clampF(rt.Burst, 1, 200000)
	rt.MaxConcurrentLLM = clampInt(rt.MaxConcurrentLLM, 1, 4096)
	rt.Workers = clampInt(rt.Workers, 1, 4096)
	rt.RetryAttempts = clampInt(rt.RetryAttempts, 1, 10)
	rt.CacheTTLSec = clampInt(rt.CacheTTLSec, 1, 86400)
	rt.BreakerRatio = clampF(rt.BreakerRatio, 0.05, 1)
	rt.BreakerMinReq = clampInt(rt.BreakerMinReq, 1, 100000)
	rt.BreakerOpenMS = clampInt(rt.BreakerOpenMS, 100, 600000)
	return rt
}

// ---------- 压测 ----------

func (s *Server) handleBenchStart(w http.ResponseWriter, r *http.Request) {
	var opts bench.Options
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<18)).Decode(&opts); err != nil && err.Error() != "EOF" {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if !s.runner.Start(opts) {
		writeErr(w, http.StatusConflict, "已有压测在运行，请先停止")
		return
	}
	writeJSON(w, http.StatusAccepted, s.runner.Status())
}

func (s *Server) handleBenchStop(w http.ResponseWriter, r *http.Request) {
	s.runner.Stop()
	writeJSON(w, http.StatusOK, s.runner.Status())
}

// ---------- 重置 ----------

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.pipe.Metrics().Reset()
	s.pipe.Breaker().Reset()
	s.pipe.ClearCache()
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ---------- 小工具 ----------

func intQuery(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func clampF(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
