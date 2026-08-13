package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// sseWriter 封装 Server-Sent Events 的写入细节。
//
// SSE 的坑集中在两点：一是必须显式 Flush，否则数据卡在缓冲区里，
// 「流式」会退化成「等全部生成完一次性返回」；二是经过 Nginx 之类的反代时
// 默认会开缓冲，必须用 X-Accel-Buffering: no 关掉。
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func newSSE(w http.ResponseWriter) *sseWriter {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	s := &sseWriter{w: w, rc: http.NewResponseController(w)}
	s.raw(": connected\n\n")
	return s
}

func (s *sseWriter) raw(str string) {
	_, _ = fmt.Fprint(s.w, str)
	_ = s.rc.Flush()
}

// send 发送一个命名事件，data 会被 JSON 序列化。
func (s *sseWriter) send(event string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", event, b); err != nil {
		return err
	}
	return s.rc.Flush()
}

// ping 发送注释行心跳，用于维持连接与探测客户端是否已断开。
func (s *sseWriter) ping() error {
	if _, err := fmt.Fprint(s.w, ": ping\n\n"); err != nil {
		return err
	}
	return s.rc.Flush()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
