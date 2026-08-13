import { useEffect, useRef, useState } from 'react'
import { streamChat } from '../api'
import type { ChatResult, SpanView } from '../types'
import { Waterfall } from './Waterfall'

interface Msg {
  role: 'user' | 'assistant'
  content: string
  result?: ChatResult
  streaming?: boolean
  error?: string
}

const SAMPLES = [
  'Go 的 context 在高并发链路里应该怎么传？',
  '为什么流式响应不能在首 token 之后重试？',
  '熔断器的半开状态是干什么用的？',
  'worker pool 的队列该设多大？',
]

export function Playground({ liveTraces }: { liveTraces: Map<string, SpanView[]> }) {
  const [msgs, setMsgs] = useState<Msg[]>([])
  const [input, setInput] = useState('')
  const [noCache, setNoCache] = useState(false)
  const [busy, setBusy] = useState(false)
  const [activeTrace, setActiveTrace] = useState<string | null>(null)
  const ctrlRef = useRef<AbortController | null>(null)
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: 'smooth' })
  }, [msgs])

  // 新链路一出现就切过去，让瀑布图跟着当前这次请求走
  useEffect(() => {
    if (!busy) return
    const ids = [...liveTraces.keys()]
    const last = ids[ids.length - 1]
    if (last && last !== activeTrace) setActiveTrace(last)
  }, [liveTraces.size, busy, activeTrace, liveTraces])

  const send = (text: string) => {
    const q = text.trim()
    if (!q || busy) return

    const history = msgs
      .filter((m) => !m.error)
      .slice(-6)
      .map((m) => ({ role: m.role, content: m.content }))

    setMsgs((p) => [...p, { role: 'user', content: q }, { role: 'assistant', content: '', streaming: true }])
    setInput('')
    setBusy(true)

    ctrlRef.current = streamChat(
      { query: q, history, noCache },
      {
        onDelta: (t) =>
          setMsgs((p) => {
            const n = [...p]
            const last = n[n.length - 1]
            n[n.length - 1] = { ...last, content: last.content + t }
            return n
          }),
        onDone: (r) => {
          setMsgs((p) => {
            const n = [...p]
            const last = n[n.length - 1]
            n[n.length - 1] = { ...last, streaming: false, result: r, error: r.error }
            return n
          })
          setActiveTrace(r.traceId)
          setBusy(false)
        },
        onError: (m) => {
          setMsgs((p) => {
            const n = [...p]
            const last = n[n.length - 1]
            n[n.length - 1] = { ...last, streaming: false, error: m }
            return n
          })
          setBusy(false)
        },
      },
    )
  }

  const stop = () => {
    // AbortController 一断，Go 那边 r.Context() 立刻 Done，
    // 整条链路的 span 会标成 canceled —— 这正是要演示的取消传播
    ctrlRef.current?.abort()
    ctrlRef.current = null
    setMsgs((p) => {
      const n = [...p]
      const last = n[n.length - 1]
      if (last?.streaming) n[n.length - 1] = { ...last, streaming: false, error: '已被用户取消' }
      return n
    })
    setBusy(false)
  }

  const spans = activeTrace ? (liveTraces.get(activeTrace) ?? []) : []

  return (
    <div className="grid" style={{ gridTemplateColumns: 'minmax(0,1fr) minmax(0,1.05fr)' }}>
      <div className="card" style={{ display: 'flex', flexDirection: 'column', minHeight: 560 }}>
        <h3>
          对话 <span className="sub">流式响应 · 可中途取消</span>
        </h3>

        <div className="chat">
          <div className="chat-log" ref={logRef}>
            {!msgs.length && (
              <div className="empty">
                <div style={{ marginBottom: 14 }}>随便问点什么，右侧会实时画出这次请求的完整链路</div>
                <div className="row" style={{ justifyContent: 'center' }}>
                  {SAMPLES.map((s) => (
                    <button key={s} className="btn sm" onClick={() => send(s)}>
                      {s.length > 20 ? s.slice(0, 20) + '…' : s}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {msgs.map((m, i) => (
              <div key={i} className={`msg ${m.role === 'user' ? 'user' : 'bot'}`}>
                <div className={`bubble ${m.streaming ? 'cursor' : ''}`}>
                  {m.content || (m.streaming ? '' : m.error ? '' : '（空）')}
                </div>
                {m.error && (
                  <div className="meta" style={{ color: 'var(--err)' }}>
                    {m.error}
                  </div>
                )}
                {m.result && !m.error && (
                  <div className="meta">
                    <span>TTFT {m.result.ttftMs.toFixed(0)}ms</span>
                    <span>总计 {m.result.totalMs.toFixed(0)}ms</span>
                    <span>{m.result.tokens} tokens</span>
                    {m.result.attempts > 1 && <span className="c-warn">重试 ×{m.result.attempts}</span>}
                    {m.result.cached && <span className="c-ok">缓存命中</span>}
                    {m.result.merged && <span className="c-accent">请求合并</span>}
                    <span className="c-dim">{m.result.traceId.slice(0, 8)}</span>
                  </div>
                )}
              </div>
            ))}
          </div>

          <div className="composer">
            <textarea
              className="input"
              rows={2}
              placeholder="问点什么…（Enter 发送，Shift+Enter 换行）"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  send(input)
                }
              }}
            />
            {busy ? (
              <button className="btn danger" onClick={stop}>
                停止
              </button>
            ) : (
              <button className="btn primary" onClick={() => send(input)} disabled={!input.trim()}>
                发送
              </button>
            )}
          </div>

          <div className="row" style={{ justifyContent: 'space-between' }}>
            <label className="switch">
              <input type="checkbox" checked={noCache} onChange={(e) => setNoCache(e.target.checked)} />
              跳过缓存与请求合并
            </label>
            <button className="btn sm" onClick={() => setMsgs([])} disabled={!msgs.length}>
              清空
            </button>
          </div>
        </div>
      </div>

      <div className="card" style={{ minHeight: 560, display: 'flex', flexDirection: 'column' }}>
        <h3>
          实时链路
          <span className="sub">{activeTrace ? `trace ${activeTrace.slice(0, 12)}` : '等待请求'}</span>
        </h3>
        <div className="scroll-y" style={{ flex: 1 }}>
          <Waterfall spans={spans} />
        </div>
      </div>
    </div>
  )
}
