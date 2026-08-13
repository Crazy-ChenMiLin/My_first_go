import { useEffect, useRef, useState } from 'react'
import type { Snapshot, SpanView, TraceView } from '../types'

interface HubEvent {
  type: 'span.start' | 'span.end' | 'trace.end'
  ts: number
  span?: SpanView
  trace?: TraceView
}

export interface LiveState {
  snapshot: Snapshot | null
  connected: boolean
  /** 当前正在进行中的链路，按 traceId 聚合 span，用于实时瀑布图 */
  liveTraces: Map<string, SpanView[]>
  /** 最近完成的链路摘要 */
  recentTraces: TraceView[]
}

/**
 * 订阅后端的全局事件流。
 *
 * 整个前端只开这一条 SSE：指标快照、span 事件、压测状态全从这里来。
 * 多开几条不仅浪费连接，还会让不同面板看到的数据出现时间差。
 */
export function useLiveStream(): LiveState {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [connected, setConnected] = useState(false)
  const [, force] = useState(0)

  // span 事件频率可能很高，放进 state 会触发海量重渲染。
  // 用 ref 累积 + 定频强制刷新，把渲染压到 10Hz。
  const liveRef = useRef<Map<string, SpanView[]>>(new Map())
  const recentRef = useRef<TraceView[]>([])
  const dirty = useRef(false)

  useEffect(() => {
    let es: EventSource | null = null
    let closed = false
    let retry: number | undefined

    const connect = () => {
      if (closed) return
      es = new EventSource('/api/stream')

      es.addEventListener('open', () => setConnected(true))

      es.addEventListener('snapshot', (e) => {
        setSnapshot(JSON.parse((e as MessageEvent).data) as Snapshot)
        setConnected(true)
      })

      const onSpan = (e: Event) => {
        const ev = JSON.parse((e as MessageEvent).data) as HubEvent
        const s = ev.span
        if (!s) return
        const list = liveRef.current.get(s.traceId) ?? []
        const i = list.findIndex((x) => x.id === s.id)
        if (i >= 0) list[i] = s
        else list.push(s)
        liveRef.current.set(s.traceId, list)
        dirty.current = true
      }
      es.addEventListener('span.start', onSpan)
      es.addEventListener('span.end', onSpan)

      es.addEventListener('trace.end', (e) => {
        const ev = JSON.parse((e as MessageEvent).data) as HubEvent
        if (!ev.trace) return
        recentRef.current = [ev.trace, ...recentRef.current].slice(0, 40)
        // 链路结束后再留一会儿，避免瀑布图刚画完就消失
        const id = ev.trace.id
        window.setTimeout(() => {
          liveRef.current.delete(id)
          dirty.current = true
        }, 20000)
        dirty.current = true
      })

      es.addEventListener('error', () => {
        setConnected(false)
        es?.close()
        // EventSource 自带重连，但连接被服务端主动关闭时它会放弃，所以自己兜一层
        retry = window.setTimeout(connect, 2000)
      })
    }

    connect()
    const timer = window.setInterval(() => {
      if (dirty.current) {
        dirty.current = false
        force((n) => n + 1)
      }
    }, 100)

    return () => {
      closed = true
      window.clearInterval(timer)
      if (retry) window.clearTimeout(retry)
      es?.close()
    }
  }, [])

  return {
    snapshot,
    connected,
    liveTraces: liveRef.current,
    recentTraces: recentRef.current,
  }
}
