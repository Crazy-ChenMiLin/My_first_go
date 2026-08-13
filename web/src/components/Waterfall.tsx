import { useMemo, useState } from 'react'
import type { SpanKind, SpanView } from '../types'

export const KIND_COLOR: Record<SpanKind, string> = {
  http: '#5b8cff',
  guard: '#94a3b8',
  limiter: '#c084fc',
  queue: '#fbbf24',
  cache: '#f472b6',
  retrieval: '#34d399',
  prompt: '#a78bfa',
  llm: '#22d3ee',
  post: '#64748b',
}

const KIND_LABEL: Record<SpanKind, string> = {
  http: '入口',
  guard: '校验',
  limiter: '限流/熔断',
  queue: '排队',
  cache: '缓存/合并',
  retrieval: '检索',
  prompt: '拼 Prompt',
  llm: '模型',
  post: '后处理',
}

/**
 * 链路瀑布图。
 *
 * 时间轴统一用「相对 trace 起点的毫秒偏移」，未结束的 span 用当前时间兜底，
 * 这样进行中的请求也能实时看到条子在长 —— 这是整个页面最直观的部分。
 */
export function Waterfall({ spans, compact = false }: { spans: SpanView[]; compact?: boolean }) {
  const [sel, setSel] = useState<string | null>(null)

  const { rows, t0, span } = useMemo(() => {
    if (!spans.length) return { rows: [] as (SpanView & { depth: number })[], t0: 0, span: 1 }

    const t0 = Math.min(...spans.map((s) => s.startMs))
    const now = Date.now()
    const tEnd = Math.max(...spans.map((s) => s.endMs || now))
    const span = Math.max(1, tEnd - t0)

    // 按父子关系排成树，同层按开始时间排序 —— 不然并发的检索分支会乱序，看不出扇出结构
    const byParent = new Map<string, SpanView[]>()
    const ids = new Set(spans.map((s) => s.id))
    for (const s of spans) {
      const key = s.parentId && ids.has(s.parentId) ? s.parentId : '__root__'
      const arr = byParent.get(key) ?? []
      arr.push(s)
      byParent.set(key, arr)
    }
    for (const arr of byParent.values()) arr.sort((a, b) => a.startMs - b.startMs)

    const rows: (SpanView & { depth: number })[] = []
    const walk = (key: string, depth: number) => {
      for (const s of byParent.get(key) ?? []) {
        rows.push({ ...s, depth })
        walk(s.id, depth + 1)
      }
    }
    walk('__root__', 0)
    return { rows, t0, span }
  }, [spans])

  if (!rows.length) return <div className="empty">还没有链路数据，去发一条消息试试</div>

  const now = Date.now()
  const kinds = [...new Set(rows.map((r) => r.kind))]

  return (
    <div className="wf">
      {rows.map((s) => {
        const end = s.endMs || now
        const left = ((s.startMs - t0) / span) * 100
        const width = Math.max(0.4, ((end - s.startMs) / span) * 100)
        const running = s.status === 'running'
        const color =
          s.status === 'error' || s.status === 'timeout'
            ? 'var(--err)'
            : s.status === 'rejected'
              ? 'var(--rej)'
              : s.status === 'canceled'
                ? 'var(--fg-2)'
                : KIND_COLOR[s.kind]
        const dur = s.durationMs || end - s.startMs
        const on = sel === s.id

        return (
          <div key={s.id}>
            <div className="wf-row" onClick={() => setSel(on ? null : s.id)}>
              <div className="wf-name" style={{ paddingLeft: s.depth * 11 }} title={s.name}>
                {s.name}
              </div>
              <div className="wf-track">
                <div
                  className="wf-bar"
                  style={{
                    left: `${left}%`,
                    width: `${width}%`,
                    background: color,
                    opacity: running ? 0.55 : 0.9,
                  }}
                />
                {s.events?.map((e) => (
                  <div
                    key={e.name + e.ts}
                    className="wf-mark"
                    title={e.name}
                    style={{ left: `${((e.ts - t0) / span) * 100}%` }}
                  />
                ))}
              </div>
              <div className="wf-dur">{running ? '…' : `${dur.toFixed(1)}ms`}</div>
            </div>

            {on && (
              <div style={{ padding: '5px 0 9px', paddingLeft: s.depth * 11 + 6 }}>
                <div className="attrs">
                  <code>{KIND_LABEL[s.kind]}</code>
                  <code>{s.status}</code>
                  {Object.entries(s.attrs ?? {}).map(([k, v]) => (
                    <code key={k}>
                      {k}={String(v)}
                    </code>
                  ))}
                </div>
                {s.error && <div style={{ color: 'var(--err)', fontSize: 11, marginTop: 4 }}>{s.error}</div>}
              </div>
            )}
          </div>
        )
      })}

      {!compact && (
        <div className="legend" style={{ marginTop: 10 }}>
          {kinds.map((k) => (
            <span key={k}>
              <i style={{ background: KIND_COLOR[k] }} />
              {KIND_LABEL[k]}
            </span>
          ))}
          <span style={{ marginLeft: 'auto' }}>总跨度 {span}ms · 白线为埋点事件</span>
        </div>
      )}
    </div>
  )
}
