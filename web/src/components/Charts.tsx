import type { ReactNode } from 'react'
import type { Point } from '../types'

export function Stat({
  label, value, unit, tone, foot,
}: {
  label: string
  value: ReactNode
  unit?: string
  tone?: 'ok' | 'err' | 'warn' | 'rej' | 'accent' | 'dim'
  foot?: ReactNode
}) {
  return (
    <div className="card stat">
      <div className="label">{label}</div>
      <div className={`value ${tone ? 'c-' + tone : ''}`}>
        {value}
        {unit && <span style={{ fontSize: 13, color: 'var(--fg-2)', marginLeft: 3 }}>{unit}</span>}
      </div>
      {foot && <div className="foot">{foot}</div>}
    </div>
  )
}

/** 堆叠面积图：成功 / 拒绝 / 失败 三层，直观看出压力下系统被什么挡住了 */
export function StackedArea({ series, height = 132 }: { series: Point[]; height?: number }) {
  const W = 640
  const H = height
  const pad = { t: 8, r: 4, b: 16, l: 30 }
  const iw = W - pad.l - pad.r
  const ih = H - pad.t - pad.b

  const data = series.length ? series : []
  const maxY = Math.max(1, ...data.map((p) => p.ok + p.rej + p.fail))
  const n = Math.max(data.length, 1)
  const x = (i: number) => pad.l + (n === 1 ? iw / 2 : (i / (n - 1)) * iw)
  const y = (v: number) => pad.t + ih - (v / maxY) * ih

  // 从底往上依次累加，每层画一条闭合路径
  const layers: { key: keyof Pick<Point, 'ok' | 'rej' | 'fail'>; color: string; label: string }[] = [
    { key: 'ok', color: 'var(--ok)', label: '成功' },
    { key: 'rej', color: 'var(--rej)', label: '拒绝' },
    { key: 'fail', color: 'var(--err)', label: '失败' },
  ]

  const bases = data.map(() => 0)
  const paths = layers.map((L) => {
    const top = data.map((p, i) => {
      const v = bases[i] + (p[L.key] as number)
      const pt = { x: x(i), y: y(v) }
      bases[i] = v
      return pt
    })
    if (!top.length) return { ...L, d: '' }
    const up = top.map((p, i) => `${i ? 'L' : 'M'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join('')
    const down = top
      .map((_, i) => {
        const idx = top.length - 1 - i
        const below = bases[idx] - (data[idx][L.key] as number)
        return `L${x(idx).toFixed(1)},${y(below).toFixed(1)}`
      })
      .join('')
    return { ...L, d: `${up}${down}Z` }
  })

  return (
    <div>
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: H }} preserveAspectRatio="none">
        {[0, 0.5, 1].map((f) => (
          <g key={f}>
            <line x1={pad.l} x2={W - pad.r} y1={y(maxY * f)} y2={y(maxY * f)} stroke="var(--line)" strokeWidth="1" />
            <text x={pad.l - 5} y={y(maxY * f) + 3.5} fill="var(--fg-2)" fontSize="9" textAnchor="end" fontFamily="var(--mono)">
              {Math.round(maxY * f)}
            </text>
          </g>
        ))}
        {paths.map((p) => (
          <path key={p.key} d={p.d} fill={p.color} opacity={0.5} stroke={p.color} strokeWidth="1" />
        ))}
        {!data.length && (
          <text x={W / 2} y={H / 2} fill="var(--fg-2)" fontSize="11" textAnchor="middle">
            暂无流量
          </text>
        )}
      </svg>
      <div className="legend" style={{ marginTop: 4 }}>
        {layers.map((l) => (
          <span key={l.key}>
            <i style={{ background: l.color }} />
            {l.label}
          </span>
        ))}
        <span style={{ marginLeft: 'auto' }}>每秒请求数 · 最近 {data.length}s</span>
      </div>
    </div>
  )
}

/** 折线图，用于延迟趋势 */
export function Line({ series, height = 110 }: { series: Point[]; height?: number }) {
  const W = 640
  const H = height
  const pad = { t: 8, r: 4, b: 16, l: 38 }
  const iw = W - pad.l - pad.r
  const ih = H - pad.t - pad.b
  const data = series.filter((p) => p.ok + p.rej + p.fail > 0)
  const maxY = Math.max(1, ...data.map((p) => p.avgMs))
  const n = Math.max(data.length, 1)
  const x = (i: number) => pad.l + (n === 1 ? iw / 2 : (i / (n - 1)) * iw)
  const y = (v: number) => pad.t + ih - (v / maxY) * ih

  const d = data.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(p.avgMs).toFixed(1)}`).join('')
  const area = d ? `${d}L${x(data.length - 1).toFixed(1)},${y(0)}L${x(0).toFixed(1)},${y(0)}Z` : ''

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: H }} preserveAspectRatio="none">
      {[0, 0.5, 1].map((f) => (
        <g key={f}>
          <line x1={pad.l} x2={W - pad.r} y1={y(maxY * f)} y2={y(maxY * f)} stroke="var(--line)" strokeWidth="1" />
          <text x={pad.l - 5} y={y(maxY * f) + 3.5} fill="var(--fg-2)" fontSize="9" textAnchor="end" fontFamily="var(--mono)">
            {Math.round(maxY * f)}
          </text>
        </g>
      ))}
      {area && <path d={area} fill="var(--accent)" opacity={0.14} />}
      {d && <path d={d} fill="none" stroke="var(--accent)" strokeWidth="1.6" />}
      {!data.length && (
        <text x={W / 2} y={H / 2} fill="var(--fg-2)" fontSize="11" textAnchor="middle">
          暂无数据
        </text>
      )}
    </svg>
  )
}

/** 水平占用条，用于信号量 / 队列 / 令牌桶 */
export function Bar({
  used, total, label, tone = 'accent', unit,
}: {
  used: number
  total: number
  label: string
  tone?: string
  unit?: string
}) {
  const pct = total > 0 ? Math.min(100, (used / total) * 100) : 0
  const color = pct > 90 ? 'var(--err)' : pct > 70 ? 'var(--warn)' : `var(--${tone})`
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11.5, marginBottom: 4 }}>
        <span style={{ color: 'var(--fg-1)' }}>{label}</span>
        <span style={{ fontFamily: 'var(--mono)', color: 'var(--fg-2)' }}>
          {fmt(used)} / {fmt(total)}
          {unit}
        </span>
      </div>
      <div style={{ height: 6, background: 'var(--bg-3)', borderRadius: 3, overflow: 'hidden' }}>
        <div style={{ width: `${pct}%`, height: '100%', background: color, transition: 'width .25s' }} />
      </div>
    </div>
  )
}

function fmt(n: number) {
  return Number.isInteger(n) ? String(n) : n.toFixed(1)
}
