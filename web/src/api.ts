import type { BenchStatus, ChatResult, MockConfig, Runtime, Snapshot, TraceView } from './types'

async function j<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error((body as { error?: string }).error || `HTTP ${res.status}`)
  }
  return res.json() as Promise<T>
}

export const api = {
  snapshot: (series = 60) => fetch(`/api/snapshot?series=${series}`).then(j<Snapshot>),
  traces: (limit = 60) => fetch(`/api/traces?limit=${limit}`).then(j<{ traces: TraceView[]; total: number }>),
  trace: (id: string) => fetch(`/api/traces/${id}`).then(j<TraceView>),
  config: () => fetch('/api/config').then(j<{ runtime: Runtime; mock?: MockConfig }>),
  saveConfig: (body: { runtime?: Runtime; mock?: MockConfig }) =>
    fetch('/api/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(j<{ runtime: Runtime; mock?: MockConfig }>),
  startBench: (opts: { qps: number; durationSec: number; concurrency: number; noCache: boolean }) =>
    fetch('/api/bench', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(opts),
    }).then(j<BenchStatus>),
  stopBench: () => fetch('/api/bench', { method: 'DELETE' }).then(j<BenchStatus>),
  reset: () => fetch('/api/reset', { method: 'POST' }).then(j<{ ok: boolean }>),
}

export interface ChatHandlers {
  onDelta: (text: string) => void
  onDone: (r: ChatResult) => void
  onError: (msg: string) => void
}

/**
 * 发起一次流式对话。
 *
 * 这里没用 EventSource，因为它只支持 GET，没法带请求体。
 * fetch + ReadableStream 手动解析 SSE 帧，顺便还能拿到 AbortController —— 
 * 用户点「停止」时能真正把取消信号传到后端，让整条 Go 链路一起收手。
 */
export function streamChat(
  body: { query: string; history?: { role: string; content: string }[]; noCache?: boolean },
  h: ChatHandlers,
): AbortController {
  const ctrl = new AbortController()

  ;(async () => {
    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      })
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`)

      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ''

      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })

        // SSE 以空行分帧。必须保留最后一个不完整片段等下一个 chunk 补齐，
        // 否则中文 token 会在字节边界被切成乱码。
        const frames = buf.split('\n\n')
        buf = frames.pop() ?? ''

        for (const frame of frames) {
          if (!frame.trim() || frame.startsWith(':')) continue
          let event = 'message'
          let data = ''
          for (const line of frame.split('\n')) {
            if (line.startsWith('event:')) event = line.slice(6).trim()
            else if (line.startsWith('data:')) data += line.slice(5).trim()
          }
          if (!data) continue
          if (event === 'delta') h.onDelta((JSON.parse(data) as { text: string }).text)
          else if (event === 'done') h.onDone(JSON.parse(data) as ChatResult)
        }
      }
    } catch (e) {
      if ((e as Error).name !== 'AbortError') h.onError((e as Error).message)
    }
  })()

  return ctrl
}
