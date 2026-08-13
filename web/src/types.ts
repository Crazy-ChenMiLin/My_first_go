export type SpanKind =
  | 'http' | 'guard' | 'cache' | 'queue' | 'limiter'
  | 'retrieval' | 'prompt' | 'llm' | 'post'

export type SpanStatus = 'running' | 'ok' | 'error' | 'canceled' | 'timeout' | 'rejected'

export interface SpanView {
  id: string
  traceId: string
  parentId?: string
  name: string
  kind: SpanKind
  startMs: number
  endMs?: number
  durationMs: number
  status: SpanStatus
  error?: string
  attrs?: Record<string, unknown>
  events?: { name: string; ts: number }[]
}

export interface TraceView {
  id: string
  name: string
  startMs: number
  endMs?: number
  durationMs: number
  status: SpanStatus
  spanCount: number
  tokens: number
  attrs?: Record<string, unknown>
  spans?: SpanView[]
}

export interface Point {
  sec: number
  ok: number
  fail: number
  rej: number
  avgMs: number
}

export interface MetricsSnapshot {
  total: number; success: number; failed: number; rejected: number
  canceled: number; timeout: number; inflight: number; tokens: number
  merged: number; cacheHits: number; retries: number
  qps: number; avgMs: number
  p50Ms: number; p90Ms: number; p95Ms: number; p99Ms: number
  ttftP50Ms: number; ttftP95Ms: number
  successPct: number; uptimeSec: number
  series: Point[]
}

export interface Runtime {
  requestTimeoutMs: number
  queueTimeoutMs: number
  retrievalTimeoutMs: number
  llmTimeoutMs: number
  rateLimit: number
  burst: number
  maxConcurrentLlm: number
  workers: number
  queueSize: number
  retryAttempts: number
  cacheEnabled: boolean
  cacheTtlSec: number
  singleflightOn: boolean
  breakerRatio: number
  breakerMinReq: number
  breakerOpenMs: number
}

export interface MockConfig {
  ttftMinMs: number; ttftMaxMs: number
  tokenMinMs: number; tokenMaxMs: number
  errorRate: number; stallRate: number; stallMs: number
  maxTokens: number; modelName: string
}

export interface BenchStatus {
  running: boolean; startedAt: number; elapsedSec: number; durationSec: number
  targetQps: number; sent: number; done: number
  ok: number; failed: number; rejected: number; timeout: number; canceled: number
  inflight: number; dropped: number
  actualQps: number; successPct: number
  avgMs: number; p50Ms: number; p95Ms: number; p99Ms: number; maxMs: number
}

export interface Snapshot {
  ts: number
  uptimeSec: number
  metrics: MetricsSnapshot
  pool: {
    workers: number; queued: number; queueCap: number; inflight: number
    completed: number; rejected: number; expired: number
    avgWaitMs: number; queueTimeoutMs: number
  }
  breaker: {
    state: 'closed' | 'open' | 'half_open'
    successes: number; failures: number; failureRatio: number
    threshold: number; transitions: number; cooldownMs: number
  }
  sem: { inflight: number; capacity: number; queued: number }
  limiter: { tokens: number; rate: number; burst: number }
  bench: BenchStatus
  runtime: Runtime
  provider: { name: string; model: string; mock: boolean }
  cacheSize: number
  traceCount: number
  watchers: number
}

export interface ChatResult {
  traceId: string
  text: string
  tokens: number
  ttftMs: number
  totalMs: number
  cached: boolean
  merged: boolean
  attempts: number
  provider: string
  model: string
  error?: string
  kind?: string
}
