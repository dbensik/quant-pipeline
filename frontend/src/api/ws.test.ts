import { afterEach, describe, expect, it, vi } from 'vitest'

import type { BacktestInput } from './client'
import { backtestSocketUrl, runBacktestOverSocket } from './ws'
import type { WsMessage } from './ws'

const REQUEST: BacktestInput = {
  symbol: 'AAPL',
  strategy_id: 'ma_crossover',
  start: '2024-01-01',
  end: '2024-12-31',
}

/**
 * Minimal WebSocket stand-in.
 *
 * ws.ts is the only part of the API surface with no OpenAPI document, so
 * nothing machine-checks it against the server — which makes its own logic
 * (the settle guard, close-before-result) worth testing directly.
 */
class FakeWebSocket {
  static last: FakeWebSocket | null = null

  url: string
  sent: string[] = []
  closed = false

  onopen: (() => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onerror: (() => void) | null = null
  onclose: (() => void) | null = null

  constructor(url: string) {
    this.url = url
    FakeWebSocket.last = this
  }

  send(data: string) {
    this.sent.push(data)
  }

  close() {
    this.closed = true
    this.onclose?.()
  }

  // -- test drivers --------------------------------------------------------
  open() {
    this.onopen?.()
  }

  emit(message: WsMessage) {
    this.onmessage?.({ data: JSON.stringify(message) })
  }

  emitRaw(data: string) {
    this.onmessage?.({ data })
  }

  fail() {
    this.onerror?.()
  }
}

function install() {
  vi.stubGlobal('WebSocket', FakeWebSocket as unknown as typeof WebSocket)
  return () => FakeWebSocket.last!
}

afterEach(() => {
  vi.unstubAllGlobals()
  FakeWebSocket.last = null
})

const PROGRESS = {
  type: 'progress',
  stage: 'running',
  pct: 40,
  detail: 'Running',
} as const

const RESULT = {
  type: 'result',
  symbol: 'AAPL',
  strategy_id: 'ma_crossover',
  strategy_name: 'Moving Average Crossover',
  bars: 251,
  trades: 2,
  metrics: { 'Final Value': 100 },
  caveat: null,
  params: {},
  seed: 42,
  initial_capital: 100_000,
} as const

describe('backtestSocketUrl', () => {
  it('converts the http base URL to a ws URL', () => {
    expect(backtestSocketUrl()).toMatch(/^ws:\/\/.+\/api\/v1\/ws\/backtest$/)
    expect(backtestSocketUrl()).not.toContain('http')
  })
})

describe('runBacktestOverSocket', () => {
  it('sends the request once the socket opens', async () => {
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})

    socket().open()
    expect(JSON.parse(socket().sent[0])).toMatchObject({ symbol: 'AAPL' })

    socket().emit(RESULT)
    await expect(promise).resolves.toMatchObject({ type: 'result' })
  })

  it('reports every progress message and resolves with the result', async () => {
    const socket = install()
    const seen: WsMessage[] = []
    const promise = runBacktestOverSocket(REQUEST, (m) => seen.push(m))

    socket().open()
    socket().emit({ ...PROGRESS, stage: 'fetching', pct: 10 })
    socket().emit(PROGRESS)
    socket().emit(RESULT)

    await expect(promise).resolves.toMatchObject({ bars: 251 })
    expect(seen.map((m) => m.type)).toEqual(['progress', 'progress', 'result'])
  })

  it('closes the socket after the result', async () => {
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().open()
    socket().emit(RESULT)
    await promise
    expect(socket().closed).toBe(true)
  })

  it('rejects on a server error message', async () => {
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().open()
    socket().emit({ type: 'error', detail: 'Unknown symbol', code: 404 })
    await expect(promise).rejects.toThrow('Unknown symbol')
  })

  it('rejects when the socket closes before any result', async () => {
    // Without this, a server that dies mid-run leaves the promise pending
    // forever and the UI stuck on "Running…" with no way out.
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().open()
    socket().close()
    await expect(promise).rejects.toThrow(/closed before/i)
  })

  it('resolves even though the server closes right after the result', async () => {
    // The server closes the socket immediately after `result`, so onclose
    // always fires on a successful run and must not turn it into a failure.
    //
    // NOTE: this passes with or without ws.ts's `settled` flag — calling
    // reject() on an already-resolved promise is a no-op in JS, so that flag is
    // documentation of intent rather than load-bearing logic. Verified by
    // removing it: no test changed. Kept because the behaviour it asserts (a
    // successful run stays successful) is worth pinning regardless.
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().open()
    socket().emit(RESULT)
    socket().close()
    await expect(promise).resolves.toMatchObject({ type: 'result' })
  })

  it('rejects on a transport error', async () => {
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().fail()
    await expect(promise).rejects.toThrow(/WebSocket error/i)
  })

  it('ignores unparseable frames rather than killing the run', async () => {
    const socket = install()
    const promise = runBacktestOverSocket(REQUEST, () => {})
    socket().open()
    socket().emitRaw('<html>not json</html>')
    socket().emit(RESULT)
    await expect(promise).resolves.toMatchObject({ type: 'result' })
  })
})
