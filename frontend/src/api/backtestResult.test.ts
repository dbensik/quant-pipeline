import { describe, expect, it } from 'vitest'

import type { BacktestResponse } from './client'
import type { WsResult } from './ws'
import { fromRest, fromSocket } from './backtestResult'

const REST: BacktestResponse = {
  symbol: 'AAPL',
  strategy_id: 'ma_crossover',
  strategy_name: 'Moving Average Crossover',
  start: '2024-01-01T00:00:00Z',
  end: '2024-12-31T00:00:00Z',
  bars: 251,
  params: { short_window: 10, long_window: 30 },
  initial_capital: 250_000,
  seed: 42,
  metrics: { 'Final Value': 123_456, 'Sharpe Ratio': null },
  caveat: null,
  equity_curve: [
    { time: '2024-01-01T00:00:00Z', total: 100, cash: 100, holdings: 0, position: 0, signal: 0 },
    { time: '2024-01-02T00:00:00Z', total: 110, cash: 0, holdings: 110, position: 1, signal: 1 },
  ],
  trades: [{ id: 1 }, { id: 2 }, { id: 3 }],
}

const SOCKET: WsResult = {
  type: 'result',
  symbol: 'AAPL',
  strategy_id: 'ma_crossover',
  strategy_name: 'Moving Average Crossover',
  bars: 251,
  trades: 3,
  metrics: { 'Final Value': 123_456, 'Sharpe Ratio': null },
  caveat: null,
  params: { short_window: 10, long_window: 30 },
  seed: 42,
  initial_capital: 250_000,
  equity_curve: [
    { time: '2024-01-01T00:00:00Z', total: 100 },
    { time: '2024-01-02T00:00:00Z', total: 110 },
  ],
}

describe('normalising the two transports', () => {
  it('produces the same view model from either', () => {
    // The whole point: nothing downstream should branch on transport. The only
    // legitimate difference is `via`.
    const { via: _restVia, ...rest } = fromRest(REST)
    const { via: _socketVia, ...socket } = fromSocket(SOCKET)
    expect(rest).toEqual(socket)
  })

  it('records which transport produced it', () => {
    expect(fromRest(REST).via).toBe('rest')
    expect(fromSocket(SOCKET).via).toBe('websocket')
  })
})

describe('trade count', () => {
  it('counts a REST trade array', () => {
    // REST sends the whole log; the UI only ever shows how many.
    expect(fromRest(REST).tradeCount).toBe(3)
  })

  it('passes through a websocket count', () => {
    // The socket sends a number, not an array — the shape difference this
    // module exists to absorb.
    expect(fromSocket(SOCKET).tradeCount).toBe(3)
  })

  it('treats a missing trade list as zero', () => {
    expect(fromRest({ ...REST, trades: undefined }).tradeCount).toBe(0)
  })
})

describe('initial capital', () => {
  it('is read from the top-level field, not from params', () => {
    // THE regression. It was read as `params.initial_capital`, which is always
    // undefined — params carries STRATEGY parameters only — so the equity
    // chart's break-even line silently fell back to a hardcoded 100,000.
    expect(fromRest(REST).initialCapital).toBe(250_000)
    expect(fromSocket(SOCKET).initialCapital).toBe(250_000)
  })

  it('does not leak into the strategy parameters', () => {
    expect(fromRest(REST).params).toEqual({ short_window: 10, long_window: 30 })
    expect(fromRest(REST).params).not.toHaveProperty('initial_capital')
  })
})

describe('field mapping', () => {
  it('keeps a null metric null rather than coercing it', () => {
    // The API converts NaN/Infinity to null (neither is valid JSON); turning
    // that into 0 would present "no value" as a real result.
    expect(fromRest(REST).metrics['Sharpe Ratio']).toBeNull()
  })

  it('reduces the REST equity curve to time and total', () => {
    expect(fromRest(REST).equityCurve).toEqual([
      { time: '2024-01-01T00:00:00Z', total: 100 },
      { time: '2024-01-02T00:00:00Z', total: 110 },
    ])
  })

  it('handles an omitted equity curve', () => {
    expect(fromRest({ ...REST, equity_curve: undefined }).equityCurve).toEqual([])
    expect(fromSocket({ ...SOCKET, equity_curve: undefined }).equityCurve).toEqual([])
  })

  it('carries the caveat through', () => {
    const caveat = 'Known look-ahead bias'
    expect(fromRest({ ...REST, caveat }).caveat).toBe(caveat)
    expect(fromSocket({ ...SOCKET, caveat }).caveat).toBe(caveat)
  })
})
