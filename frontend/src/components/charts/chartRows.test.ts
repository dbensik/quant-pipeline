import { describe, expect, it } from 'vitest'

import type { OHLCVBar, SignalPoint } from '@/api/client'
import { buildChartRows } from './chartRows'

function bar(day: number, close: number | null): OHLCVBar {
  const date = String(day).padStart(2, '0')
  return {
    time: `2024-01-${date}T00:00:00Z`,
    open: close,
    high: close,
    low: close,
    close,
    volume: 1000,
  }
}

function signal(day: number, value: number | null): SignalPoint {
  const date = String(day).padStart(2, '0')
  return { time: `2024-01-${date}T00:00:00Z`, signal: value, close: 100 }
}

const BARS = Array.from({ length: 10 }, (_, i) => bar(i + 1, 100 + i))

describe('buildChartRows — rows', () => {
  it('maps bars to date/close rows', () => {
    const { rows } = buildChartRows(BARS)
    expect(rows).toHaveLength(10)
    expect(rows[0]).toEqual({ date: '2024-01-01', close: 100 })
  })

  it('drops bars with a null close', () => {
    // The migration kept partial rows where OHLC was incomplete. Plotting them
    // draws a line diving to zero.
    const { rows } = buildChartRows([bar(1, 100), bar(2, null), bar(3, 102)])
    expect(rows.map((r) => r.close)).toEqual([100, 102])
  })

  it('returns no markers when signals are omitted', () => {
    const { buyCount, sellCount, rows } = buildChartRows(BARS)
    expect(buyCount).toBe(0)
    expect(sellCount).toBe(0)
    expect(rows.every((row) => row.buy === undefined && row.sell === undefined)).toBe(true)
  })
})

describe('buildChartRows — markers show transitions, not held positions', () => {
  it('marks a hold-forever strategy once, not on every bar', () => {
    // THE regression. A strategy that goes long on bar 1 and holds emits +1 on
    // every subsequent bar. Marking each one buries the price line under a
    // solid band of triangles.
    const signals = BARS.map((_, i) => signal(i + 1, 1))
    const { buyCount, sellCount, rows } = buildChartRows(BARS, signals)

    expect(buyCount).toBe(1)
    expect(sellCount).toBe(0)
    expect(rows.filter((row) => row.buy !== undefined)).toHaveLength(1)
  })

  it("marks a buy-and-hold strategy's opening entry", () => {
    // The scan seeds `previous` at 0 (flat), so a first signal of +1 is an
    // entry. Seeding with null instead dropped it, and Buy and Hold reported
    // "no signal changes" for a strategy that plainly makes one.
    const signals = BARS.map((_, i) => signal(i + 1, 1))
    const { rows } = buildChartRows(BARS, signals)
    expect(rows[0].buy).toBe(rows[0].close)
  })

  it('marks each direction change', () => {
    // flat, long, long, flat, short, short, flat, long, long, long
    const values = [0, 1, 1, 0, -1, -1, 0, 1, 1, 1]
    const signals = values.map((value, i) => signal(i + 1, value))
    const { buyCount, sellCount } = buildChartRows(BARS, signals)

    // up-transitions: 0->1 (bar 2), -1->0 (bar 7), 0->1 (bar 8)
    expect(buyCount).toBe(3)
    // down-transitions: 1->0 (bar 4), 0->-1 (bar 5)
    expect(sellCount).toBe(2)
  })

  it('places a marker at the closing price of its bar', () => {
    const signals = [signal(1, 0), signal(2, 1)]
    const { rows } = buildChartRows(BARS, signals)
    expect(rows[1].buy).toBe(rows[1].close)
  })

  it('ignores null warm-up signals without treating them as a change', () => {
    // Strategies emit NaN (null over the wire) while rolling windows fill.
    // Counting those as transitions would litter the warm-up period.
    const signals = [
      signal(1, null),
      signal(2, null),
      signal(3, 1),
      signal(4, 1),
    ]
    const { buyCount, sellCount } = buildChartRows(BARS, signals)
    expect(buyCount).toBe(1)
    expect(sellCount).toBe(0)
  })

  it('reports nothing for a strategy that never leaves flat', () => {
    const signals = BARS.map((_, i) => signal(i + 1, 0))
    const { buyCount, sellCount } = buildChartRows(BARS, signals)
    expect(buyCount).toBe(0)
    expect(sellCount).toBe(0)
  })

  it('ignores signals whose date has no matching bar', () => {
    // A signal outside the plotted range must not throw or invent a row.
    const signals = [signal(1, 0), { ...signal(2, 1), time: '1999-01-01T00:00:00Z' }]
    const { rows, buyCount } = buildChartRows(BARS, signals)
    expect(rows).toHaveLength(10)
    expect(buyCount).toBe(0)
  })
})
