import { describe, expect, it } from 'vitest'

import type { OHLCVBar } from '@/api/client'
import { buildCandlestickSeries } from './candlestickData'

function bar(
  day: number,
  values: Partial<Pick<OHLCVBar, 'open' | 'high' | 'low' | 'close'>> = {},
): OHLCVBar {
  const date = String(day).padStart(2, '0')
  return {
    time: `2024-01-${date}T00:00:00Z`,
    open: 100,
    high: 105,
    low: 95,
    close: 102,
    volume: 1000,
    ...values,
  }
}

describe('buildCandlestickSeries — parallel arrays', () => {
  it('splits bars into the arrays Plotly expects', () => {
    const series = buildCandlestickSeries([bar(1), bar(2)])
    expect(series.x).toEqual(['2024-01-01', '2024-01-02'])
    expect(series.open).toEqual([100, 100])
    expect(series.high).toEqual([105, 105])
    expect(series.low).toEqual([95, 95])
    expect(series.close).toEqual([102, 102])
  })

  it('keeps every array the same length', () => {
    // Plotly pairs these by index; a ragged set silently misaligns candles
    // against their dates.
    const series = buildCandlestickSeries([
      bar(1),
      bar(2, { high: null }),
      bar(3),
    ])
    const lengths = [
      series.x.length,
      series.open.length,
      series.high.length,
      series.low.length,
      series.close.length,
    ]
    expect(new Set(lengths).size).toBe(1)
  })

  it('preserves input order', () => {
    const series = buildCandlestickSeries([bar(3), bar(1), bar(2)])
    expect(series.x).toEqual(['2024-01-03', '2024-01-01', '2024-01-02'])
  })

  it('handles an empty input', () => {
    const series = buildCandlestickSeries([])
    expect(series.x).toEqual([])
    expect(series.droppedIncomplete).toBe(0)
  })
})

describe('buildCandlestickSeries — incomplete bars', () => {
  it('drops a bar that has a close but no open/high/low', () => {
    // NOT hypothetical: 481 rows in the migrated data are exactly this shape.
    // A line chart plots them (it needs only close); a candle cannot be drawn
    // from them at all.
    const series = buildCandlestickSeries([
      bar(1),
      bar(2, { open: null, high: null, low: null }),
      bar(3),
    ])
    expect(series.x).toEqual(['2024-01-01', '2024-01-03'])
    expect(series.droppedIncomplete).toBe(1)
  })

  it.each(['open', 'high', 'low', 'close'] as const)(
    'drops a bar missing %s',
    (field) => {
      const series = buildCandlestickSeries([bar(1, { [field]: null })])
      expect(series.x).toEqual([])
      expect(series.droppedIncomplete).toBe(1)
    },
  )

  it('reports how many were dropped', () => {
    // The count is surfaced in the UI. Without it, a candlestick view showing
    // fewer bars than the line view of the same range looks like a bug.
    const series = buildCandlestickSeries([
      bar(1),
      bar(2, { open: null }),
      bar(3, { low: null }),
      bar(4),
    ])
    expect(series.x).toHaveLength(2)
    expect(series.droppedIncomplete).toBe(2)
  })

  it('drops non-finite values', () => {
    // JSON has no NaN/Infinity, but a bad transform upstream could produce
    // them, and Plotly renders them as gaps rather than erroring.
    const series = buildCandlestickSeries([
      bar(1, { high: Number.NaN }),
      bar(2, { low: Number.POSITIVE_INFINITY }),
    ])
    expect(series.x).toEqual([])
    expect(series.droppedIncomplete).toBe(2)
  })

  it('keeps a bar whose values are legitimately zero', () => {
    // 0 is falsy. A truthiness check instead of a null check would discard
    // real bars — and crypto pairs do trade at sub-cent prices that round
    // toward it.
    const series = buildCandlestickSeries([
      bar(1, { open: 0, high: 0, low: 0, close: 0 }),
    ])
    expect(series.x).toHaveLength(1)
    expect(series.droppedIncomplete).toBe(0)
    expect(series.open).toEqual([0])
  })
})
