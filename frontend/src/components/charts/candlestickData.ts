/**
 * components/charts/candlestickData.ts
 *
 * Turns bars into the four parallel arrays Plotly's candlestick trace expects.
 *
 * Pure and separate from the component for the same reason as chartRows.ts,
 * only more so: Plotly needs canvas APIs jsdom does not implement, so anything
 * left inside the component cannot be tested at all.
 *
 * Phase 4 — React frontend
 */

import type { OHLCVBar } from '@/api/client'

export interface CandlestickSeries {
  x: string[]
  open: number[]
  high: number[]
  low: number[]
  close: number[]
  /**
   * Bars dropped for want of a complete OHLC quartet.
   *
   * This is NOT hypothetical: 481 rows in the migrated data carry a close with
   * one or more of open/high/low missing. A line chart plots those happily —
   * it only needs close — but a candle cannot be drawn from them. Reporting
   * the count keeps a candlestick view that silently shows fewer bars than the
   * line view of the same range from looking like a rendering bug.
   */
  droppedIncomplete: number
}

/** True when every field a candle needs is present and finite. */
function isComplete(bar: OHLCVBar): boolean {
  return (
    bar.open != null &&
    bar.high != null &&
    bar.low != null &&
    bar.close != null &&
    Number.isFinite(bar.open) &&
    Number.isFinite(bar.high) &&
    Number.isFinite(bar.low) &&
    Number.isFinite(bar.close)
  )
}

export function buildCandlestickSeries(bars: OHLCVBar[]): CandlestickSeries {
  const series: CandlestickSeries = {
    x: [],
    open: [],
    high: [],
    low: [],
    close: [],
    droppedIncomplete: 0,
  }

  for (const bar of bars) {
    if (!isComplete(bar)) {
      series.droppedIncomplete += 1
      continue
    }
    series.x.push(bar.time.slice(0, 10))
    series.open.push(bar.open as number)
    series.high.push(bar.high as number)
    series.low.push(bar.low as number)
    series.close.push(bar.close as number)
  }

  return series
}
