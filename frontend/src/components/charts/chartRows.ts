/**
 * components/charts/chartRows.ts
 *
 * Turns bars + signals into the rows the price chart plots.
 *
 * Extracted from PriceChart so it can be tested without Recharts. This is
 * where two real bugs lived — marking every held bar instead of transitions,
 * and dropping the opening entry by seeding the scan with null — and neither
 * is testable through a chart that renders nothing in jsdom.
 *
 * Phase 4 — React frontend
 */

import type { OHLCVBar, SignalPoint } from '@/api/client'

export interface ChartRow {
  date: string
  close: number
  /** Set only on bars where the signal turns more long — drives buy markers. */
  buy?: number
  /** Set only on bars where the signal turns less long — drives sell markers. */
  sell?: number
}

export interface ChartRows {
  rows: ChartRow[]
  buyCount: number
  sellCount: number
}

export function buildChartRows(
  bars: OHLCVBar[],
  signals?: SignalPoint[],
): ChartRows {
  // Bars with a null close are real: the migration kept partial rows where
  // OHLC was incomplete. Dropping them avoids a line that dives to zero.
  const rows: ChartRow[] = bars
    .filter((bar) => bar.close != null)
    .map((bar) => ({
      date: bar.time.slice(0, 10),
      close: bar.close as number,
    }))

  let buyCount = 0
  let sellCount = 0

  if (!signals || signals.length === 0 || rows.length === 0) {
    return { rows, buyCount, sellCount }
  }

  const byDate = new Map(rows.map((row) => [row.date, row]))

  // Mark TRANSITIONS, not every bar holding a position. A strategy that goes
  // long once and holds emits +1 on hundreds of bars; plotting all of them
  // buries the price line under a solid band of markers. Only a bar where the
  // signal changes represents an actual trade decision.
  //
  // Seeded at 0, not null: the implicit opening state is flat, so a first
  // signal of +1 IS an entry. Seeding with null silently dropped it — Buy and
  // Hold reported "no signal changes" for a strategy that plainly makes one.
  let previous = 0
  for (const point of signals) {
    const value = point.signal
    if (value == null) continue // warm-up bars carry no decision

    if (value !== previous) {
      const row = byDate.get(point.time.slice(0, 10))
      if (row) {
        if (value > previous) {
          row.buy = row.close
          buyCount += 1
        } else {
          row.sell = row.close
          sellCount += 1
        }
      }
    }
    previous = value
  }

  return { rows, buyCount, sellCount }
}
