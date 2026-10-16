/**
 * components/charts/equityRows.ts
 * A backtest's equity curve with buy and hold beside it, one row per date.
 *
 * Pure, for the reason frontend/CLAUDE.md gives: charts render nothing under
 * jsdom, so the merge is tested here rather than through the chart.
 */

export interface EquityPoint {
  time: string
  total: number
}

export interface EquityRow {
  date: string
  total: number
  benchmark: number | null
}

/**
 * Rows on the strategy's dates. A benchmark value missing on a date is null,
 * not carried forward: Recharts breaks the line there, which is honest.
 */
export function equityRows(points: EquityPoint[], benchmark?: EquityPoint[]): EquityRow[] {
  const byDate = new Map((benchmark ?? []).map((p) => [p.time.slice(0, 10), p.total]))
  return points.map((p) => {
    const date = p.time.slice(0, 10)
    return { date, total: p.total, benchmark: byDate.get(date) ?? null }
  })
}
