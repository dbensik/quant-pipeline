/**
 * components/charts/comparisonRows.ts
 *
 * Merges several equity curves into the row shape Recharts wants:
 * one object per timestamp, one key per series.
 *
 * Pure and separate from the component for the reason CLAUDE.md records:
 * charts render nothing under jsdom, so the only way to test this logic is to
 * keep it out of the chart.
 *
 * Phase 5 — React pages for the ported routers
 */

export interface Series {
  name: string
  points: { time: string; total: number }[]
}

export interface ComparisonRow {
  time: string
  [series: string]: string | number | null
}

/**
 * Union of every series' timestamps, sorted.
 *
 * A series with no value at a timestamp gets null rather than being omitted or
 * carried forward: Recharts breaks the line at null, which is honest, whereas
 * carrying the previous value invents a flat segment that reads as "the
 * strategy held" when in fact there was no data.
 */
export function comparisonRows(series: Series[]): ComparisonRow[] {
  const byTime = new Map<string, ComparisonRow>()

  for (const { name, points } of series) {
    for (const point of points) {
      const existing = byTime.get(point.time)
      if (existing) {
        existing[name] = point.total
      } else {
        byTime.set(point.time, { time: point.time, [name]: point.total })
      }
    }
  }

  const names = series.map((s) => s.name)
  return [...byTime.values()]
    .sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0))
    .map((row) => {
      const filled: ComparisonRow = { time: row.time }
      for (const name of names) filled[name] = row[name] ?? null
      return filled
    })
}

/**
 * Distinct colours, cycled. Ten is more strategies than the compare endpoint
 * accepts, so the cycle never actually repeats within one chart.
 */
export const SERIES_COLOURS = [
  '#2563eb',
  '#dc2626',
  '#16a34a',
  '#ca8a04',
  '#9333ea',
  '#0891b2',
  '#db2777',
  '#65a30d',
  '#ea580c',
  '#4f46e5',
]

export function colourFor(index: number): string {
  return SERIES_COLOURS[index % SERIES_COLOURS.length]
}
