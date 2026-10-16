/**
 * lib/dateRange.ts
 * Quick ranges for the shared date range (Chart & Backtest, Compare).
 *
 * The default stays the last year, so a first page load is fast; but stored
 * history goes back to 2010 for equities and ETFs (decision 26), and one year
 * is too short to judge most strategies — a 200-day rule spends most of it
 * warming up. "Max" asks from 2010-01-01; the API returns what exists.
 */

export type PresetKey = '1Y' | '5Y' | '10Y' | 'Max'

export const PRESETS: PresetKey[] = ['1Y', '5Y', '10Y', 'Max']

/** Where stored history begins (decision 26). */
export const HISTORY_START = '2010-01-01'

function isoYearsBefore(endIso: string, years: number): string {
  const d = new Date(`${endIso}T00:00:00Z`)
  d.setUTCFullYear(d.getUTCFullYear() - years)
  return d.toISOString().slice(0, 10)
}

export function presetRange(key: PresetKey, todayIso: string): { start: string; end: string } {
  const years = { '1Y': 1, '5Y': 5, '10Y': 10 } as const
  return {
    start: key === 'Max' ? HISTORY_START : isoYearsBefore(todayIso, years[key]),
    end: todayIso,
  }
}

/** The preset a range matches, if any — so its button can show as selected. */
export function matchingPreset(start: string, end: string, todayIso: string): PresetKey | null {
  return (
    PRESETS.find((k) => {
      const r = presetRange(k, todayIso)
      return r.start === start && r.end === end
    }) ?? null
  )
}

export function todayIso(): string {
  return new Date().toISOString().slice(0, 10)
}
