/**
 * components/simulate/fanRows.ts
 *
 * Turns the API's percentile bands plus the historical equity curve into the
 * rows the fan chart plots. Pure, so the alignment logic — the part that can
 * be wrong without anything looking wrong — is testable without Recharts.
 *
 * ALIGNMENT. Band step 0 is start equity on every path. In `returns` mode the
 * draws start at the strategy's first invested bar f = bars - resampled_from,
 * so step j corresponds to historical bar f + j - 1 (bar f - 1 is the last
 * all-cash bar, whose total is the starting capital). In `prices` mode
 * resampled_from = bars - 1, so f = 1 and step j is historical bar j: the
 * same formula. Beyond the history, or when no curve was returned, the
 * overlay is simply absent.
 *
 * Phase 4 of research/monte-carlo-plan-2026-10-07.md
 */

import type { FanBand } from '@/api/client'

export interface FanRow {
  step: number
  p05: number
  p25: number
  p50: number
  p75: number
  p95: number
  /** [p05, p95] — the outer band, drawn as a range area. */
  outer: [number, number]
  /** [p25, p75] — the inner band. */
  inner: [number, number]
  /** The realised equity at this step, where the history covers it. */
  historical?: number
}

export function buildFanRows(args: {
  bands: FanBand[]
  historical: Array<{ total: number }>
  bars: number
  resampledFrom: number
  initialCapital: number
}): FanRow[] {
  const { bands, historical, bars, resampledFrom, initialCapital } = args
  const firstInvested = bars - resampledFrom

  return bands.map((band) => {
    const row: FanRow = {
      step: band.step,
      p05: band.p05,
      p25: band.p25,
      p50: band.p50,
      p75: band.p75,
      p95: band.p95,
      outer: [band.p05, band.p95],
      inner: [band.p25, band.p75],
    }
    if (historical.length > 0) {
      if (band.step === 0) {
        row.historical = initialCapital
      } else {
        const index = firstInvested + band.step - 1
        if (index >= 0 && index < historical.length) {
          row.historical = historical[index].total
        }
      }
    }
    return row
  })
}
