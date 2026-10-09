/**
 * components/options/coneRows.ts
 *
 * The ATM term structure and the realised-vol cone on ONE x-axis.
 *
 * TWO CLOCKS. Option time is calendar days (ACT/365): an expiry 30 days out
 * is plotted at 30. The cone's windows are TRADING days (10, 20, 60, 120 of
 * them), annualised over 252. A 20-trading-day window spans 20 x 365/252 =
 * 29 calendar days, so that is where it sits on the axis. The vols
 * themselves are both annualised and directly comparable; only the x
 * position needs converting.
 *
 * Nulls stay null so the chart draws a gap: an ATM vol that could not be
 * interpolated is not zero.
 */

import type { OptionsTermPoint, OptionsVolCone } from '@/api/client'

export const CALENDAR_DAYS_PER_TRADING_DAY = 365 / 252

export interface TermRow {
  days: number
  expiry: string
  atmIv: number | null
  skew: number | null
  flagged: boolean
}

export interface ConeRow {
  days: number
  window: number
  min: number
  p25: number
  median: number
  p75: number
  max: number
  current: number | null
  outer: [number, number]
  inner: [number, number]
}

export function buildTermRows(term: OptionsTermPoint[]): TermRow[] {
  return [...term]
    .sort((a, b) => a.T - b.T)
    .map((t) => ({
      days: t.T * 365,
      expiry: t.expiry,
      atmIv: t.atm_iv ?? null,
      skew: t.skew_25d ?? null,
      flagged: t.dividend_date_uncertain,
    }))
}

/**
 * The y-range that keeps what the panel is for readable: the ATM vols, the
 * realised interquartile band, the median and today's value. The min-max
 * band is NOT in it — SPY's 10-day max is 117% (March 2020) and would flatten
 * everything else; it is drawn and simply runs off the top.
 */
export function termYDomain(term: TermRow[], cone: ConeRow[]): [number, number] {
  const values = [
    ...term.map((t) => t.atmIv),
    ...cone.flatMap((c) => [c.p75, c.median, c.current]),
  ].filter((v): v is number => v !== null && Number.isFinite(v))
  if (values.length === 0) return [0, 1]
  const top = Math.max(...values) * 1.25
  return [0, Math.ceil(top / 0.05) * 0.05]
}

export function buildConeRows(cone: OptionsVolCone | null | undefined): ConeRow[] {
  if (!cone) return []
  return [...cone.windows]
    .sort((a, b) => a.window - b.window)
    .map((w) => ({
      days: w.window * CALENDAR_DAYS_PER_TRADING_DAY,
      window: w.window,
      min: w.min,
      p25: w.p25,
      median: w.median,
      p75: w.p75,
      max: w.max,
      current: w.current ?? null,
      outer: [w.min, w.max],
      inner: [w.p25, w.p75],
    }))
}
