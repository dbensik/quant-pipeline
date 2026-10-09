/**
 * components/options/smileRows.ts
 *
 * The smile chart's series, built from the surface. Pure, so what is plotted
 * — and what is NOT — is testable without Recharts.
 *
 * ONLY OUR IV IS PLOTTED. Yahoo's `vendor_iv` is wrong off the money (median
 * 75% on deep-ITM calls in the archive) and decision 3 of the plan keeps it
 * for comparison only. It never enters a series; `vendorGap` summarises the
 * disagreement as one number instead.
 *
 * Phase 4 of research/option-pricing-plan-2026-10-08.md
 */

import type { OptionsExpiry, OptionsSmilePoint } from '@/api/client'

export interface SmilePoint {
  /** ln(K / F): zero is the forward, negative is below it. */
  x: number
  iv: number
  strike: number
  isCall: boolean
}

export interface SmileSeries {
  expiry: string
  days: number
  points: SmilePoint[]
}

/** About `n` expiries spread evenly across the term, always first and last. */
export function defaultExpiries(expiries: OptionsExpiry[], n = 4): string[] {
  const sorted = [...expiries].sort((a, b) => a.T - b.T).map((e) => e.expiry)
  if (sorted.length <= n) return sorted
  const picks = new Set<number>()
  for (let i = 0; i < n; i++) picks.add(Math.round((i * (sorted.length - 1)) / (n - 1)))
  return [...picks].sort((a, b) => a - b).map((i) => sorted[i])
}

/**
 * Points beyond this forward delta are "far wings": a $300 SPY put at 90%
 * vol is a real quote, but plotted by default it squeezes the region near
 * the money, where the surface is used, into a corner. Shown on request.
 */
export const DEFAULT_MIN_ABS_DELTA = 0.02

export function buildSmileSeries(
  smiles: Record<string, OptionsSmilePoint[]>,
  expiries: OptionsExpiry[],
  selected: string[],
  minAbsDelta = DEFAULT_MIN_ABS_DELTA,
): SmileSeries[] {
  const tByExpiry = new Map(expiries.map((e) => [e.expiry, e.T]))
  return selected
    .filter((expiry) => smiles[expiry] !== undefined)
    .map((expiry) => ({
      expiry,
      days: Math.round((tByExpiry.get(expiry) ?? 0) * 365),
      points: smiles[expiry]
        .filter((p) => minAbsDelta <= 0 || p.forward_delta == null || Math.abs(p.forward_delta) >= minAbsDelta)
        .map((p) => ({ x: p.log_moneyness, iv: p.iv, strike: p.strike, isCall: p.is_call }))
        .sort((a, b) => a.x - b.x),
    }))
}

/** Why an expiry deserves a caution mark, if it does. */
export function expiryFlags(expiry: OptionsExpiry): string[] {
  const flags: string[] = []
  if (expiry.dividend_date_uncertain) flags.push('near a projected ex-date')
  if (!expiry.forward_converged) flags.push('forward did not converge')
  return flags
}

/**
 * Median |ours - Yahoo's| in vol points over the points where Yahoo gave a
 * value, or null when it gave none. For a one-line comparison, not a chart.
 */
export function vendorGap(points: OptionsSmilePoint[]): number | null {
  const gaps = points
    .filter((p) => p.vendor_iv !== null && p.vendor_iv !== undefined && p.vendor_iv > 0)
    .map((p) => Math.abs(p.iv - (p.vendor_iv as number)) * 100)
    .sort((a, b) => a - b)
  if (gaps.length === 0) return null
  const mid = Math.floor(gaps.length / 2)
  return gaps.length % 2 ? gaps[mid] : (gaps[mid - 1] + gaps[mid]) / 2
}
