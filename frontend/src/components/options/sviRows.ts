/**
 * components/options/sviRows.ts
 *
 * What the Options page shows of the SVI fits and arbitrage checks (decision
 * 56, research/svi-surface-2026-10-20.md). Pure, so the wording of a flag —
 * whose arbitrage it is, the fit's or the market's — is testable.
 *
 * The page never fits: every curve and number comes from GET /options/svi.
 */

import type { OptionSVIFit, OptionSVIRange } from '@/api/client'

export type Verdict = OptionSVIFit['butterfly']['verdict']

export interface FitPoint {
  x: number
  iv: number
}

/**
 * Each selected expiry's fitted curve, clipped to [lo, hi] of the points the
 * chart shows, so a fit never stretches the axis past the plotted quotes.
 */
export function fitCurves(
  fits: OptionSVIFit[],
  shown: Record<string, { lo: number; hi: number }>,
): Record<string, FitPoint[]> {
  const out: Record<string, FitPoint[]> = {}
  for (const fit of fits) {
    const range = shown[fit.expiry]
    if (!range) continue
    out[fit.expiry] = fit.curve
      .filter((p) => p.log_moneyness >= range.lo && p.log_moneyness <= range.hi)
      .map((p) => ({ x: p.log_moneyness, iv: p.iv }))
  }
  return out
}

const WHOSE: Record<Verdict, string> = {
  none: 'none',
  both: 'fit and quotes',
  fit_only: 'fit only',
  quotes_only: 'quotes only',
}

/** Who shows the arbitrage: the label in a table cell. */
export function verdictLabel(verdict: Verdict): string {
  return WHOSE[verdict]
}

/** What the verdict means, for a cell's title. */
export function verdictMeaning(verdict: Verdict, check: 'butterfly' | 'calendar'): string {
  const what =
    check === 'butterfly'
      ? 'a butterfly arbitrage (negative density)'
      : 'a calendar arbitrage (total variance falling with time)'
  switch (verdict) {
    case 'none':
      return `Neither the fit nor the quotes show ${what}.`
    case 'both':
      return `The fit and the quotes both show ${what}.`
    case 'fit_only':
      return `The fit shows ${what} the quotes do not: it is the fit's, not the market's.`
    case 'quotes_only':
      return `The quotes show ${what} the fit smooths away.`
  }
}

/** "−0.123 to −0.050; 0.300" in ln(K/F), or a dash: SPY's $1 strikes are about 0.0013 apart. */
export function rangesText(ranges: OptionSVIRange[]): string {
  if (ranges.length === 0) return '—'
  const f = (v: number) => v.toFixed(3).replace('-', '−')
  return ranges.map((r) => (r.low === r.high ? f(r.low) : `${f(r.low)} to ${f(r.high)}`)).join('; ')
}

/** Why a fit's parameters should not be read one by one, or null when they can be. */
export function parameterCaution(fit: OptionSVIFit): string | null {
  const notes: string[] = []
  if (!fit.bottom_quoted)
    notes.push(
      "the smile's bottom m lies past the quotes, so b, rho and m trade off to draw the same curve",
    )
  if (!fit.wings_within_lee) notes.push('a wing rises faster than Lee’s bound allows beyond the quotes')
  return notes.length === 0 ? null : notes.join('; ')
}
