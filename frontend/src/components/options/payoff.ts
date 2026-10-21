/**
 * components/options/payoff.ts
 *
 * Profit at expiry of a multi-leg option position (decision 54,
 * research/option-payoff-2026-10-20.md). Pure arithmetic: no pricer, no
 * volatility.
 *
 * Profit is piecewise linear in the underlying and bends only at the strikes,
 * so breakevens and the extremes are solved exactly from S = 0, each strike
 * and the slope beyond the highest one, never by sampling a grid.
 */

export type LegKind = 'call' | 'put' | 'stock'
export type Side = 1 | -1

export interface Leg {
  kind: LegKind
  /** +1 bought, -1 sold. */
  side: Side
  /** Contracts for an option, shares for stock. */
  quantity: number
  /** Ignored for stock. */
  strike: number
  /** Per share: the option's premium, or the price the stock was bought or sold at. */
  premium: number
}

/** Shares per unit: 100 a contract, 1 a share. */
export const MULTIPLIER: Record<LegKind, number> = { call: 100, put: 100, stock: 1 }

const EPS = 1e-9

export function legValue(leg: Leg, s: number): number {
  if (leg.kind === 'call') return Math.max(s - leg.strike, 0)
  if (leg.kind === 'put') return Math.max(leg.strike - s, 0)
  return s
}

const units = (leg: Leg) => leg.side * leg.quantity * MULTIPLIER[leg.kind]

/** Dollars at expiry for the whole position, at underlying price s. */
export function profitAt(legs: Leg[], s: number): number {
  return legs.reduce((sum, leg) => sum + units(leg) * (legValue(leg, s) - leg.premium), 0)
}

/** Dollars of profit per $1 of the underlying above the highest strike. */
export function rightSlope(legs: Leg[]): number {
  return legs.reduce((sum, leg) => sum + (leg.kind === 'put' ? 0 : units(leg)), 0)
}

/** Positive: paid to open (a debit); negative: received (a credit). */
export function netPremium(legs: Leg[]): number {
  return legs.reduce((sum, leg) => sum + units(leg) * leg.premium, 0)
}

/** The strikes, in order, once each: the only places profit bends. */
export function kinks(legs: Leg[]): number[] {
  const strikes = legs.filter((l) => l.kind !== 'stock' && l.strike > 0).map((l) => l.strike)
  return [...new Set(strikes)].sort((a, b) => a - b)
}

export interface PayoffSummary {
  /** Underlying prices where profit is zero, ascending. */
  breakevens: number[]
  /** Infinity when profit rises without limit. */
  maxGain: number
  /** -Infinity when profit falls without limit. */
  maxLoss: number
  netPremium: number
}

export function analyse(legs: Leg[]): PayoffSummary {
  const points = [0, ...kinks(legs).filter((k) => k > 0)]
  const values = points.map((s) => profitAt(legs, s))
  const slope = rightSlope(legs)

  const found: number[] = []
  points.forEach((s, i) => {
    if (s > 0 && Math.abs(values[i]) < EPS) found.push(s)
  })
  for (let i = 0; i + 1 < points.length; i++) {
    const [a, b, pa, pb] = [points[i], points[i + 1], values[i], values[i + 1]]
    if ((pa < -EPS && pb > EPS) || (pa > EPS && pb < -EPS)) {
      found.push(a - (pa * (b - a)) / (pb - pa))
    }
  }
  const last = points[points.length - 1]
  const pLast = values[values.length - 1]
  if (Math.abs(slope) > EPS && Math.abs(pLast) > EPS && pLast * slope < 0) {
    found.push(last - pLast / slope)
  }
  const breakevens = found
    .sort((a, b) => a - b)
    .filter((s, i, all) => s > 0 && (i === 0 || s - all[i - 1] > EPS))

  return {
    breakevens,
    maxGain: slope > EPS ? Infinity : Math.max(...values),
    maxLoss: slope < -EPS ? -Infinity : Math.min(...values),
    netPremium: netPremium(legs),
  }
}

export interface PayoffPoint {
  x: number
  profit: number
}

/** How far the chart reaches past the outermost strike, breakeven or reference price. */
export const WINDOW_MARGIN = 0.15

/**
 * From 15% below the lowest strike, breakeven or reference price to 15% above
 * the highest: wide enough for every bend, narrow enough to see them.
 */
export function payoffWindow(legs: Leg[], reference: number): [number, number] {
  const marks = [reference, ...kinks(legs), ...analyse(legs).breakevens]
  return [Math.min(...marks) * (1 - WINDOW_MARGIN), Math.max(...marks) * (1 + WINDOW_MARGIN)]
}

/**
 * The curve across `payoffWindow`, for the chart: an even grid plus every
 * strike and breakeven, so each bend is drawn where it is.
 */
export function payoffCurve(legs: Leg[], reference: number, steps = 120): PayoffPoint[] {
  const [lo, hi] = payoffWindow(legs, reference)
  const xs = Array.from({ length: steps + 1 }, (_, i) => lo + ((hi - lo) * i) / steps)
  const exact = [...kinks(legs), ...analyse(legs).breakevens].filter((x) => x > lo && x < hi)
  return [...new Set([...xs, ...exact])]
    .sort((a, b) => a - b)
    .map((x) => ({ x, profit: profitAt(legs, x) }))
}
