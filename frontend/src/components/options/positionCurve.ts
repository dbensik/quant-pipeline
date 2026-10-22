/**
 * components/options/positionCurve.ts
 *
 * The curve before expiry on the payoff page (decision 55,
 * research/option-greeks-2026-10-20.md): the request the page sends to
 * POST /api/v1/options/position-curve, and the chart rows it draws from the
 * answer. Pure, so it is tested without the API or a chart. Every price and
 * Greek is the API's; nothing is priced here.
 */

import type {
  OptionPositionCurveRequest,
  OptionPositionCurveResponse,
  OptionPositionPoint,
} from '@/api/client'

import type { Leg, PayoffPoint } from './payoff'

export type VolRule = 'sticky_strike' | 'sticky_moneyness'
export type Greek = 'delta' | 'gamma' | 'vega' | 'theta' | 'rho'

export const VOL_RULES: { id: VolRule; label: string }[] = [
  { id: 'sticky_strike', label: 'Sticky strike: each strike keeps its vol' },
  { id: 'sticky_moneyness', label: 'Sticky moneyness: vol follows the spot' },
]

/** Units match the API's: dollars for the whole position. */
export const GREEKS: { id: Greek; label: string; unit: string }[] = [
  { id: 'delta', label: 'Delta', unit: '$ per $1 of the underlying' },
  { id: 'gamma', label: 'Gamma', unit: 'delta per $1 of the underlying' },
  { id: 'vega', label: 'Vega', unit: '$ per vol point' },
  { id: 'theta', label: 'Theta', unit: '$ per calendar day' },
  { id: 'rho', label: 'Rho', unit: '$ per point of the rate' },
]

/** A priced leg and its own expiry; null is the page's. */
export interface DatedLeg extends Leg {
  expiry: string | null
}

const DAY_MS = 86_400_000

/** Calendar days from a to b, both ISO dates. */
export function daysBetween(a: string, b: string): number {
  return Math.round((Date.parse(b) - Date.parse(a)) / DAY_MS)
}

export function addDays(day: string, n: number): string {
  return new Date(Date.parse(day) + n * DAY_MS).toISOString().slice(0, 10)
}

/** The expiry each option leg trades, resolved; stock has none. */
export function legExpiries(legs: DatedLeg[], pageExpiry: string): string[] {
  return legs.filter((l) => l.kind !== 'stock').map((l) => l.expiry ?? pageExpiry)
}

/** True when every option leg expires on one date, so payoff.ts draws its profit at expiry. */
export function oneExpiry(legs: DatedLeg[], pageExpiry: string): boolean {
  return new Set(legExpiries(legs, pageExpiry)).size <= 1
}

/** The last date the slider reaches; the capture day for stock alone. */
export function lastExpiry(legs: DatedLeg[], pageExpiry: string, captureDay: string): string {
  return legExpiries(legs, pageExpiry).reduce((a, b) => (b > a ? b : a), captureDay)
}

export function curveRequest(args: {
  ticker: string
  day: string
  legs: DatedLeg[]
  pageExpiry: string
  asOf: string
  volRule: VolRule
  spot: number
  window: [number, number]
}): OptionPositionCurveRequest {
  return {
    ticker: args.ticker,
    day: args.day,
    as_of: args.asOf,
    vol_rule: args.volRule,
    spot: args.spot,
    low: args.window[0],
    high: args.window[1],
    legs: args.legs.map((l) =>
      l.kind === 'stock'
        ? { kind: 'stock', side: l.side, quantity: l.quantity, premium: l.premium }
        : {
            kind: l.kind,
            side: l.side,
            quantity: l.quantity,
            premium: l.premium,
            strike: l.strike,
            expiry: l.expiry ?? args.pageExpiry,
          },
    ),
  }
}

/** Profit on the API's grid, in the at-expiry chart's shape. */
export function curvePoints(curve: OptionPositionCurveResponse): PayoffPoint[] {
  return curve.points.map((p) => ({ x: p.spot, profit: p.profit }))
}

export interface GreekPoint {
  x: number
  /** The rule chosen. */
  value: number
  /** The other rule, on the same grid; absent while it loads. */
  other?: number
}

/** One Greek on the chosen rule's grid, beside the other rule's where the grids meet. */
export function greekPoints(
  chosen: OptionPositionCurveResponse,
  other: OptionPositionCurveResponse | undefined,
  greek: Greek,
): GreekPoint[] {
  const byspot = new Map<number, OptionPositionPoint>(other?.points.map((p) => [p.spot, p]))
  return chosen.points.map((p) => {
    const o = byspot.get(p.spot)
    return o ? { x: p.spot, value: p[greek], other: o[greek] } : { x: p.spot, value: p[greek] }
  })
}

/** The point at the reference spot, which the API always puts on its grid. */
export function atSpot(curve: OptionPositionCurveResponse): OptionPositionPoint | undefined {
  return curve.points.find((p) => p.spot === curve.spot)
}
