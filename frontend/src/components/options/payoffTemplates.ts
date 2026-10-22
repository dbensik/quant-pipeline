/**
 * components/options/payoffTemplates.ts
 *
 * The named structures the payoff page starts from (decision 54), and the
 * premium each leg would have cost in a capture: a buy pays the ask and a
 * sale receives the bid, unless mid is chosen.
 *
 * Calendars (decision 55) sell the page's expiry and buy a later one at the
 * same strike. At the near expiry the far leg is still an option with a
 * price, so their profit comes from the curve before expiry, not payoff.ts.
 */

import type { OptionsQuote } from '@/api/client'

import type { LegKind, Side } from './payoff'

/** A template leg's strike is the one nearest reference x (1 + at x STRIKE_STEP). */
export const STRIKE_STEP = 0.025

interface TemplateLeg {
  kind: LegKind
  side: Side
  quantity: number
  at: number
  /** Expires on the far expiry, not the page's. */
  far?: boolean
}

export interface Template {
  id: string
  label: string
  legs: TemplateLeg[]
}

const leg = (side: Side, kind: LegKind, at = 0, quantity = 1): TemplateLeg => ({
  kind,
  side,
  quantity,
  at,
})
const stock = (side: Side): TemplateLeg => ({ kind: 'stock', side, quantity: 100, at: 0 })

export const TEMPLATES: Template[] = [
  { id: 'long_call', label: 'Long call', legs: [leg(1, 'call')] },
  { id: 'short_call', label: 'Short call', legs: [leg(-1, 'call')] },
  { id: 'long_put', label: 'Long put', legs: [leg(1, 'put')] },
  { id: 'short_put', label: 'Short put', legs: [leg(-1, 'put')] },
  { id: 'bull_call_spread', label: 'Bull call spread', legs: [leg(1, 'call'), leg(-1, 'call', 1)] },
  { id: 'bear_call_spread', label: 'Bear call spread', legs: [leg(-1, 'call'), leg(1, 'call', 1)] },
  { id: 'bull_put_spread', label: 'Bull put spread', legs: [leg(-1, 'put'), leg(1, 'put', -1)] },
  { id: 'bear_put_spread', label: 'Bear put spread', legs: [leg(1, 'put'), leg(-1, 'put', -1)] },
  { id: 'long_straddle', label: 'Long straddle', legs: [leg(1, 'call'), leg(1, 'put')] },
  { id: 'short_straddle', label: 'Short straddle', legs: [leg(-1, 'call'), leg(-1, 'put')] },
  { id: 'long_strangle', label: 'Long strangle', legs: [leg(1, 'put', -1), leg(1, 'call', 1)] },
  {
    id: 'short_strangle',
    label: 'Short strangle',
    legs: [leg(-1, 'put', -1), leg(-1, 'call', 1)],
  },
  {
    id: 'call_butterfly',
    label: 'Call butterfly',
    legs: [leg(1, 'call', -1), leg(-1, 'call', 0, 2), leg(1, 'call', 1)],
  },
  {
    id: 'put_butterfly',
    label: 'Put butterfly',
    legs: [leg(1, 'put', -1), leg(-1, 'put', 0, 2), leg(1, 'put', 1)],
  },
  {
    id: 'iron_condor',
    label: 'Iron condor',
    legs: [leg(1, 'put', -2), leg(-1, 'put', -1), leg(-1, 'call', 1), leg(1, 'call', 2)],
  },
  {
    id: 'call_calendar',
    label: 'Call calendar',
    legs: [leg(-1, 'call'), { ...leg(1, 'call'), far: true }],
  },
  {
    id: 'put_calendar',
    label: 'Put calendar',
    legs: [leg(-1, 'put'), { ...leg(1, 'put'), far: true }],
  },
  { id: 'covered_call', label: 'Covered call', legs: [stock(1), leg(-1, 'call', 1)] },
  { id: 'collar', label: 'Collar', legs: [stock(1), leg(1, 'put', -1), leg(-1, 'call', 1)] },
]

/** A round strike spacing for typed premiums: about 0.5% of the price, as 1, 2.5 or 5 x 10^k. */
export function strikeSpacing(reference: number): number {
  const raw = reference * 0.005
  const power = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2.5, 5, 10].find((m) => m * power >= raw) ?? 10
  return step * power
}

/** The listed strike nearest a target, or the target rounded to the spacing when none is listed. */
export function nearestStrike(target: number, strikes: number[], reference: number): number {
  if (strikes.length === 0) {
    const spacing = strikeSpacing(reference)
    return Number((Math.round(target / spacing) * spacing).toFixed(6))
  }
  return strikes.reduce((best, k) => (Math.abs(k - target) < Math.abs(best - target) ? k : best))
}

export interface DraftLeg {
  kind: LegKind
  side: Side
  quantity: number
  strike: number
  /** Null: the page's expiry. */
  expiry: string | null
}

/** A calendar's far leg: one month past the near expiry. */
export const FAR_EXPIRY_DAYS = 28

const DAY_MS = 86_400_000

/**
 * The first listed expiry at least FAR_EXPIRY_DAYS after `near`, else the
 * last one after it; null when nothing is listed after it. Expiries in
 * `avoid` (a projected ex-date near expiry: the surface may price their calls
 * on the wrong side of the dividend) are passed over while another will do.
 */
export function farExpiry(expiries: string[], near: string, avoid: string[] = []): string | null {
  const t = Date.parse(near)
  const pick = (listed: string[]) => {
    const later = [...listed].sort().filter((x) => Date.parse(x) > t)
    return later.find((x) => Date.parse(x) - t >= FAR_EXPIRY_DAYS * DAY_MS) ?? later.at(-1) ?? null
  }
  return pick(expiries.filter((x) => !avoid.includes(x))) ?? pick(expiries)
}

/** A far leg takes `far` (null when there is none: typed premiums, or no later expiry). */
export function legsOf(
  template: Template,
  reference: number,
  strikes: number[],
  far: string | null = null,
): DraftLeg[] {
  return template.legs.map((l) => ({
    expiry: l.far ? far : null,
    kind: l.kind,
    side: l.side,
    quantity: l.quantity,
    strike:
      l.kind === 'stock'
        ? 0
        : nearestStrike(reference * (1 + l.at * STRIKE_STEP), strikes, reference),
  }))
}

export type Basis = 'cross' | 'mid'

/**
 * What one share of the leg cost in the capture, or null when it has no
 * usable quote. A sale may receive a zero bid; a buy needs an ask above zero.
 */
export function quotedPremium(
  leg: Pick<DraftLeg, 'kind' | 'side' | 'strike'>,
  quotes: OptionsQuote[],
  spot: number | null,
  basis: Basis,
): number | null {
  if (leg.kind === 'stock') return spot
  const q = quotes.find((x) => x.strike === leg.strike)
  if (!q) return null
  const bid = leg.kind === 'call' ? q.call_bid : q.put_bid
  const ask = leg.kind === 'call' ? q.call_ask : q.put_ask
  const b = bid !== null && bid !== undefined && bid >= 0 ? bid : null
  const a = ask !== null && ask !== undefined && ask > 0 ? ask : null
  if (basis === 'mid') return b !== null && a !== null ? (b + a) / 2 : null
  return leg.side === 1 ? a : b
}
