/**
 * components/options/pricerPrefill.ts
 *
 * Starting inputs for the pricer, taken from one expiry of the surface.
 *
 * NO RECORDED SPOT. The surface deliberately has none (the capture reads
 * spot seconds away from each expiry's quotes). The spot is the expiry's
 * implied spot, and the continuous dividend yield is whatever makes
 * S e^{(r - q)T} equal that expiry's parity forward, so the calculator
 * reproduces the market's forward exactly. The surface itself priced
 * DISCRETE projected dividends; a calculator with one continuous q is an
 * approximation of that, and the form says so.
 */

import type { OptionsExpiry, OptionsSurfaceResponse } from '@/api/client'

export interface PricerInputs {
  spot: number
  strike: number
  expiryYears: number
  rate: number
  dividendYield: number
  sigma: number | null
}

const round = (v: number, places: number) => Number(v.toFixed(places))

/**
 * Rounded for a form a person reads (773.3252, not 773.3251840082949), and
 * q is solved AFTER rounding S, T and r, so the forward still matches to
 * well under a cent.
 */
/** The expiry nearest a month out: the usual place to start. */
export function defaultPricerExpiry(expiries: OptionsExpiry[], targetDays = 30): OptionsExpiry | undefined {
  return [...expiries].sort((a, b) => Math.abs(a.T * 365 - targetDays) - Math.abs(b.T * 365 - targetDays))[0]
}

export function prefillFromExpiry(surface: OptionsSurfaceResponse, expiry: OptionsExpiry): PricerInputs {
  const r = round(surface.rate.continuous, 6)
  const S = round(expiry.implied_spot, 4)
  const T = round(expiry.T, 6)
  const q = round(r - Math.log(expiry.forward / S) / T, 8)
  const term = surface.term_structure.find((t) => t.expiry === expiry.expiry)
  return {
    spot: S,
    strike: Math.round(expiry.forward),
    expiryYears: T,
    rate: r,
    dividendYield: q,
    sigma: term?.atm_iv == null ? null : round(term.atm_iv, 4),
  }
}
