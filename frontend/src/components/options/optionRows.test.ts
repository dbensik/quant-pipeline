/**
 * The Options page's pure row modules. What is plotted, where it sits on the
 * axis, and what the pricer starts from — the parts that can be wrong while
 * the page still looks right.
 */

import { describe, expect, it } from 'vitest'

import { surfaceFixture } from '@/test/optionsFixture'
import { buildConeRows, buildTermRows, CALENDAR_DAYS_PER_TRADING_DAY, termYDomain } from './coneRows'
import { defaultPricerExpiry, prefillFromExpiry } from './pricerPrefill'
import { buildSmileSeries, defaultExpiries, expiryFlags, vendorGap } from './smileRows'

const S = surfaceFixture()

describe('smile rows', () => {
  it('plots only our IV: no series point carries Yahoo’s', () => {
    const series = buildSmileSeries(S.smiles, S.expiries, ['2026-10-30'])
    for (const p of series[0].points) {
      expect(Object.keys(p).sort()).toEqual(['isCall', 'iv', 'strike', 'x'])
    }
    // And the plotted values are ours, not the vendor's.
    expect(series[0].points.map((p) => p.iv)).toEqual([0.16, 0.14, 0.11, 0.105])
  })

  it('sorts each smile by log-moneyness and labels it with its days', () => {
    const [s] = buildSmileSeries(S.smiles, S.expiries, ['2026-10-30'])
    expect(s.points.map((p) => p.x)).toEqual([-0.053, -0.026, 0.025, 0.05])
    expect(s.days).toBe(22)
  })

  it('defaults to expiries spread across the term, first and last included', () => {
    const many = Array.from({ length: 16 }, (_, i) => ({ ...S.expiries[0], expiry: `e${String(i).padStart(2, '0')}`, T: (i + 1) / 100 }))
    expect(defaultExpiries(many, 4)).toEqual(['e00', 'e05', 'e10', 'e15'])
    expect(defaultExpiries(S.expiries, 4)).toEqual(['2026-10-30', '2026-12-18', '2027-01-15'])
  })

  it('flags an expiry near an ex-date or with an unconverged forward', () => {
    expect(expiryFlags(S.expiries[0])).toEqual([])
    expect(expiryFlags(S.expiries[1])).toEqual(['near a projected ex-date'])
    expect(expiryFlags(S.expiries[2])).toEqual(['forward did not converge'])
  })

  it('summarises Yahoo’s disagreement as a median, skipping missing values', () => {
    // |0.14-0.15|, |0.16-0.19|, |0.105-0.12| -> 1, 3, 1.5 vol pts; median 1.5
    expect(vendorGap(S.smiles['2026-10-30'])).toBeCloseTo(1.5, 10)
    expect(vendorGap([])).toBeNull()
  })
})

describe('term structure and cone rows', () => {
  it('places a trading-day window at its calendar length', () => {
    const rows = buildConeRows(S.cone)
    expect(CALENDAR_DAYS_PER_TRADING_DAY).toBeCloseTo(365 / 252, 12)
    expect(rows.map((r) => r.window)).toEqual([10, 20, 60, 120])
    expect(rows[1].days).toBeCloseTo((20 * 365) / 252, 10)
    expect(rows[3].outer).toEqual([0.064, 0.48])
    expect(rows[3].inner).toEqual([0.116, 0.191])
  })

  it('keeps nulls null so the chart draws a gap, not a zero', () => {
    const term = buildTermRows(S.term_structure)
    expect(term[1].atmIv).toBeNull()
    expect(term[1].skew).toBeNull()
    expect(buildConeRows(S.cone)[2].current).toBeNull()
  })

  it('puts expiries on calendar days and returns no cone rows when there is no cone', () => {
    expect(buildTermRows(S.term_structure).map((t) => t.days)).toEqual([0.06 * 365, 0.19 * 365, 0.27 * 365])
    expect(buildConeRows(null)).toEqual([])
  })
})

describe('pricer prefill', () => {
  it('reproduces the expiry’s parity forward to a hundredth of a cent, after rounding', () => {
    const odd = { ...S.expiries[0], implied_spot: 773.3251840082949, T: 0.019206550260908166, forward: 773.9384 }
    for (const e of [...S.expiries, odd]) {
      const x = prefillFromExpiry(S, e)
      expect(x.spot).toBe(Number(e.implied_spot.toFixed(4)))
      expect(Math.abs(x.spot * Math.exp((x.rate - x.dividendYield) * x.expiryYears) - e.forward)).toBeLessThan(1e-4)
    }
    expect(String(prefillFromExpiry(S, odd).spot)).toBe('773.3252')
  })

  it('starts the pricer at the expiry nearest a month out', () => {
    expect(defaultPricerExpiry(S.expiries)?.expiry).toBe('2026-10-30')
    const spread = [7, 31, 90].map((d) => ({ ...S.expiries[0], expiry: `d${d}`, T: d / 365 }))
    expect(defaultPricerExpiry(spread)?.expiry).toBe('d31')
    expect(defaultPricerExpiry([])).toBeUndefined()
  })

  it('takes the ATM vol and the continuous rate, and leaves sigma empty when ATM is unknown', () => {
    expect(prefillFromExpiry(S, S.expiries[0])).toMatchObject({ rate: 0.0412, sigma: 0.12, strike: 780, expiryYears: 0.06 })
    expect(prefillFromExpiry(S, S.expiries[1]).sigma).toBeNull()
  })
})

describe('readability rules', () => {
  it('trims far wings by forward delta unless asked for them', () => {
    const smiles = {
      e: [
        { ...S.smiles['2026-10-30'][0], log_moneyness: -0.9, forward_delta: -0.004, iv: 0.9 },
        { ...S.smiles['2026-10-30'][0], log_moneyness: -0.05, forward_delta: -0.2, iv: 0.15 },
        { ...S.smiles['2026-10-30'][0], log_moneyness: -0.4, forward_delta: null, iv: 0.5 },
      ],
    }
    const exp = [{ ...S.expiries[0], expiry: 'e' }]
    expect(buildSmileSeries(smiles, exp, ['e'])[0].points.map((p) => p.x)).toEqual([-0.4, -0.05])
    expect(buildSmileSeries(smiles, exp, ['e'], 0)[0].points.map((p) => p.x)).toEqual([-0.9, -0.4, -0.05])
  })

  it('sizes the term chart to the ATM vols and the realised IQR, not the realised max', () => {
    const term = buildTermRows(S.term_structure)
    const cone = buildConeRows(S.cone)
    const [lo, hi] = termYDomain(term, cone)
    expect(lo).toBe(0)
    // Largest of ATM 0.137, p75 0.191, median 0.144, current 0.124 is 0.191;
    // x1.25 = 0.239, rounded up to 0.25. The 1.17 max is NOT in range.
    expect(hi).toBeCloseTo(0.25, 12)
    expect(termYDomain([], [])).toEqual([0, 1])
  })

  it('stretches the term chart when ATM vol sits above the realised band (TQQQ-like)', () => {
    const term = buildTermRows([{ ...S.term_structure[0], atm_iv: 0.55 }])
    const [, hi] = termYDomain(term, buildConeRows(S.cone))
    expect(hi).toBeCloseTo(0.7, 12) // 0.55 x 1.25 = 0.6875 -> 0.70
  })
})
