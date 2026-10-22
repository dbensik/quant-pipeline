import { describe, expect, it } from 'vitest'

import { sviFixture } from '@/test/optionsFixture'
import { creditsText, fitCurves, parameterCaution, rangesText, verdictLabel, verdictMeaning } from './sviRows'

describe('fitCurves', () => {
  it('clips each fitted curve to the range of the points the chart shows', () => {
    const { fits } = sviFixture()
    const out = fitCurves(fits, { '2026-10-30': { lo: -0.05, hi: 0.05 } })
    expect(Object.keys(out)).toEqual(['2026-10-30'])
    const xs = out['2026-10-30'].map((p) => p.x)
    expect(Math.min(...xs)).toBeGreaterThanOrEqual(-0.05)
    expect(Math.max(...xs)).toBeLessThanOrEqual(0.05)
    // The fixture's curve has points at -0.0375, 0 and 0.0375 inside that range.
    expect(xs).toHaveLength(3)
    expect(out['2026-10-30'][1]).toEqual({ x: 0, iv: 0.12 })
  })

  it('draws nothing for an expiry the chart does not show', () => {
    expect(fitCurves(sviFixture().fits, {})).toEqual({})
  })
})

describe('verdicts', () => {
  it('names whose arbitrage it is', () => {
    expect(verdictLabel('both')).toBe('fit and quotes')
    expect(verdictLabel('fit_only')).toBe('fit only')
    expect(verdictLabel('quotes_only')).toBe('quotes only')
    expect(verdictLabel('none')).toBe('none')
  })

  it('says a fit-only flag is not the market’s, and names the check', () => {
    expect(verdictMeaning('fit_only', 'calendar')).toMatch(/calendar.*not the market's/)
    expect(verdictMeaning('quotes_only', 'butterfly')).toMatch(/quotes show a butterfly.*fit smooths away/)
    expect(verdictMeaning('both', 'butterfly')).toMatch(/both/)
    expect(verdictMeaning('none', 'calendar')).toMatch(/^Neither/)
  })
})

describe('rangesText', () => {
  it('writes ranges in ln(K/F) with a true minus sign, a point once, or a dash', () => {
    expect(rangesText([])).toBe('—')
    expect(rangesText([{ low: -0.123, high: -0.05 }, { low: 0.3, high: 0.3 }])).toBe('−0.123 to −0.050; 0.300')
  })
})

describe('creditsText', () => {
  it('names each strike and right with the near bid above the far ask, or a dash', () => {
    expect(creditsText([])).toBe('—')
    expect(
      creditsText([
        { strike: 375, right: 'call', near_bid: 0.11, far_ask: 0.07, credit: 0.04 },
        { strike: 380, right: 'call', near_bid: 0.2, far_ask: 0.05, credit: 0.15 },
      ]),
    ).toBe('375 call: bid 0.11 > ask 0.07; 380 call: bid 0.20 > ask 0.05')
  })
})

describe('parameterCaution', () => {
  it('is null for a fit whose bottom is quoted and wings are within Lee', () => {
    expect(parameterCaution(sviFixture().fits[0])).toBeNull()
  })

  it('names each reason the parameters cannot be read one by one', () => {
    const [first, second] = sviFixture().fits
    expect(parameterCaution(second)).toMatch(/bottom m lies past the quotes/)
    expect(parameterCaution({ ...first, wings_within_lee: false })).toMatch(/Lee/)
    expect(parameterCaution({ ...second, wings_within_lee: false })).toMatch(/past the quotes.*; .*Lee/)
  })
})
