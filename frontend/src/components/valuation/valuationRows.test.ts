/**
 * The Valuation page's pure rows, on real API responses (Apple and Oracle as
 * of 2026-10-08, captured from the running server). What can be wrong while
 * the page still looks right: a negative value shown as a price, a bridge
 * that does not add up, a grid with the base case in the wrong cell, a
 * filing that could not be valued drawn as zero.
 */

import { describe, expect, it } from 'vitest'

import { AAPL_HISTORY, AAPL_VALUATION, ORCL_VALUATION } from '@/test/valuationFixture'
import { bridgeRows, formatMoney, gridRows, historyRows, inputLines, parseOverride, verdict } from './valuationRows'

describe('verdict', () => {
  it('a positive value is a price with its upside', () => {
    const v = verdict(AAPL_VALUATION)
    expect(v.kind).toBe('value')
    if (v.kind === 'value') expect(v.upside).toBeCloseTo(AAPL_VALUATION.result.per_share! / AAPL_VALUATION.price - 1, 12)
  })

  it('negative free cash flow is explained, never shown as a price', () => {
    // Oracle's TTM free cash flow is negative (AI capex above operating cash flow).
    expect(ORCL_VALUATION.result.inputs.base_fcf).toBeLessThan(0)
    const v = verdict(ORCL_VALUATION)
    expect(v.kind).toBe('negative')
    if (v.kind === 'negative') expect(v.why).toMatch(/nothing to grow/)
  })

  it('no value at all says why', () => {
    const none = { ...AAPL_VALUATION, result: { ...AAPL_VALUATION.result, per_share: null, reason: 'wacc 0.0300 <= terminal growth 0.0300: no Gordon value' } }
    const v = verdict(none)
    expect(v.kind).toBe('none')
    if (v.kind === 'none') expect(v.why).toMatch(/terminal growth/)
  })
})

describe('bridge', () => {
  it('adds up: PVs to EV, less net debt to equity, divided by shares to the per-share value', () => {
    const r = AAPL_VALUATION.result
    const rows = bridgeRows(AAPL_VALUATION)
    const flows = rows.filter((x) => x.kind === 'flow').reduce((a, x) => a + x.value, 0)
    const ev = rows.find((x) => x.label === 'Enterprise value')!.value
    const equity = rows.find((x) => x.label === 'Equity value')!.value
    const adjust = rows.find((x) => x.kind === 'adjust')!
    expect(flows).toBeCloseTo(ev, 0)
    expect(ev + adjust.value).toBeCloseTo(equity, 0)
    expect(equity / r.inputs.shares).toBeCloseTo(rows.at(-1)!.value, 9)
    // Apple holds net cash: the bridge says "plus", not "less".
    expect(r.inputs.net_debt).toBeLessThan(0)
    expect(adjust.label).toBe('Plus net cash')
    expect(rows.filter((x) => x.kind === 'flow')).toHaveLength(r.inputs.years + 1)
  })

  it('takes preferred stock off between enterprise and equity value, and only when there is some', () => {
    const r = AAPL_VALUATION.result
    const pref = 5e9
    const v = {
      ...AAPL_VALUATION,
      result: { ...r, inputs: { ...r.inputs, preferred: pref }, equity_value: r.enterprise_value! - r.inputs.net_debt - pref },
    } as typeof AAPL_VALUATION
    const rows = bridgeRows(v)
    const adjusts = rows.filter((x) => x.kind === 'adjust')
    expect(adjusts.map((x) => x.label)).toEqual(['Plus net cash', 'Less preferred stock'])
    const ev = rows.find((x) => x.label === 'Enterprise value')!.value
    const equity = rows.find((x) => x.label === 'Equity value')!.value
    expect(ev + adjusts.reduce((a, x) => a + x.value, 0)).toBeCloseTo(equity, 0)
    expect(bridgeRows(AAPL_VALUATION).filter((x) => x.kind === 'adjust')).toHaveLength(1)
  })

  it('is empty when there is no value', () => {
    expect(bridgeRows({ ...AAPL_VALUATION, result: { ...AAPL_VALUATION.result, per_share: null } })).toEqual([])
  })
})

describe('sensitivity grid', () => {
  it('marks the base case in the middle, at the inputs actually used', () => {
    const rows = gridRows(AAPL_VALUATION.sensitivity, AAPL_VALUATION.price)
    const base = rows.flatMap((r) => r.cells.map((c) => ({ ...c, wacc: r.wacc }))).filter((c) => c.base)
    expect(base).toHaveLength(1)
    expect(base[0].wacc).toBeCloseTo(AAPL_VALUATION.result.inputs.wacc, 12)
    expect(base[0].growth).toBeCloseTo(AAPL_VALUATION.result.inputs.growth_terminal, 12)
    expect(base[0].value).toBeCloseTo(AAPL_VALUATION.result.per_share!, 6)
  })

  it('a cell without a value has no upside, not a zero', () => {
    const grid = { wacc: [0.03], growth_terminal: [0.03], per_share: [[null]] }
    expect(gridRows(grid, 100)[0].cells[0]).toEqual({ growth: 0.03, value: null, upside: null, base: true })
  })
})

describe('history', () => {
  it('keeps priced filings, gaps where there is no value, and counts why the rest were skipped', () => {
    const { rows, skipped } = historyRows(AAPL_HISTORY.points)
    const priced = AAPL_HISTORY.points.filter((p) => p.price !== null)
    expect(rows).toHaveLength(priced.length)
    expect(rows.every((r) => r.price !== null)).toBe(true)
    const unvalued = AAPL_HISTORY.points.length - priced.length
    expect(skipped.reduce((a, [, n]) => a + n, 0)).toBe(unvalued)
    const gap = { as_of: '2020-01-01', price: 75, per_share: null, reason: 'x' }
    expect(historyRows([gap]).rows[0].value).toBeNull()
  })
})

describe('inputs and form', () => {
  it('every input line carries the source the API reported', () => {
    const lines = inputLines(AAPL_VALUATION)
    expect(lines.map((l) => l.key)).toContain('base_fcf')
    for (const l of lines) expect(l.source).toBe(AAPL_VALUATION.lines[l.key].source)
    expect(lines.find((l) => l.key === 'beta')!.source).toMatch(/weekly 5y/)
  })

  it('shows the noncontrolling interests deducted, after stock-based compensation', () => {
    const v = {
      ...AAPL_VALUATION,
      lines: {
        ...AAPL_VALUATION.lines,
        noncontrolling_interests: { value: 332e6, source: "noncontrolling interests' share of income, TTM to 2026-06-30; subtracted" },
      },
    } as typeof AAPL_VALUATION
    const keys = inputLines(v).map((l) => l.key)
    expect(keys.indexOf('noncontrolling_interests')).toBe(keys.indexOf('share_based_comp') + 1)
    const row = inputLines(v).find((l) => l.key === 'noncontrolling_interests')!
    expect(row.display).toBe('$332.0M')
    expect(row.source).toMatch(/subtracted$/)
  })

  it('parses percentages as percent and ignores blanks', () => {
    expect(parseOverride('8.5', true)).toBeCloseTo(0.085, 12)
    expect(parseOverride('1.2', false)).toBe(1.2)
    expect(parseOverride('', true)).toBeUndefined()
    expect(parseOverride('abc', false)).toBeUndefined()
  })

  it('formats money with sign and scale', () => {
    expect(formatMoney(-62.22e9)).toBe('−$62.22B')
    expect(formatMoney(4.97e12)).toBe('$4.97T')
  })
})
