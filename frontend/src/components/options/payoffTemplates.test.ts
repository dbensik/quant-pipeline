/**
 * payoffTemplates.ts (decision 54): the strikes a template starts at, and the
 * premium a leg would have cost in a capture.
 */

import { describe, expect, it } from 'vitest'

import type { OptionsQuote } from '@/api/client'

import { analyse } from './payoff'
import { legsOf, nearestStrike, quotedPremium, strikeSpacing, TEMPLATES } from './payoffTemplates'

const QUOTES: OptionsQuote[] = [
  { strike: 745, call_bid: 25.1, call_ask: 25.5, put_bid: 2.0, put_ask: 2.1 },
  { strike: 750, call_bid: 21.0, call_ask: 21.2, put_bid: 2.9, put_ask: 3.0 },
  { strike: 770, call_bid: 6.0, call_ask: 6.2, put_bid: 7.1, put_ask: 7.3 },
  { strike: 790, call_bid: 0.0, call_ask: 0.4, put_bid: null, put_ask: null },
  { strike: 805, call_bid: 0.0, call_ask: 0.0, put_bid: 40, put_ask: 41 },
]
const STRIKES = QUOTES.map((q) => q.strike)
const template = (id: string) => TEMPLATES.find((t) => t.id === id)!

describe('templates', () => {
  it('start at the listed strikes nearest the reference and 2.5% steps from it', () => {
    // 768 x 0.975 = 748.8 -> 750; x 1.025 = 787.2 -> 790; x 1.05 = 806.4 -> 805.
    const legs = legsOf(template('iron_condor'), 768, STRIKES)
    expect(legs.map((l) => [l.side, l.kind, l.strike])).toEqual([
      [1, 'put', 745],
      [-1, 'put', 750],
      [-1, 'call', 790],
      [1, 'call', 805],
    ])
  })

  it('round to a spacing of about 0.5% when no strikes are listed', () => {
    expect(strikeSpacing(768)).toBe(5)
    expect(strikeSpacing(100)).toBe(0.5)
    expect(strikeSpacing(42)).toBe(0.25)
    expect(nearestStrike(100 * 1.025, [], 100)).toBe(102.5)
    expect(legsOf(template('long_strangle'), 100, []).map((l) => l.strike)).toEqual([97.5, 102.5])
  })

  it('give a covered call 100 shares against each contract', () => {
    const legs = legsOf(template('covered_call'), 100, [])
    expect(legs[0]).toMatchObject({ kind: 'stock', side: 1, quantity: 100 })
    expect(legs[1]).toMatchObject({ kind: 'call', side: -1, quantity: 1 })
  })

  it('have the shape their names say', () => {
    const priced = (id: string) =>
      analyse(
        legsOf(template(id), 100, []).map((l) => ({ ...l, premium: l.kind === 'stock' ? 100 : 1 })),
      )
    expect(priced('long_call').maxGain).toBe(Infinity)
    expect(priced('short_call').maxLoss).toBe(-Infinity)
    expect(priced('long_straddle').breakevens).toHaveLength(2)
    expect(priced('short_strangle').maxLoss).toBe(-Infinity)
    for (const id of [
      'bull_call_spread',
      'bear_put_spread',
      'call_butterfly',
      'put_butterfly',
      'iron_condor',
      'collar',
      'covered_call',
    ]) {
      expect(Number.isFinite(priced(id).maxGain), id).toBe(true)
      expect(Number.isFinite(priced(id).maxLoss), id).toBe(true)
    }
    expect(TEMPLATES.map((t) => t.id)).not.toContain('calendar')
  })
})

describe('quotedPremium', () => {
  it('pays the ask to buy and receives the bid to sell', () => {
    expect(quotedPremium({ kind: 'call', side: 1, strike: 770 }, QUOTES, 768, 'cross')).toBe(6.2)
    expect(quotedPremium({ kind: 'call', side: -1, strike: 770 }, QUOTES, 768, 'cross')).toBe(6.0)
    expect(quotedPremium({ kind: 'put', side: 1, strike: 770 }, QUOTES, 768, 'cross')).toBe(7.3)
    expect(quotedPremium({ kind: 'put', side: -1, strike: 770 }, QUOTES, 768, 'cross')).toBe(7.1)
  })

  it('takes the mid when asked', () => {
    expect(quotedPremium({ kind: 'put', side: 1, strike: 750 }, QUOTES, 768, 'mid')).toBeCloseTo(
      2.95,
    )
  })

  it('sells at a zero bid, but will not buy at a zero ask', () => {
    expect(quotedPremium({ kind: 'call', side: -1, strike: 790 }, QUOTES, 768, 'cross')).toBe(0)
    expect(quotedPremium({ kind: 'call', side: 1, strike: 805 }, QUOTES, 768, 'cross')).toBeNull()
    expect(quotedPremium({ kind: 'call', side: 1, strike: 805 }, QUOTES, 768, 'mid')).toBeNull()
  })

  it('has no premium for a strike or right not quoted', () => {
    expect(quotedPremium({ kind: 'put', side: 1, strike: 790 }, QUOTES, 768, 'cross')).toBeNull()
    expect(quotedPremium({ kind: 'call', side: 1, strike: 760 }, QUOTES, 768, 'cross')).toBeNull()
  })

  it('prices stock at the capture spot', () => {
    expect(quotedPremium({ kind: 'stock', side: 1, strike: 0 }, QUOTES, 768.46, 'cross')).toBe(
      768.46,
    )
  })
})
