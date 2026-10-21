/**
 * payoff.ts (decision 54): each named structure against hand-computed profits
 * at three underlying prices, its breakevens and its extremes. Premiums are
 * per share; every option is 100 shares.
 */

import { describe, expect, it } from 'vitest'

import type { Leg, LegKind, Side } from './payoff'
import { analyse, payoffCurve, payoffWindow, profitAt } from './payoff'

const opt = (side: Side, kind: LegKind, strike: number, premium: number, quantity = 1): Leg => ({
  side,
  kind,
  strike,
  premium,
  quantity,
})
const shares = (price: number): Leg => ({
  side: 1,
  kind: 'stock',
  strike: 0,
  premium: price,
  quantity: 100,
})

interface Case {
  name: string
  legs: Leg[]
  at: [number, number][]
  breakevens: number[]
  maxGain: number
  maxLoss: number
}

const CASES: Case[] = [
  {
    name: 'long call',
    legs: [opt(1, 'call', 100, 5)],
    at: [
      [90, -500],
      [100, -500],
      [120, 1500],
    ],
    breakevens: [105],
    maxGain: Infinity,
    maxLoss: -500,
  },
  {
    name: 'short call',
    legs: [opt(-1, 'call', 100, 5)],
    at: [
      [90, 500],
      [100, 500],
      [120, -1500],
    ],
    breakevens: [105],
    maxGain: 500,
    maxLoss: -Infinity,
  },
  {
    name: 'long put',
    legs: [opt(1, 'put', 100, 4)],
    at: [
      [80, 1600],
      [100, -400],
      [120, -400],
    ],
    breakevens: [96],
    maxGain: 9600,
    maxLoss: -400,
  },
  {
    // Bounded: the underlying stops at zero.
    name: 'short put',
    legs: [opt(-1, 'put', 100, 4)],
    at: [
      [80, -1600],
      [100, 400],
      [120, 400],
    ],
    breakevens: [96],
    maxGain: 400,
    maxLoss: -9600,
  },
  {
    name: 'bull call spread',
    legs: [opt(1, 'call', 95, 7), opt(-1, 'call', 105, 2)],
    at: [
      [90, -500],
      [100, 0],
      [110, 500],
    ],
    breakevens: [100],
    maxGain: 500,
    maxLoss: -500,
  },
  {
    name: 'bear put spread',
    legs: [opt(1, 'put', 105, 7), opt(-1, 'put', 95, 2)],
    at: [
      [90, 500],
      [100, 0],
      [110, -500],
    ],
    breakevens: [100],
    maxGain: 500,
    maxLoss: -500,
  },
  {
    name: 'bull put spread',
    legs: [opt(-1, 'put', 100, 4), opt(1, 'put', 95, 2)],
    at: [
      [90, -300],
      [97, -100],
      [110, 200],
    ],
    breakevens: [98],
    maxGain: 200,
    maxLoss: -300,
  },
  {
    name: 'long straddle',
    legs: [opt(1, 'call', 100, 5), opt(1, 'put', 100, 4)],
    at: [
      [80, 1100],
      [100, -900],
      [120, 1100],
    ],
    breakevens: [91, 109],
    maxGain: Infinity,
    maxLoss: -900,
  },
  {
    name: 'long strangle',
    legs: [opt(1, 'put', 95, 2), opt(1, 'call', 105, 3)],
    at: [
      [80, 1000],
      [100, -500],
      [120, 1000],
    ],
    breakevens: [90, 110],
    maxGain: Infinity,
    maxLoss: -500,
  },
  {
    name: 'call butterfly',
    legs: [opt(1, 'call', 95, 8), opt(-1, 'call', 100, 5, 2), opt(1, 'call', 105, 3)],
    at: [
      [90, -100],
      [100, 400],
      [110, -100],
    ],
    breakevens: [96, 104],
    maxGain: 400,
    maxLoss: -100,
  },
  {
    name: 'iron condor',
    legs: [
      opt(1, 'put', 90, 1),
      opt(-1, 'put', 95, 2),
      opt(-1, 'call', 105, 2),
      opt(1, 'call', 110, 1),
    ],
    at: [
      [85, -300],
      [100, 200],
      [115, -300],
    ],
    breakevens: [93, 107],
    maxGain: 200,
    maxLoss: -300,
  },
  {
    name: 'covered call',
    legs: [shares(100), opt(-1, 'call', 105, 2)],
    at: [
      [80, -1800],
      [100, 200],
      [120, 700],
    ],
    breakevens: [98],
    maxGain: 700,
    maxLoss: -9800,
  },
  {
    name: 'collar',
    legs: [shares(100), opt(1, 'put', 95, 2), opt(-1, 'call', 105, 2)],
    at: [
      [80, -500],
      [100, 0],
      [120, 500],
    ],
    breakevens: [100],
    maxGain: 500,
    maxLoss: -500,
  },
]

describe.each(CASES)('$name', ({ legs, at, breakevens, maxGain, maxLoss }) => {
  it('profits as computed by hand', () => {
    for (const [s, profit] of at) expect(profitAt(legs, s)).toBeCloseTo(profit, 9)
  })

  it('has the breakevens and extremes', () => {
    const summary = analyse(legs)
    expect(summary.breakevens).toHaveLength(breakevens.length)
    summary.breakevens.forEach((b, i) => expect(b).toBeCloseTo(breakevens[i], 9))
    for (const b of summary.breakevens) expect(profitAt(legs, b)).toBeCloseTo(0, 9)
    expect(summary.maxGain).toBeCloseTo(maxGain, 9)
    expect(summary.maxLoss).toBeCloseTo(maxLoss, 9)
  })
})

describe('payoff', () => {
  it("is symmetric about a long straddle's strike", () => {
    const legs = [opt(1, 'call', 100, 6.3), opt(1, 'put', 100, 4.1)]
    for (const d of [0.5, 7, 30, 100]) {
      expect(profitAt(legs, 100 + d)).toBeCloseTo(profitAt(legs, 100 - d), 9)
    }
  })

  it('reports net premium as a debit when paid and a credit when received', () => {
    expect(analyse([opt(1, 'call', 95, 7), opt(-1, 'call', 105, 2)]).netPremium).toBeCloseTo(500)
    expect(analyse([opt(-1, 'put', 100, 4), opt(1, 'put', 95, 2)]).netPremium).toBeCloseTo(-200)
  })

  it('finds a breakeven between two strikes, not only beyond them', () => {
    // A ratio spread: long one 100 call, short two 110 calls. For a credit
    // of $1 it makes $11 at 110 and loses $1 a dollar above: one breakeven.
    const credit = [opt(1, 'call', 100, 5), opt(-1, 'call', 110, 3, 2)]
    expect(analyse(credit).breakevens).toEqual([121])
    expect(analyse(credit).maxGain).toBeCloseTo(1100, 9)
    // For a debit of $2 it loses $2 below 100: the first breakeven is between
    // the strikes, the second where the $8 at 110 runs out.
    const debit = [opt(1, 'call', 100, 8), opt(-1, 'call', 110, 3, 2)]
    const [first, second] = analyse(debit).breakevens
    expect(first).toBeCloseTo(102, 9)
    expect(second).toBeCloseTo(118, 9)
    expect(analyse(debit).maxLoss).toBe(-Infinity)
  })

  it('finds a breakeven where profit touches zero at a strike', () => {
    // Short stock at 105 and two 100 calls at 2.50: a straddle that only
    // breaks even at 100 itself.
    const legs = [{ ...shares(105), side: -1 as Side }, opt(1, 'call', 100, 2.5, 2)]
    expect(profitAt(legs, 90)).toBeCloseTo(1000, 9)
    expect(profitAt(legs, 110)).toBeCloseTo(1000, 9)
    expect(analyse(legs).breakevens).toEqual([100])
    expect(analyse(legs).maxLoss).toBeCloseTo(0, 9)
  })

  it('draws every strike and breakeven inside the window exactly', () => {
    const legs = [opt(1, 'call', 95, 8), opt(-1, 'call', 100, 5, 2), opt(1, 'call', 105, 3)]
    const xs = payoffCurve(legs, 100).map((p) => p.x)
    for (const x of [95, 96, 100, 104, 105]) expect(xs).toContain(x)
    expect(xs[0]).toBeCloseTo(95 * 0.85, 9)
    expect(xs[xs.length - 1]).toBeCloseTo(105 * 1.15, 9)
    expect(xs).toEqual([...xs].sort((a, b) => a - b))
  })

  it('reaches 15% past the outermost strike, breakeven or reference price', () => {
    // A straddle near the money: the breakevens are the outermost marks.
    const straddle = [opt(1, 'call', 100, 6), opt(1, 'put', 100, 4)]
    expect(payoffWindow(straddle, 101)).toEqual([90 * 0.85, 110 * 1.15])
    // A reference outside every strike widens the window to include it.
    const [lo] = payoffWindow([opt(1, 'call', 100, 5)], 80)
    expect(lo).toBeCloseTo(80 * 0.85, 9)
  })

  it('has nothing to report for no legs', () => {
    expect(analyse([])).toEqual({ breakevens: [], maxGain: 0, maxLoss: 0, netPremium: 0 })
  })
})
