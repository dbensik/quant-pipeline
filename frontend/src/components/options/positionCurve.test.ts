import { describe, expect, it } from 'vitest'

import type { OptionPositionCurveResponse } from '@/api/client'

import {
  addDays,
  atSpot,
  curvePoints,
  curveRequest,
  daysBetween,
  greekPoints,
  lastExpiry,
  oneExpiry,
} from './positionCurve'
import type { DatedLeg } from './positionCurve'

const CALENDAR: DatedLeg[] = [
  { kind: 'call', side: -1, quantity: 1, strike: 770, premium: 6.0, expiry: null },
  { kind: 'call', side: 1, quantity: 1, strike: 770, premium: 14.2, expiry: '2026-12-18' },
]
const COVERED: DatedLeg[] = [
  { kind: 'stock', side: 1, quantity: 100, strike: 0, premium: 768.5, expiry: null },
  { kind: 'call', side: -1, quantity: 1, strike: 790, premium: 1.0, expiry: null },
]

const point = (spot: number, delta: number) => ({
  spot,
  value: 0,
  profit: spot - 768.5,
  delta,
  gamma: delta / 10,
  vega: 1,
  theta: -1,
  rho: 0,
})

const curve = (spots: number[], delta: number): OptionPositionCurveResponse => ({
  ticker: 'SPY',
  day: '2026-10-20',
  as_of: '2026-10-20',
  last_expiry: '2026-11-20',
  vol_rule: 'sticky_strike',
  spot: 768.5,
  rate: 0.04,
  cache_hit: true,
  legs: [],
  points: spots.map((s) => point(s, delta)),
})

describe('dates', () => {
  it('count calendar days and add them back', () => {
    expect(daysBetween('2026-10-20', '2026-11-20')).toBe(31)
    expect(addDays('2026-10-20', 31)).toBe('2026-11-20')
    // Across the US clock change on 2026-11-01: still whole days.
    expect(daysBetween('2026-10-30', '2026-11-02')).toBe(3)
    expect(addDays('2026-10-31', 1)).toBe('2026-11-01')
  })

  it('end the slider at the last leg to expire', () => {
    expect(lastExpiry(CALENDAR, '2026-11-20', '2026-10-20')).toBe('2026-12-18')
    expect(lastExpiry(COVERED, '2026-11-20', '2026-10-20')).toBe('2026-11-20')
    expect(lastExpiry(COVERED.slice(0, 1), '2026-11-20', '2026-10-20')).toBe('2026-10-20')
  })

  it('know when every option leg shares one expiry', () => {
    expect(oneExpiry(CALENDAR, '2026-11-20')).toBe(false)
    expect(oneExpiry(CALENDAR, '2026-12-18')).toBe(true)
    expect(oneExpiry(COVERED, '2026-11-20')).toBe(true)
  })
})

describe('curveRequest', () => {
  it('dates every option leg and leaves stock without strike or expiry', () => {
    const body = curveRequest({
      ticker: 'SPY',
      day: '2026-10-20',
      legs: [...COVERED, CALENDAR[1]],
      pageExpiry: '2026-11-20',
      asOf: '2026-10-25',
      volRule: 'sticky_moneyness',
      spot: 768.5,
      window: [600, 900],
    })
    expect(body).toEqual({
      ticker: 'SPY',
      day: '2026-10-20',
      as_of: '2026-10-25',
      vol_rule: 'sticky_moneyness',
      spot: 768.5,
      low: 600,
      high: 900,
      legs: [
        { kind: 'stock', side: 1, quantity: 100, premium: 768.5 },
        { kind: 'call', side: -1, quantity: 1, premium: 1.0, strike: 790, expiry: '2026-11-20' },
        { kind: 'call', side: 1, quantity: 1, premium: 14.2, strike: 770, expiry: '2026-12-18' },
      ],
    })
  })
})

describe('rows', () => {
  it('draw profit on the API grid', () => {
    expect(curvePoints(curve([700, 768.5], 0.5))).toEqual([
      { x: 700, profit: -68.5 },
      { x: 768.5, profit: 0 },
    ])
  })

  it('pair the chosen Greek with the other rule spot by spot', () => {
    const rows = greekPoints(curve([700, 768.5], 50), curve([768.5, 800], 40), 'gamma')
    expect(rows).toEqual([
      { x: 700, value: 5 },
      { x: 768.5, value: 5, other: 4 },
    ])
    expect(greekPoints(curve([700], 50), undefined, 'delta')).toEqual([{ x: 700, value: 50 }])
  })

  it('find the reference spot on the grid', () => {
    expect(atSpot(curve([700, 768.5, 800], 1))?.spot).toBe(768.5)
    expect(atSpot(curve([700, 800], 1))).toBeUndefined()
  })
})
