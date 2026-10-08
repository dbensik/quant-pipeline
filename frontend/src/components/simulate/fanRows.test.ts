import { describe, expect, it } from 'vitest'

import type { FanBand } from '@/api/client'
import { buildFanRows } from './fanRows'

function band(step: number, mid: number): FanBand {
  return { step, p05: mid - 20, p25: mid - 10, p50: mid, p75: mid + 10, p95: mid + 20 }
}

const BANDS = [band(0, 100), band(1, 101), band(2, 102), band(3, 103)]
const CURVE = [100, 110, 120, 130, 140, 150].map((total) => ({ total }))

describe('buildFanRows — bands', () => {
  it('carries the percentiles and builds the two range pairs', () => {
    const rows = buildFanRows({ bands: BANDS, historical: [], bars: 6, resampledFrom: 3, initialCapital: 100 })
    expect(rows).toHaveLength(4)
    expect(rows[1]).toMatchObject({ step: 1, p05: 81, p50: 101, p95: 121, outer: [81, 121], inner: [91, 111] })
    expect(rows.every((row) => row.historical === undefined)).toBe(true)
  })
})

describe('buildFanRows — historical overlay alignment', () => {
  it('a strategy invested from bar 0 (buy and hold): step j is bar j - 1', () => {
    // bars = 6, resampled_from = 6 -> first invested bar 0.
    const rows = buildFanRows({ bands: BANDS, historical: CURVE, bars: 6, resampledFrom: 6, initialCapital: 100 })
    expect(rows.map((row) => row.historical)).toEqual([100, 100, 110, 120])
  })

  it('a strategy that waited two bars: step j is bar j + 1', () => {
    // bars = 6, resampled_from = 4 -> first invested bar 2; bar 1 is the last cash bar.
    const rows = buildFanRows({ bands: BANDS, historical: CURVE, bars: 6, resampledFrom: 4, initialCapital: 100 })
    expect(rows.map((row) => row.historical)).toEqual([100, 120, 130, 140])
  })

  it('prices mode (resampled_from = bars - 1): step j is bar j', () => {
    const rows = buildFanRows({ bands: BANDS, historical: CURVE, bars: 6, resampledFrom: 5, initialCapital: 100 })
    expect(rows.map((row) => row.historical)).toEqual([100, 110, 120, 130])
  })

  it('step 0 is the starting capital even when the curve starts elsewhere', () => {
    const rows = buildFanRows({ bands: BANDS, historical: CURVE, bars: 6, resampledFrom: 6, initialCapital: 250 })
    expect(rows[0].historical).toBe(250)
  })

  it('stops the overlay where the history ends', () => {
    const longBands = [0, 1, 2, 3, 4, 5, 6, 7].map((step) => band(step, 100 + step))
    const rows = buildFanRows({ bands: longBands, historical: CURVE, bars: 6, resampledFrom: 6, initialCapital: 100 })
    expect(rows.map((row) => row.historical)).toEqual([100, 100, 110, 120, 130, 140, 150, undefined])
  })
})
