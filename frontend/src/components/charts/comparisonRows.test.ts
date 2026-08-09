/**
 * comparisonRows — merging several equity curves into Recharts rows.
 *
 * Tested here rather than through the chart: Recharts measures a 0x0 parent
 * under jsdom and renders nothing, so assertions on chart output are vacuous.
 *
 * Phase 5 — React pages for the ported routers
 */

import { describe, expect, it } from 'vitest'

import { colourFor, comparisonRows, SERIES_COLOURS } from './comparisonRows'

const A = {
  name: 'A',
  points: [
    { time: '2024-01-01', total: 100 },
    { time: '2024-01-02', total: 110 },
  ],
}
const B = {
  name: 'B',
  points: [
    { time: '2024-01-01', total: 200 },
    { time: '2024-01-02', total: 190 },
  ],
}

describe('comparisonRows', () => {
  it('merges series sharing timestamps into one row each', () => {
    const rows = comparisonRows([A, B])
    expect(rows).toEqual([
      { time: '2024-01-01', A: 100, B: 200 },
      { time: '2024-01-02', A: 110, B: 190 },
    ])
  })

  it('sorts by time regardless of input order', () => {
    const unsorted = {
      name: 'A',
      points: [
        { time: '2024-01-03', total: 3 },
        { time: '2024-01-01', total: 1 },
        { time: '2024-01-02', total: 2 },
      ],
    }
    expect(comparisonRows([unsorted]).map((r) => r.time)).toEqual([
      '2024-01-01',
      '2024-01-02',
      '2024-01-03',
    ])
  })

  it('fills a missing point with null, not the previous value', () => {
    // Carrying the last value forward would draw a flat segment that reads as
    // "the strategy held" when in fact there was no data. Recharts breaks the
    // line at null, which is honest.
    const short = { name: 'B', points: [{ time: '2024-01-01', total: 200 }] }
    const rows = comparisonRows([A, short])
    expect(rows[1]).toEqual({ time: '2024-01-02', A: 110, B: null })
  })

  it('gives every series a key in every row', () => {
    // A key missing from a row makes Recharts drop the point silently rather
    // than render a gap.
    const short = { name: 'B', points: [{ time: '2024-01-02', total: 5 }] }
    for (const row of comparisonRows([A, short])) {
      expect(Object.keys(row).sort()).toEqual(['A', 'B', 'time'])
    }
  })

  it('returns nothing for no series', () => {
    expect(comparisonRows([])).toEqual([])
  })

  it('handles a series with no points', () => {
    expect(comparisonRows([{ name: 'Empty', points: [] }])).toEqual([])
  })

  it('keeps a zero value rather than treating it as missing', () => {
    const zeroed = { name: 'Z', points: [{ time: '2024-01-01', total: 0 }] }
    expect(comparisonRows([zeroed])[0].Z).toBe(0)
  })
})

describe('colourFor', () => {
  it('gives adjacent series different colours', () => {
    expect(colourFor(0)).not.toBe(colourFor(1))
  })

  it('cycles rather than running off the end', () => {
    expect(colourFor(SERIES_COLOURS.length)).toBe(colourFor(0))
  })
})
