import { describe, expect, it } from 'vitest'

import { HISTORY_START, matchingPreset, presetRange } from './dateRange'

describe('presetRange', () => {
  it('counts whole years back from today', () => {
    expect(presetRange('1Y', '2026-10-15')).toEqual({ start: '2025-10-15', end: '2026-10-15' })
    expect(presetRange('5Y', '2026-10-15')).toEqual({ start: '2021-10-15', end: '2026-10-15' })
    expect(presetRange('10Y', '2026-10-15')).toEqual({ start: '2016-10-15', end: '2026-10-15' })
  })

  it('Max starts where stored history does', () => {
    expect(presetRange('Max', '2026-10-15')).toEqual({ start: HISTORY_START, end: '2026-10-15' })
  })

  it('a leap day lands on the same calendar date a year back, rolling into March', () => {
    expect(presetRange('1Y', '2028-02-29').start).toBe('2027-03-01')
  })
})

describe('matchingPreset', () => {
  it('names the preset a range matches', () => {
    expect(matchingPreset('2021-10-15', '2026-10-15', '2026-10-15')).toBe('5Y')
  })

  it('is null for a hand-picked range', () => {
    expect(matchingPreset('2020-03-01', '2026-10-15', '2026-10-15')).toBeNull()
  })
})
