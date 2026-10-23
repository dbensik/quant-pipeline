import { describe, expect, it } from 'vitest'

import type { OHLCVBar } from '@/api/client'
import {
  buildVolumeSeries,
  missingVolumeNote,
  volumeDirection,
  volumeOf,
  volumeUnit,
} from './volumeData'

function bar(
  day: number,
  open: number | null,
  close: number | null,
  volume: number | null,
): OHLCVBar {
  const date = String(day).padStart(2, '0')
  return { time: `2024-01-${date}T00:00:00Z`, open, high: null, low: null, close, volume }
}

describe('volumeOf', () => {
  it('keeps a real zero and refuses missing or impossible values', () => {
    expect(volumeOf(bar(1, 1, 1, 0))).toBe(0)
    expect(volumeOf(bar(1, 1, 1, null))).toBeNull()
    expect(volumeOf(bar(1, 1, 1, Number.NaN))).toBeNull()
    expect(volumeOf(bar(1, 1, 1, -5))).toBeNull()
  })
})

describe('volumeDirection', () => {
  it("follows the candle: close against the bar's own open", () => {
    expect(volumeDirection(bar(1, 100, 101, 1))).toBe('up')
    expect(volumeDirection(bar(1, 100, 100, 1))).toBe('up')
    expect(volumeDirection(bar(1, 100, 99, 1))).toBe('down')
  })

  it('is unknown without an open, never guessed from another bar', () => {
    expect(volumeDirection(bar(1, null, 99, 1))).toBe('unknown')
    expect(volumeDirection(bar(1, 100, null, 1))).toBe('unknown')
  })
})

describe('volumeUnit', () => {
  it('labels crypto in dollars and everything else in shares', () => {
    expect(volumeUnit('crypto').label).toBe('Volume (USD)')
    expect(volumeUnit('crypto').format(30_470_182_680)).toBe('$30.5B')
    expect(volumeUnit('equity').label).toBe('Volume (shares)')
    expect(volumeUnit('equity').format(31_812_600)).toBe('31.8M')
    expect(volumeUnit(undefined).label).toBe('Volume (shares)')
  })
})

describe('buildVolumeSeries', () => {
  it('keeps every bar with volume, even one a candle cannot draw', () => {
    const series = buildVolumeSeries([
      bar(1, 100, 101, 10),
      bar(2, null, 99, 20),
      bar(3, 100, 99, null),
      bar(4, 100, 99, 30),
    ])
    expect(series.x).toEqual(['2024-01-01', '2024-01-02', '2024-01-04'])
    expect(series.volume).toEqual([10, 20, 30])
    expect(series.direction).toEqual(['up', 'unknown', 'down'])
    expect(series.missing).toBe(1)
  })
})

describe('missingVolumeNote', () => {
  it('says nothing when every bar has volume', () => {
    expect(missingVolumeNote(0, 10)).toBeNull()
  })

  it('counts the gaps, and says so plainly when there is no volume at all', () => {
    expect(missingVolumeNote(1, 10)).toBe(
      '1 bar has no volume and is left blank in the volume pane.',
    )
    expect(missingVolumeNote(3, 10)).toBe(
      '3 bars have no volume and are left blank in the volume pane.',
    )
    expect(missingVolumeNote(10, 10)).toBe('No volume is stored for this range.')
  })
})
