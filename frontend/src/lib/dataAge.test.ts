import { describe, expect, it } from 'vitest'

import type { DataFreshnessResponse } from '@/api/client'
import { isoDate, summariseDataAge } from './dataAge'

function response(over: Partial<DataFreshnessResponse> = {}): DataFreshnessResponse {
  return {
    as_of: '2026-09-14T13:00:00Z',
    max_age_days: 5,
    newest_bar: '2026-09-13T00:00:00Z',
    age_days: 1,
    assets: 612,
    stale: 0,
    by_class: [],
    stale_assets: [],
    ...over,
  }
}

describe('summariseDataAge', () => {
  it('is ok with a one-day-old store', () => {
    const out = summariseDataAge(response(), null)
    expect(out.tone).toBe('ok')
    expect(out.label).toBe('Bars to 2026-09-13 · 1d')
  })

  it('tolerates a weekend: three days is still ok', () => {
    const out = summariseDataAge(
      response({ newest_bar: '2026-09-11T00:00:00Z', age_days: 3 }),
      null,
    )
    expect(out.tone).toBe('ok')
  })

  it('goes stale past the server threshold', () => {
    const out = summariseDataAge(
      response({ newest_bar: '2026-09-08T00:00:00Z', age_days: 6 }),
      null,
    )
    expect(out.tone).toBe('stale')
    expect(out.label).toBe('Bars to 2026-09-08 · 6d')
    expect(out.detail).toMatch(/6 days old/)
  })

  it('uses the threshold the server echoes back, not a hardcoded one', () => {
    const out = summariseDataAge(response({ age_days: 6, max_age_days: 10 }), null)
    expect(out.tone).toBe('ok')
  })

  it('puts a per-asset stale count in the label while the store is current', () => {
    const out = summariseDataAge(response({ stale: 21 }), null)
    expect(out.tone).toBe('ok')
    expect(out.label).toBe('Bars to 2026-09-13 · 1d · 21 stale')
    expect(out.detail).toMatch(/21 of 612/)
  })

  it('treats an empty store as stale, not unknown', () => {
    const out = summariseDataAge(
      response({ newest_bar: null, age_days: null, assets: 4, stale: 4 }),
      null,
    )
    expect(out.tone).toBe('stale')
    expect(out.label).toBe('No bars stored')
  })

  it('reports an API error as unknown rather than silently ok', () => {
    const out = summariseDataAge(undefined, new Error('boom'))
    expect(out.tone).toBe('unknown')
    expect(out.label).toBe('Data age unknown')
  })
})

describe('isoDate', () => {
  it('takes the date part without a timezone shift', () => {
    expect(isoDate('2026-09-13T00:00:00Z')).toBe('2026-09-13')
    expect(isoDate(null)).toBe('—')
  })
})
