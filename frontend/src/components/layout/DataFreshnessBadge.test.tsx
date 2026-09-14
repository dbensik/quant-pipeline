/**
 * The header badge that makes stale data visible instead of silent.
 *
 * Asserts on the rendered label and tone, not on values the response merely
 * echoes back: the tone is computed, and it is the thing a viewer notices.
 */

import { screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { DataFreshnessBadge } from './DataFreshnessBadge'

const FRESH = {
  as_of: '2026-09-14T13:00:00Z',
  max_age_days: 5,
  newest_bar: '2026-09-13T00:00:00Z',
  age_days: 1,
  assets: 612,
  stale: 0,
  by_class: [],
  stale_assets: [],
}

afterEach(() => vi.restoreAllMocks())

describe('DataFreshnessBadge', () => {
  it('shows the newest bar date and age when current', async () => {
    vi.spyOn(api, 'dataFreshness').mockResolvedValue(FRESH as never)
    renderPage(<DataFreshnessBadge />)
    const badge = await screen.findByText('Bars to 2026-09-13 · 1d')
    expect(badge.closest('a')).toHaveAttribute('data-tone', 'ok')
  })

  it('turns destructive when the store is older than the threshold', async () => {
    vi.spyOn(api, 'dataFreshness').mockResolvedValue({
      ...FRESH,
      newest_bar: '2026-09-06T00:00:00Z',
      age_days: 8,
      stale: 612,
    } as never)
    renderPage(<DataFreshnessBadge />)
    const badge = await screen.findByText('Bars to 2026-09-06 · 8d')
    expect(badge.closest('a')).toHaveAttribute('data-tone', 'stale')
    expect(badge.closest('a')).toHaveAttribute('title', expect.stringContaining('8 days old'))
  })

  it('does not go quiet when the API fails', async () => {
    vi.spyOn(api, 'dataFreshness').mockRejectedValue(new ApiError(0, 'down'))
    renderPage(<DataFreshnessBadge />)
    const badge = await screen.findByText('Data age unknown')
    expect(badge.closest('a')).toHaveAttribute('data-tone', 'unknown')
  })

  it('links to the Data page', async () => {
    vi.spyOn(api, 'dataFreshness').mockResolvedValue(FRESH as never)
    renderPage(<DataFreshnessBadge />)
    const link = await screen.findByRole('link', { name: /Data freshness/ })
    expect(link).toHaveAttribute('href', '/data')
  })
})
