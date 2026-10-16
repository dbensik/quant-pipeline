import { act, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it } from 'vitest'

import { HISTORY_START, presetRange, todayIso } from '@/lib/dateRange'
import { useAppStore } from '@/store/useAppStore'
import { renderPage } from '@/test/renderPage'
import { DateRangePresets } from './DateRangePresets'

const initial = useAppStore.getState()
afterEach(() => act(() => useAppStore.setState(initial)))

describe('DateRangePresets', () => {
  it('sets the shared range', async () => {
    renderPage(<DateRangePresets />)
    await userEvent.click(screen.getByRole('button', { name: 'Max' }))
    expect(useAppStore.getState().startDate).toBe(HISTORY_START)
    expect(useAppStore.getState().endDate).toBe(todayIso())
  })

  it('shows which preset the current range is', async () => {
    const r = presetRange('5Y', todayIso())
    act(() => useAppStore.getState().setDateRange(r.start, r.end))
    renderPage(<DateRangePresets />)
    expect(screen.getByRole('button', { name: '5Y' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: '1Y' })).toHaveAttribute('aria-pressed', 'false')
  })
})
