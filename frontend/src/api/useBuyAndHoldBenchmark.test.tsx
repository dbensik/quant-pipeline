import { QueryClientProvider } from '@tanstack/react-query'
import { renderHook, waitFor } from '@testing-library/react'
import type { ReactNode } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { api } from '@/api/client'
import { useBuyAndHoldBenchmark } from '@/api/queries'
import { makeQueryClient } from '@/test/renderPage'

const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={makeQueryClient()}>{children}</QueryClientProvider>
)

afterEach(() => vi.restoreAllMocks())

describe('useBuyAndHoldBenchmark', () => {
  it('runs buy and hold on the same symbol and window as the backtest', async () => {
    const run = vi.spyOn(api, 'runBacktest').mockResolvedValue({} as never)
    renderHook(
      () =>
        useBuyAndHoldBenchmark({
          symbol: 'SPY',
          start: '2015-01-01',
          end: '2020-01-01',
          strategyId: 'rsi',
        }),
      { wrapper },
    )
    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]).toMatchObject({
      symbol: 'SPY',
      strategy_id: 'buy_and_hold',
      start: '2015-01-01',
      end: '2020-01-01',
    })
  })

  it('does not benchmark buy and hold against itself', async () => {
    const run = vi.spyOn(api, 'runBacktest').mockResolvedValue({} as never)
    renderHook(
      () =>
        useBuyAndHoldBenchmark({
          symbol: 'SPY',
          start: '2015-01-01',
          end: '2020-01-01',
          strategyId: 'buy_and_hold',
        }),
      { wrapper },
    )
    await new Promise((r) => setTimeout(r, 20))
    expect(run).not.toHaveBeenCalled()
  })

  it('waits until there is a run', async () => {
    const run = vi.spyOn(api, 'runBacktest').mockResolvedValue({} as never)
    renderHook(() => useBuyAndHoldBenchmark(null), { wrapper })
    await new Promise((r) => setTimeout(r, 20))
    expect(run).not.toHaveBeenCalled()
  })
})
