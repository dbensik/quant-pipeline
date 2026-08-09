import { beforeEach, describe, expect, it } from 'vitest'

import { useAppStore } from './useAppStore'

const initial = useAppStore.getState()

beforeEach(() => {
  useAppStore.setState(initial, true)
})

describe('useAppStore — selections only', () => {
  it('defaults to the last twelve months, ending today', () => {
    const state = useAppStore.getState()
    expect(state.selectedSymbol).toBe('AAPL')

    // Asserted as a RELATIONSHIP, not a literal date. This used to pin
    // '2025-07-15' because ingestion wrote to SQLite while the API read
    // TimescaleDB, leaving the data thirteen months stale; that was fixed on
    // 2026-08-09 and the default is now relative. A literal would make this
    // test fail with the passage of time rather than with a code change.
    const today = new Date().toISOString().slice(0, 10)
    expect(state.endDate).toBe(today)
    expect(state.startDate < state.endDate).toBe(true)

    const spanDays =
      (Date.parse(state.endDate) - Date.parse(state.startDate)) / 86_400_000
    expect(spanDays).toBe(365)
  })

  it('holds no server data', () => {
    // The whole structure rests on this split: TanStack Query owns server
    // state, Zustand owns UI selections. A `bars` or `results` key here means
    // someone has started hand-rolling cache invalidation.
    const keys = Object.keys(useAppStore.getState())
    const serverish = keys.filter((key) =>
      /bars|assets|results|metrics|equity|strategies$/i.test(key),
    )
    expect(serverish).toEqual([])
  })
})

describe('useAppStore — strategy selection', () => {
  it('clears parameters when the strategy changes', () => {
    // THE regression. Parameter names are strategy-specific: carrying
    // short_window over to mean_reversion sends an unknown name the API
    // rejects with 422.
    useAppStore.getState().setStrategy('ma_crossover')
    useAppStore.getState().setStrategyParam('short_window', 10)
    expect(useAppStore.getState().strategyParams).toEqual({ short_window: 10 })

    useAppStore.getState().setStrategy('mean_reversion')
    expect(useAppStore.getState().strategyParams).toEqual({})
  })

  it('merges individual parameter updates', () => {
    const { setStrategyParam } = useAppStore.getState()
    setStrategyParam('window', 20)
    setStrategyParam('threshold', 1.5)
    expect(useAppStore.getState().strategyParams).toEqual({
      window: 20,
      threshold: 1.5,
    })
  })

  it('replaces all parameters on reset', () => {
    useAppStore.getState().setStrategyParam('stale', 1)
    useAppStore.getState().resetStrategyParams({ window: 20 })
    expect(useAppStore.getState().strategyParams).toEqual({ window: 20 })
  })
})

describe('useAppStore — toggles', () => {
  it('signal overlay is off by default and toggles', () => {
    expect(useAppStore.getState().showSignals).toBe(false)
    useAppStore.getState().toggleSignals()
    expect(useAppStore.getState().showSignals).toBe(true)
  })

  it('progress streaming is on by default', () => {
    // The websocket is the better experience; REST is the fallback.
    expect(useAppStore.getState().streamProgress).toBe(true)
    useAppStore.getState().toggleStreamProgress()
    expect(useAppStore.getState().streamProgress).toBe(false)
  })

  it('setDateRange sets both ends together', () => {
    useAppStore.getState().setDateRange('2020-01-01', '2021-01-01')
    const { startDate, endDate } = useAppStore.getState()
    expect([startDate, endDate]).toEqual(['2020-01-01', '2021-01-01'])
  })
})
