/**
 * store/useAppStore.ts
 *
 * Zustand store for UI SELECTIONS ONLY.
 *
 * What belongs here: the symbol the user picked, the date range, the active
 * strategy and its parameters — things the user chose that no server knows about.
 *
 * What must NEVER go here: bars, assets, strategy catalogues, backtest results,
 * or anything else fetched from the API. That is server state and belongs to
 * TanStack Query (see api/queries.ts). Caching fetched data here means
 * hand-rolling staleness, invalidation and refetching that Query already does,
 * and it is how these two libraries end up fighting each other.
 *
 * Phase 4 — React frontend
 */

import { create } from 'zustand'

/** ISO date (YYYY-MM-DD) — the format the API's date query params expect. */
export type IsoDate = string

function isoDaysAgo(days: number): IsoDate {
  const date = new Date()
  date.setUTCDate(date.getUTCDate() - days)
  return date.toISOString().slice(0, 10)
}

interface AppState {
  // --- selections ---------------------------------------------------------
  selectedSymbol: string | null
  startDate: IsoDate
  endDate: IsoDate
  strategyId: string | null
  strategyParams: Record<string, number | string>

  // --- actions ------------------------------------------------------------
  setSymbol: (symbol: string | null) => void
  setDateRange: (start: IsoDate, end: IsoDate) => void
  setStrategy: (strategyId: string | null) => void
  setStrategyParam: (name: string, value: number | string) => void
  resetStrategyParams: (params: Record<string, number | string>) => void
}

/**
 * Default window ends at the last stored bar (2025-07-15), not today. The
 * dataset's most recent bar predates the current date by roughly a year, so a
 * "last 12 months from today" default would render an empty chart and look like
 * a bug. Revisit whenever the ingest pipeline is brought current.
 */
const DEFAULT_END: IsoDate = '2025-07-15'
const DEFAULT_START: IsoDate = '2024-07-15'

export const useAppStore = create<AppState>((set) => ({
  selectedSymbol: 'AAPL',
  startDate: DEFAULT_START,
  endDate: DEFAULT_END,
  strategyId: null,
  strategyParams: {},

  setSymbol: (symbol) => set({ selectedSymbol: symbol }),

  setDateRange: (startDate, endDate) => set({ startDate, endDate }),

  // Changing strategy clears params — parameter names are strategy-specific,
  // so carrying them across would send unknown names the API rejects with 422.
  setStrategy: (strategyId) => set({ strategyId, strategyParams: {} }),

  setStrategyParam: (name, value) =>
    set((state) => ({
      strategyParams: { ...state.strategyParams, [name]: value },
    })),

  resetStrategyParams: (strategyParams) => set({ strategyParams }),
}))

export { isoDaysAgo }
