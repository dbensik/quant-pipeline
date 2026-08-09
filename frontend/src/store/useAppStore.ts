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

/**
 * 'line' is Recharts and ships in the main bundle; 'candlestick' is Plotly and
 * is loaded on demand (~1.2 MB). Default to line so the initial page load
 * never pays for Plotly.
 */
export type ChartType = 'line' | 'candlestick'

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
  /** Whether the price chart draws the strategy's buy/sell markers. */
  showSignals: boolean
  /** Run backtests over the websocket (live progress) rather than plain REST. */
  streamProgress: boolean
  /** Which price chart to render. */
  chartType: ChartType

  // --- actions ------------------------------------------------------------
  setSymbol: (symbol: string | null) => void
  setDateRange: (start: IsoDate, end: IsoDate) => void
  setStrategy: (strategyId: string | null) => void
  setStrategyParam: (name: string, value: number | string) => void
  resetStrategyParams: (params: Record<string, number | string>) => void
  toggleSignals: () => void
  toggleStreamProgress: () => void
  setChartType: (chartType: ChartType) => void
}

/**
 * The last twelve months, relative to today.
 *
 * This used to be pinned to '2025-07-15' — the newest bar in the database —
 * because ingestion had been writing to SQLite while the API read TimescaleDB,
 * so the data was thirteen months stale and a "last 12 months" default
 * rendered an empty chart. That was fixed on 2026-08-09: /api/v1/ingest writes
 * to TimescaleDB, and a full run brought 580 of 616 symbols current. The
 * remaining stragglers are delisted or acquired tickers (HES, ANSS, WBA and
 * others) that legitimately have no newer bars.
 */
const DEFAULT_END: IsoDate = isoDaysAgo(0)
const DEFAULT_START: IsoDate = isoDaysAgo(365)

export const useAppStore = create<AppState>((set) => ({
  selectedSymbol: 'AAPL',
  startDate: DEFAULT_START,
  endDate: DEFAULT_END,
  strategyId: null,
  strategyParams: {},
  showSignals: false,
  streamProgress: true,
  chartType: 'line',

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

  toggleSignals: () => set((state) => ({ showSignals: !state.showSignals })),

  toggleStreamProgress: () =>
    set((state) => ({ streamProgress: !state.streamProgress })),

  setChartType: (chartType) => set({ chartType }),
}))

export { isoDaysAgo }
