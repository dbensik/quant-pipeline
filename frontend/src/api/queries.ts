/**
 * api/queries.ts
 *
 * TanStack Query hooks — the only way components read server state.
 *
 * The division of responsibility this whole structure rests on:
 *   TanStack Query owns SERVER state (bars, assets, strategies, results)
 *   Zustand      owns UI state     (selected symbol, date range, strategy)
 *
 * Putting fetched data into the Zustand store is the failure mode that makes
 * the split pointless — you end up hand-rolling cache invalidation, staleness
 * and refetch logic that Query already does.
 *
 * Phase 4 — React frontend
 */

import { useMutation, useQuery } from '@tanstack/react-query'

import { api, ApiError } from './client'
import type { BacktestInput } from './client'

/**
 * Query keys in one place so invalidation can never typo a key.
 * Each is a function so the key array shape stays consistent.
 */
export const queryKeys = {
  health: () => ['health'] as const,
  assets: (search?: string, assetClass?: string) =>
    ['assets', { search, assetClass }] as const,
  asset: (symbol: string) => ['asset', symbol] as const,
  ohlcv: (symbol: string, start: string, end: string) =>
    ['ohlcv', symbol, start, end] as const,
  strategies: (contract?: string) => ['strategies', { contract }] as const,
  signals: (symbol: string, strategyId: string, start: string, end: string) =>
    ['signals', symbol, strategyId, start, end] as const,
}

/**
 * A 404 means the symbol or strategy genuinely does not exist — retrying cannot
 * fix it and just delays the error reaching the user. Retry everything else,
 * which is where transient network failures live.
 */
function retryUnlessNotFound(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && error.isNotFound) return false
  return failureCount < 2
}

export function useHealth() {
  return useQuery({
    queryKey: queryKeys.health(),
    queryFn: api.health,
    // Cheap probe; refresh so a dead API surfaces without a manual reload.
    refetchInterval: 30_000,
    retry: false,
  })
}

export function useAssets(search?: string, assetClass?: string) {
  return useQuery({
    queryKey: queryKeys.assets(search, assetClass),
    queryFn: () => api.listAssets({ search, asset_class: assetClass, limit: 2000 }),
    // The registry changes only when the pipeline ingests new tickers.
    staleTime: 5 * 60_000,
  })
}

export function useAsset(symbol: string | null) {
  return useQuery({
    queryKey: queryKeys.asset(symbol ?? ''),
    queryFn: () => api.getAsset(symbol!),
    enabled: Boolean(symbol),
    retry: retryUnlessNotFound,
  })
}

export function useOhlcv(symbol: string | null, start: string, end: string) {
  return useQuery({
    queryKey: queryKeys.ohlcv(symbol ?? '', start, end),
    queryFn: () => api.getOhlcv({ symbol: symbol!, start, end }),
    enabled: Boolean(symbol) && Boolean(start) && Boolean(end),
    retry: retryUnlessNotFound,
    // Stored history is immutable until the next ingest — no need to refetch
    // the same window on every remount.
    staleTime: 5 * 60_000,
  })
}

export function useStrategies(contract: 'single' | 'multi' = 'single') {
  return useQuery({
    queryKey: queryKeys.strategies(contract),
    queryFn: () => api.listStrategies({ input_contract: contract }),
    // Only changes when the server restarts with a new registry.
    staleTime: Infinity,
  })
}

export function useSignals(
  symbol: string | null,
  strategyId: string | null,
  start: string,
  end: string,
  enabled = true,
) {
  return useQuery({
    queryKey: queryKeys.signals(symbol ?? '', strategyId ?? '', start, end),
    queryFn: () =>
      api.getSignals({ symbol: symbol!, strategy_id: strategyId!, start, end }),
    enabled: enabled && Boolean(symbol) && Boolean(strategyId),
    retry: retryUnlessNotFound,
  })
}

/**
 * Backtests are a mutation, not a query: they are user-triggered, expensive,
 * and should not fire on mount or refetch in the background.
 */
export function useRunBacktest() {
  return useMutation({
    mutationFn: (request: BacktestInput) => api.runBacktest(request),
  })
}
