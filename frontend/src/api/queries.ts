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

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

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
  signals: (
    symbol: string,
    strategyId: string,
    start: string,
    end: string,
    params?: Record<string, number | string>,
  ) => ['signals', symbol, strategyId, start, end, params ?? {}] as const,
  watchlists: (symbol?: string) => ['watchlists', { symbol }] as const,
  portfolios: () => ['portfolios'] as const,
  portfolio: (name: string, withPrices: boolean) =>
    ['portfolio', name, { withPrices }] as const,
  trades: (name: string) => ['trades', name] as const,
  profile: (symbol: string) => ['profile', symbol] as const,
  financials: (symbol: string, quarterly: boolean) =>
    ['financials', symbol, { quarterly }] as const,
  news: (source: Record<string, unknown>) => ['news', source] as const,
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

/**
 * For the research endpoints. Like retryUnlessNotFound, but 503 is also
 * terminal: it means the upstream provider failed, the server has ALREADY
 * attempted the call, and it caches the outcome — so retrying twice only adds
 * seconds before the user sees the same message.
 */
function retryTransientOnly(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && (error.isNotFound || error.status === 503)) {
    return false
  }
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
  params?: Record<string, number | string>,
  enabled = true,
) {
  return useQuery({
    // params are part of the key: the same symbol and strategy with different
    // windows are different signals, and sharing a key would serve stale
    // markers that disagree with the chart's parameters.
    queryKey: queryKeys.signals(symbol ?? '', strategyId ?? '', start, end, params),
    queryFn: () =>
      api.getSignals({
        symbol: symbol!,
        strategy_id: strategyId!,
        start,
        end,
        params,
      }),
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


// ---------------------------------------------------------------------------
// Watchlists
// ---------------------------------------------------------------------------
//
// The first WRITE path in the frontend. Every earlier hook was read-only, so
// this establishes the pattern the other CRUD pages copy: a mutation whose
// onSuccess invalidates the list it changed.
//
// Without that invalidation the write succeeds and the UI keeps showing the
// pre-write list — the frontend twin of the expire_on_commit=False bug that
// made a watchlist save look like it had wiped the list on the backend.

export function useWatchlists(symbol?: string) {
  return useQuery({
    queryKey: queryKeys.watchlists(symbol),
    queryFn: () => api.listWatchlists({ symbol }),
    staleTime: 30_000,
  })
}

export function useSaveWatchlist() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, symbols }: { name: string; symbols: string[] }) =>
      api.saveWatchlist(name, symbols),
    onSuccess: () => {
      // Every watchlists query, not just the unfiltered one: a save changes
      // which lists contain a symbol, so `?symbol=` results go stale too.
      void queryClient.invalidateQueries({ queryKey: ['watchlists'] })
    },
  })
}

export function useDeleteWatchlist() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.deleteWatchlist(name),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['watchlists'] })
    },
  })
}


// ---------------------------------------------------------------------------
// Portfolios
// ---------------------------------------------------------------------------
//
// Writes invalidate BOTH the trade log and the derived state. Cash, positions
// and P&L are computed from the trades, so adding one changes the state query
// even though nothing wrote to it — invalidating only ['trades'] would leave
// the summary showing pre-trade cash.

export function usePortfolios() {
  return useQuery({
    queryKey: queryKeys.portfolios(),
    queryFn: api.listPortfolios,
    staleTime: 30_000,
  })
}

export function usePortfolio(name: string | null, withPrices = true) {
  return useQuery({
    queryKey: queryKeys.portfolio(name ?? '', withPrices),
    queryFn: () => api.getPortfolio(name as string, { include_prices: withPrices }),
    enabled: Boolean(name),
    retry: retryUnlessNotFound,
  })
}

export function useTrades(name: string | null) {
  return useQuery({
    queryKey: queryKeys.trades(name ?? ''),
    queryFn: () => api.listTrades(name as string),
    enabled: Boolean(name),
    retry: retryUnlessNotFound,
  })
}

function invalidatePortfolio(queryClient: ReturnType<typeof useQueryClient>) {
  void queryClient.invalidateQueries({ queryKey: ['portfolios'] })
  void queryClient.invalidateQueries({ queryKey: ['portfolio'] })
  void queryClient.invalidateQueries({ queryKey: ['trades'] })
}

export function useCreatePortfolio() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: { name: string; initial_cash?: number }) =>
      api.createPortfolio(body),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useDeletePortfolio() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.deletePortfolio(name),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useAddTrade() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      name,
      trade,
      allowOverdraft,
    }: {
      name: string
      trade: import('./client').TradeIn
      allowOverdraft?: boolean
    }) => api.addTrade(name, trade, { allow_overdraft: allowOverdraft }),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useDeleteTrade() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, tradeId }: { name: string; tradeId: string }) =>
      api.deleteTrade(name, tradeId),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useRebalancePreview() {
  // A mutation rather than a query: it is an explicit "compute this now"
  // action with a body, and caching a preview across weight edits would show
  // orders for weights the user has already changed.
  return useMutation({
    mutationFn: ({
      name,
      targetWeights,
    }: {
      name: string
      targetWeights: Record<string, number>
    }) => api.previewRebalance(name, { target_weights: targetWeights }),
  })
}


// ---------------------------------------------------------------------------
// Research
// ---------------------------------------------------------------------------
//
// These are the only endpoints that reach the network (yfinance). The server
// caches them — 3600s profile/financials, 600s news — so the client's
// staleTime mirrors those rather than refetching into a warm server cache.
//
// A 503 means the upstream provider failed, not that the symbol is wrong;
// retrying once is reasonable, but a 404-style "no such thing" is not.

export function useProfile(symbol: string | null) {
  return useQuery({
    queryKey: queryKeys.profile(symbol ?? ''),
    queryFn: () => api.getProfile(symbol as string),
    enabled: Boolean(symbol),
    staleTime: 60 * 60_000,
    retry: retryTransientOnly,
  })
}

export function useFinancials(symbol: string | null, quarterly: boolean) {
  return useQuery({
    queryKey: queryKeys.financials(symbol ?? '', quarterly),
    queryFn: () => api.getFinancials(symbol as string, { quarterly }),
    enabled: Boolean(symbol),
    staleTime: 60 * 60_000,
    retry: retryTransientOnly,
  })
}

export function useNews(params: {
  symbols?: string[]
  portfolio?: string
  watchlist?: string
  limit?: number
}) {
  return useQuery({
    queryKey: queryKeys.news(params as Record<string, unknown>),
    queryFn: () => api.getNews(params),
    staleTime: 10 * 60_000,
    retry: retryTransientOnly,
  })
}


// ---------------------------------------------------------------------------
// Compare & optimize
// ---------------------------------------------------------------------------
//
// Mutations, not queries. Both are expensive, explicit "run this now" actions
// with a request body — a grid search is hundreds of backtests. Modelling them
// as queries would fire one on every parameter keystroke.

export function useCompare() {
  return useMutation({ mutationFn: api.compareStrategies })
}

export function useOptimizeStrategy() {
  return useMutation({ mutationFn: api.optimizeStrategy })
}

export function useOptimizePortfolio() {
  return useMutation({ mutationFn: api.optimizePortfolio })
}


// ---------------------------------------------------------------------------
// Screeners & statistics
// ---------------------------------------------------------------------------

export function useScreeners() {
  return useQuery({
    queryKey: ['screeners'],
    queryFn: api.listScreeners,
    staleTime: 60 * 60_000,
  })
}

export function useRunScreen() {
  return useMutation({ mutationFn: api.runScreen })
}

export function useStatisticsCatalogue() {
  return useQuery({
    queryKey: ['statistics-catalogue'],
    queryFn: api.listStatistics,
    staleTime: 60 * 60_000,
  })
}

export function useRunStatistic() {
  return useMutation({
    mutationFn: ({
      testId,
      body,
    }: {
      testId: string
      body: { symbols: string[]; start: string; end: string; params?: Record<string, unknown> }
    }) => api.runStatistic(testId, body),
  })
}


// ---------------------------------------------------------------------------
// Ingest & results
// ---------------------------------------------------------------------------

export function useIngestStatus(enabled: boolean) {
  return useQuery({
    queryKey: ['ingest-status'],
    queryFn: api.ingestStatus,
    // Polled only while a run is in flight. A permanent 2s poll would keep the
    // API busy for a page nobody is watching.
    refetchInterval: enabled ? 2_000 : false,
    enabled,
  })
}

export function useRunIngest() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: api.runIngest,
    onSuccess: () => {
      // New bars change every price-derived view, so the OHLCV and asset
      // caches are stale the moment this returns.
      void queryClient.invalidateQueries({ queryKey: ['ohlcv'] })
      void queryClient.invalidateQueries({ queryKey: ['assets'] })
      void queryClient.invalidateQueries({ queryKey: ['ingest-status'] })
    },
  })
}

export function useAddAsset() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: api.addAsset,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assets'] })
    },
  })
}

export function useUniverse(source: string | null) {
  return useQuery({
    queryKey: ['universe', source],
    queryFn: () => api.getUniverse(source as string),
    enabled: Boolean(source),
    staleTime: 24 * 60 * 60_000,
    retry: false,
  })
}

export function useResults() {
  return useQuery({ queryKey: ['results'], queryFn: api.listResults })
}

export function useResult(name: string | null) {
  return useQuery({
    queryKey: ['result', name],
    queryFn: () => api.loadResult(name as string),
    enabled: Boolean(name),
    retry: retryUnlessNotFound,
  })
}

export function useDeleteResult() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.deleteResult(name),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['results'] })
    },
  })
}
