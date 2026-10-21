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
import type { BacktestInput,
  OptionPriceRequest,
  ValuationRequest,
  SimulationInput } from './client'

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
  dataFreshness: () => ['data-freshness'] as const,
  optionsArchive: () => ['options-archive'] as const,
  optionsSurface: (ticker: string, day: string | null) => ['options-surface', ticker, { day }] as const,
  optionPricers: () => ['option-pricers'] as const,
  optionQuotes: (ticker: string, day: string | null, expiry: string | null) =>
    ['option-quotes', ticker, { day, expiry }] as const,
  valuationCoverage: () => ['valuation-coverage'] as const,
  valuation: (body: Record<string, unknown>) => ['valuation', body] as const,
  valuationHistory: (symbol: string, since: string | null) => ['valuation-history', symbol, { since }] as const,
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

/**
 * Buy and hold of the same symbol over the same window as a backtest just run,
 * so the result can be read against it — every strategy finding in this
 * project is "compared with buy and hold". Off for a buy-and-hold run itself.
 * Keyed on symbol and window only: buy and hold has no parameters.
 */
export function useBuyAndHoldBenchmark(
  run: { symbol: string; start: string; end: string; strategyId: string } | null,
) {
  return useQuery({
    queryKey: ['backtest-benchmark', run?.symbol, run?.start, run?.end],
    queryFn: () =>
      api.runBacktest({
        symbol: run!.symbol,
        strategy_id: 'buy_and_hold',
        start: run!.start,
        end: run!.end,
        include_trades: false,
        // Only its metrics and curve are drawn, beside the strategy's.
        include_benchmark_report: false,
      }),
    enabled: Boolean(run) && run?.strategyId !== 'buy_and_hold',
    staleTime: 5 * 60_000,
  })
}

export function useRunSimulation() {
  return useMutation({
    mutationFn: (request: SimulationInput) => api.runSimulation(request),
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

/**
 * Put one ticker in, or take it out of, a watchlist from a page that shows a
 * single ticker (Research, Valuation, Chart & Backtest). One-ticker endpoints,
 * not a re-save: re-saving would write back this page's copy of the list.
 */
export function useToggleWatchlistSymbol() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, symbol, add }: { name: string; symbol: string; add: boolean }) =>
      add ? api.addToWatchlist(name, symbol) : api.removeFromWatchlist(name, symbol),
    onSuccess: () => {
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

/** Short terms change the derived state (borrow, margin), so it is invalidated too. */
export function useSetShortTerms() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      name,
      terms,
    }: {
      name: string
      terms: import('./client').PortfolioShortTerms
    }) => api.setShortTerms(name, terms),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useClearShortTerms() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.clearShortTerms(name),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

/** Attaching or detaching changes the state (its strategy) and what may be traded. */
export function useAttachStrategy() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      name,
      body,
    }: {
      name: string
      body: import('./client').AttachStrategyRequest
    }) => api.attachStrategy(name, body),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

export function useDetachStrategy() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.detachStrategy(name),
    onSuccess: () => invalidatePortfolio(queryClient),
  })
}

/**
 * The dry run: what the attached strategy would trade next, and what it
 * holds against what it wants. A query, not a mutation: it writes nothing.
 * Keyed under 'portfolio' so every portfolio write refreshes it.
 */
export function useStrategyPreview(name: string | null, attached: boolean) {
  return useQuery({
    queryKey: ['portfolio', name ?? '', 'run-preview'] as const,
    queryFn: () => api.runStrategy(name as string),
    enabled: Boolean(name) && attached,
    retry: retryUnlessNotFound,
  })
}

/** Keyed under 'portfolio', so a run or any other portfolio write refreshes it. */
export function useTracking(name: string | null, attached: boolean) {
  return useQuery({
    queryKey: ['portfolio', name ?? '', 'tracking'] as const,
    queryFn: () => api.getTracking(name as string),
    enabled: Boolean(name) && attached,
    retry: retryUnlessNotFound,
  })
}

/** Trade the pending sessions now: writes trades, so every portfolio query refreshes. */
export function useRunStrategy() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => api.runStrategy(name, true),
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

/**
 * A multi-asset strategy and, beside it, the SAME basket equal-weighted and
 * rebalanced monthly. Decision 29 measured the allocation rules against exactly
 * that benchmark; a run without it cannot say whether the rule did anything.
 */
export function usePortfolioBacktest() {
  return useMutation({
    mutationFn: async (body: Parameters<typeof api.runPortfolioBacktest>[0]) => {
      const [strategy, benchmark] = await Promise.all([
        api.runPortfolioBacktest(body),
        api.runPortfolioBacktest({
          ...body,
          strategy_id: 'index_rebalancing',
          params: {},
          weights: null,
          // The benchmark never shorts; the server refuses short terms on it.
          borrow_rate: undefined,
          max_gross: undefined,
          maintenance: undefined,
        }),
      ])
      return { strategy, benchmark }
    },
  })
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

/**
 * Age of the newest stored bar, rendered in the header on every page.
 *
 * Refetched on an interval rather than only on mount: the badge exists so a
 * dead 06:00 job is noticed by whoever has the dashboard open, and a tab
 * left open overnight is exactly the viewer that would otherwise keep showing
 * yesterday's green.
 */
export function useDataFreshness() {
  return useQuery({
    queryKey: queryKeys.dataFreshness(),
    queryFn: () => api.dataFreshness(),
    refetchInterval: 5 * 60_000,
    staleTime: 60_000,
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
      // ...and the data-age badge, which is derived from the newest bar.
      void queryClient.invalidateQueries({ queryKey: queryKeys.dataFreshness() })
    },
  })
}

export function useAddAsset() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: api.addAsset,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['assets'] })
      // A newly registered asset has no bars yet, so it is stale by definition.
      void queryClient.invalidateQueries({ queryKey: queryKeys.dataFreshness() })
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

export function useSaveResult() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, payload }: { name: string; payload: unknown }) =>
      api.saveResult(name, payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['results'] })
    },
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


// ---------------------------------------------------------------------------
// Options (tier 2 phase 4)
// ---------------------------------------------------------------------------

/**
 * 404 (no such capture) and 409 (no risk-free rate stored for that day) are
 * answers, not transient failures: retrying repeats a 10-35 s server-side
 * attempt only to get the same reply.
 */
function retryOptions(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && (error.isNotFound || error.status === 409 || error.status === 422)) {
    return false
  }
  return failureCount < 1
}

export function useOptionsArchive() {
  return useQuery({
    queryKey: queryKeys.optionsArchive(),
    queryFn: () => api.getOptionsArchive(),
    staleTime: 5 * 60_000,
  })
}

/**
 * A surface for a past day is fixed until its inputs change server-side, and
 * the server's cache key handles that; an hour's staleness here is safe.
 */
export function useOptionsSurface(ticker: string | null, day: string | null) {
  return useQuery({
    queryKey: queryKeys.optionsSurface(ticker ?? '', day),
    queryFn: () => api.getOptionsSurface(ticker as string, day),
    enabled: Boolean(ticker),
    staleTime: 60 * 60_000,
    retry: retryOptions,
  })
}

/** A capture never changes once complete, so its quotes never go stale. */
export function useOptionQuotes(ticker: string | null, day: string | null, expiry: string | null) {
  return useQuery({
    queryKey: queryKeys.optionQuotes(ticker ?? '', day, expiry),
    queryFn: () => api.getOptionQuotes(ticker as string, day, expiry),
    enabled: Boolean(ticker),
    staleTime: Infinity,
    retry: retryOptions,
  })
}

export function useOptionPricers() {
  return useQuery({
    queryKey: queryKeys.optionPricers(),
    queryFn: () => api.getOptionPricers(),
    staleTime: Infinity,
  })
}

export function usePriceOption() {
  return useMutation({
    mutationFn: (body: OptionPriceRequest) => api.priceOption(body),
  })
}


// ---------------------------------------------------------------------------
// Valuation (tier 3 phase 5)
// ---------------------------------------------------------------------------

/**
 * 404 (not an equity), 409 (no rate for the date) and 422 (refused: a bank, a
 * REIT, no capex on file...) are answers with a reason, not faults to retry.
 */
function retryValuation(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && [404, 409, 422].includes(error.status)) return false
  return failureCount < 1
}

export function useValuationCoverage() {
  return useQuery({ queryKey: queryKeys.valuationCoverage(), queryFn: () => api.getValuationCoverage(), staleTime: 10 * 60_000 })
}

/**
 * A DCF is a POST (it takes overrides) but has no side effects, so it is a
 * query keyed by its request: the same inputs are served from the cache.
 */
export function useValuation(body: ValuationRequest | null) {
  return useQuery({
    queryKey: queryKeys.valuation((body ?? {}) as Record<string, unknown>),
    queryFn: () => api.valueCompany(body as ValuationRequest),
    enabled: Boolean(body?.symbol),
    staleTime: 10 * 60_000,
    retry: retryValuation,
  })
}

export function useValuationHistory(symbol: string | null, since: string | null = null) {
  return useQuery({
    queryKey: queryKeys.valuationHistory(symbol ?? '', since),
    queryFn: () => api.getValuationHistory(symbol as string, since),
    enabled: Boolean(symbol),
    staleTime: 30 * 60_000,
    retry: retryValuation,
  })
}
