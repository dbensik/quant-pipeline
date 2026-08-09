/**
 * api/client.ts
 *
 * The single place that talks HTTP. Everything else goes through the hooks in
 * queries.ts — pages and components never call fetch directly, so caching,
 * error shape and the base URL all have exactly one definition.
 *
 * Types come from schema.d.ts, generated from the live OpenAPI document
 * (`npm run gen:api`). They are not hand-written: hand-written copies drift
 * from the Pydantic models silently, which is the same failure the strategy
 * registry was created to fix on the Python side.
 *
 * Phase 4 — React frontend
 */

import type { components } from './schema'

// ---------------------------------------------------------------------------
// Re-exported response types — import these rather than reaching into `components`
// ---------------------------------------------------------------------------

export type OHLCVBar = components['schemas']['OHLCVBar']
export type OHLCVResponse = components['schemas']['OHLCVResponse']
export type AssetSummary = components['schemas']['AssetSummary']
export type AssetDetail = components['schemas']['AssetDetail']
export type AssetListResponse = components['schemas']['AssetListResponse']
export type StrategySchema = components['schemas']['StrategySchema']
export type StrategyListResponse = components['schemas']['StrategyListResponse']
export type ParamSchema = components['schemas']['ParamSchema']
export type BacktestRequest = components['schemas']['BacktestRequest']
export type BacktestResponse = components['schemas']['BacktestResponse']
export type EquityPoint = components['schemas']['EquityPoint']

/**
 * What a caller actually has to supply for a backtest.
 *
 * The generated BacktestRequest marks `initial_capital`, `transaction_cost`,
 * `seed`, `include_equity_curve` and `include_trades` as required, because
 * Pydantic emits them with defaults rather than as optional. The server fills
 * them in when omitted (verified), so this type spares every call site from
 * restating five defaults it does not care about.
 *
 * `start`/`end` are typed date-time but accept plain YYYY-MM-DD — Pydantic
 * coerces, and that is the shape the Zustand store holds.
 */
export type BacktestInput = Pick<
  BacktestRequest,
  'symbol' | 'strategy_id' | 'start' | 'end'
> &
  Partial<Omit<BacktestRequest, 'symbol' | 'strategy_id' | 'start' | 'end'>>
export type WatchlistOut = components['schemas']['WatchlistOut']
export type CompareRequest = components['schemas']['CompareRequest']
export type CompareResponse = components['schemas']['CompareResponse']
export type ComparisonRow = components['schemas']['ComparisonRow']
export type StrategyOptimizeRequest = components['schemas']['StrategyOptimizeRequest']
export type StrategyOptimizeResponse = components['schemas']['StrategyOptimizeResponse']
export type PortfolioOptimizeRequest = components['schemas']['PortfolioOptimizeRequest']
export type PortfolioOptimizeResponse = components['schemas']['PortfolioOptimizeResponse']
export type Profile = components['schemas']['Profile']
export type Financials = components['schemas']['Financials']
export type NewsFeed = components['schemas']['NewsFeed']
export type NewsItem = components['schemas']['NewsItem']
export type StatementLine = components['schemas']['StatementLine']
export type PortfolioSummary = components['schemas']['PortfolioSummary']
export type PortfolioStateOut = components['schemas']['PortfolioStateOut']
export type PositionOut = components['schemas']['PositionOut']
export type TradeOut = components['schemas']['TradeOut']
export type TradeIn = components['schemas']['TradeIn']
export type RebalancePreview = components['schemas']['RebalancePreview']
export type RebalanceOrder = components['schemas']['RebalanceOrder']
export type SignalsResponse = components['schemas']['SignalsResponse']
export type SignalPoint = components['schemas']['SignalPoint']

export const API_BASE_URL: string =
  import.meta.env.VITE_API_BASE_URL ?? 'http://127.0.0.1:8001'

/**
 * Error carrying the HTTP status, so callers can distinguish "unknown symbol"
 * (404) from "bad request" (422) from "the server fell over" (5xx) rather than
 * treating every failure as one undifferentiated blob.
 */
export class ApiError extends Error {
  readonly status: number
  readonly detail: string

  constructor(status: number, detail: string) {
    super(`API ${status}: ${detail}`)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
  }

  /** True when the request was well-formed but the resource does not exist. */
  get isNotFound(): boolean {
    return this.status === 404
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      headers: { 'Content-Type': 'application/json' },
      ...init,
    })
  } catch {
    // fetch only rejects on network-level failure — the API being down, DNS,
    // or CORS. Status codes resolve normally and are handled below.
    throw new ApiError(
      0,
      `Could not reach the API at ${API_BASE_URL}. Is it running? ` +
        `Start it with: poetry run uvicorn api.main:app --port 8001`,
    )
  }

  if (!response.ok) {
    let detail = response.statusText
    try {
      const body = await response.json()
      // FastAPI puts the message in `detail`; validation errors make it an array.
      detail =
        typeof body.detail === 'string'
          ? body.detail
          : JSON.stringify(body.detail ?? body)
    } catch {
      /* non-JSON error body — keep statusText */
    }
    throw new ApiError(response.status, detail)
  }

  return (await response.json()) as T
}

/**
 * For 204 responses. `request` calls response.json(), which throws on an empty
 * body — every DELETE in this API returns 204 with nothing.
 */
async function requestNoContent(path: string, init?: RequestInit): Promise<void> {
  let response: Response
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      headers: { 'Content-Type': 'application/json' },
      ...init,
    })
  } catch {
    throw new ApiError(0, `Could not reach the API at ${API_BASE_URL}.`)
  }
  if (!response.ok) {
    let detail = response.statusText
    try {
      const body = await response.json()
      detail = typeof body.detail === 'string' ? body.detail : JSON.stringify(body.detail ?? body)
    } catch {
      /* empty or non-JSON body */
    }
    throw new ApiError(response.status, detail)
  }
}

function qs(params: Record<string, string | number | boolean | undefined>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') search.set(key, String(value))
  }
  const s = search.toString()
  return s ? `?${s}` : ''
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

export const api = {
  health(): Promise<{ status: string }> {
    return request('/api/v1/health/ready')
  },

  listAssets(params: {
    asset_class?: string
    search?: string
    limit?: number
  } = {}): Promise<AssetListResponse> {
    return request(`/api/v1/assets${qs(params)}`)
  },

  getAsset(symbol: string): Promise<AssetDetail> {
    return request(`/api/v1/assets/${encodeURIComponent(symbol)}`)
  },

  getOhlcv(params: {
    symbol: string
    start: string
    end: string
    source?: string
  }): Promise<OHLCVResponse> {
    const { symbol, ...rest } = params
    return request(`/api/v1/ohlcv/${encodeURIComponent(symbol)}${qs(rest)}`)
  },

  listStrategies(params: { input_contract?: 'single' | 'multi' } = {}): Promise<
    StrategyListResponse
  > {
    return request(`/api/v1/strategies${qs(params)}`)
  },

  runBacktest(body: BacktestInput): Promise<BacktestResponse> {
    return request('/api/v1/backtest', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  getSignals(args: {
    symbol: string
    strategy_id: string
    start: string
    end: string
    /** Strategy parameters. Serialised to JSON for the query string. */
    params?: Record<string, number | string>
  }): Promise<SignalsResponse> {
    const { symbol, params, ...rest } = args
    // The endpoint takes `params` as a JSON-encoded query value so it stays a
    // GET. Omit it entirely when empty so the server uses registry defaults.
    const encoded =
      params && Object.keys(params).length > 0
        ? { ...rest, params: JSON.stringify(params) }
        : rest
    return request(`/api/v1/signals/${encodeURIComponent(symbol)}${qs(encoded)}`)
  },

  // -- watchlists ----------------------------------------------------------

  listWatchlists(params: { symbol?: string } = {}): Promise<WatchlistOut[]> {
    return request(`/api/v1/watchlists${qs(params)}`)
  },

  /**
   * PUT, not POST: the endpoint replaces the whole list, so the same call
   * creates a watchlist or overwrites one. Sending a partial list removes the
   * symbols left out — that is the intent, matching the multiselect it drives.
   */
  saveWatchlist(name: string, symbols: string[]): Promise<WatchlistOut> {
    return request(`/api/v1/watchlists/${encodeURIComponent(name)}`, {
      method: 'PUT',
      body: JSON.stringify({ symbols }),
    })
  },

  deleteWatchlist(name: string): Promise<void> {
    return requestNoContent(`/api/v1/watchlists/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },

  // -- portfolios ----------------------------------------------------------

  listPortfolios(): Promise<PortfolioSummary[]> {
    return request('/api/v1/portfolios')
  },

  /**
   * Derived state: cash, positions, P&L. Nothing here is stored — the trade
   * log is, and the server computes the rest from it.
   */
  getPortfolio(
    name: string,
    params: { include_prices?: boolean } = {},
  ): Promise<PortfolioStateOut> {
    return request(`/api/v1/portfolios/${encodeURIComponent(name)}${qs(params)}`)
  },

  createPortfolio(body: {
    name: string
    initial_cash?: number
  }): Promise<PortfolioSummary> {
    return request('/api/v1/portfolios', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  deletePortfolio(name: string): Promise<void> {
    return requestNoContent(`/api/v1/portfolios/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    })
  },

  listTrades(name: string): Promise<TradeOut[]> {
    return request(`/api/v1/portfolios/${encodeURIComponent(name)}/trades`)
  },

  /**
   * `allow_overdraft` is a query parameter, not part of the body: it is a
   * caller stance ("record this even though cash goes negative"), not a
   * property of the trade.
   */
  addTrade(
    name: string,
    trade: TradeIn,
    options: { allow_overdraft?: boolean } = {},
  ): Promise<TradeOut> {
    return request(
      `/api/v1/portfolios/${encodeURIComponent(name)}/trades${qs(options)}`,
      { method: 'POST', body: JSON.stringify(trade) },
    )
  },

  deleteTrade(name: string, tradeId: string): Promise<void> {
    return requestNoContent(
      `/api/v1/portfolios/${encodeURIComponent(name)}/trades/${encodeURIComponent(tradeId)}`,
      { method: 'DELETE' },
    )
  },

  // -- compare & optimize --------------------------------------------------

  compareStrategies(body: {
    symbol: string
    start: string
    end: string
    strategies: { strategy_id: string; params?: Record<string, unknown>; grid?: Record<string, unknown[]> }[]
    benchmark_symbol?: string | null
    optimize?: boolean
    metric?: string
    include_equity_curves?: boolean
  }): Promise<CompareResponse> {
    return request('/api/v1/backtest/compare', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  optimizeStrategy(body: {
    symbol: string
    strategy_id: string
    start: string
    end: string
    grid: Record<string, unknown[] | { min: number; max: number; step?: number }>
    metric?: string
    top_n?: number
  }): Promise<StrategyOptimizeResponse> {
    return request('/api/v1/optimize/strategy', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  optimizePortfolio(body: {
    symbols: string[]
    start: string
    end: string
    num_portfolios?: number
    include_frontier?: boolean
  }): Promise<PortfolioOptimizeResponse> {
    return request('/api/v1/optimize/portfolio', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  // -- research (the only network-touching endpoints) ----------------------

  getProfile(symbol: string): Promise<Profile> {
    return request(`/api/v1/research/${encodeURIComponent(symbol)}/profile`)
  },

  getFinancials(
    symbol: string,
    params: { quarterly?: boolean } = {},
  ): Promise<Financials> {
    return request(
      `/api/v1/research/${encodeURIComponent(symbol)}/financials${qs(params)}`,
    )
  },

  /**
   * Sources are mutually exclusive and resolved server-side in order:
   * explicit symbols, then portfolio, then watchlist, then market proxies.
   */
  getNews(params: {
    symbols?: string[]
    portfolio?: string
    watchlist?: string
    limit?: number
  } = {}): Promise<NewsFeed> {
    const search = new URLSearchParams()
    for (const symbol of params.symbols ?? []) search.append('symbols', symbol)
    if (params.portfolio) search.set('portfolio', params.portfolio)
    if (params.watchlist) search.set('watchlist', params.watchlist)
    if (params.limit) search.set('limit', String(params.limit))
    const query = search.toString()
    return request(`/api/v1/research/news${query ? `?${query}` : ''}`)
  },

  /** A PREVIEW — it returns orders, it does not record them. */
  previewRebalance(
    name: string,
    body: { target_weights: Record<string, number>; minimum_order_value?: number },
  ): Promise<RebalancePreview> {
    return request(`/api/v1/portfolios/${encodeURIComponent(name)}/rebalance`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },
}
