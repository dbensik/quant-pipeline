/**
 * pages/OptimizePage.tsx
 * Grid-search strategy parameters, or Monte Carlo portfolio weights.
 *
 * Two independent tools on one page because they are what "optimize" means
 * here — but they share no inputs and answer different questions, which is why
 * the API keeps them as separate endpoints.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'

import type { ApiError } from '@/api/client'
import {
  useOptimizePortfolio,
  useOptimizeStrategy,
  useStrategies,
} from '@/api/queries'
import { PageHeader } from '@/components/layout/PageHeader'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useAppStore } from '@/store/useAppStore'

/**
 * `Trade Count` is deliberately absent: the API rejects it, because neither
 * more nor fewer trades is better on its own.
 */
const METRICS = [
  'Sharpe Ratio',
  'Sortino Ratio',
  'Calmar Ratio',
  'Total Return',
  'Annualized Return',
  'Annualized Volatility',
  'Max Drawdown',
]

const RISK_METRICS = new Set(['Annualized Volatility', 'Max Drawdown'])

const percent = (v: number | null | undefined) =>
  v == null ? '—' : `${(v * 100).toFixed(2)}%`
const ratio = (v: number | null | undefined) => (v == null ? '—' : v.toFixed(3))

/** Metrics that read as a share of capital rather than as a ratio. */
const PERCENT_METRICS = new Set([
  'Total Return',
  'Annualized Return',
  'Annualized Volatility',
  'Max Drawdown',
])
const metricValue = (metric: string, v: unknown) =>
  PERCENT_METRICS.has(metric) ? percent(v as number | null) : ratio(v as number | null)
const paramText = (params: Record<string, unknown> | null | undefined) =>
  params
    ? Object.entries(params)
        .map(([k, v]) => `${k}=${v}`)
        .join(', ')
    : 'none (nothing traded)'

export function OptimizePage() {
  const { startDate, endDate } = useAppStore()
  const { data: catalogue } = useStrategies('single')
  const strategyRun = useOptimizeStrategy()
  const portfolioRun = useOptimizePortfolio()

  const [symbol, setSymbol] = useState('AAPL')
  const [strategyId, setStrategyId] = useState('ma_crossover')
  const [metric, setMetric] = useState('Sharpe Ratio')
  const [gridText, setGridText] = useState('')

  const [universe, setUniverse] = useState('AAPL, MSFT, JPM, XOM')
  const [trials, setTrials] = useState('4000')

  const spec = catalogue?.strategies.find((s) => s.id === strategyId)
  const defaultGrid = spec?.default_grid ?? null

  function runStrategy() {
    let grid: Record<string, unknown> = {}
    if (gridText.trim()) {
      try {
        grid = JSON.parse(gridText)
      } catch {
        return
      }
    } else if (defaultGrid) {
      grid = defaultGrid as Record<string, unknown>
    }
    strategyRun.mutate({
      symbol: symbol.toUpperCase().trim(),
      strategy_id: strategyId,
      start: startDate,
      end: endDate,
      grid: grid as never,
      metric,
      top_n: 15,
    })
  }

  function runPortfolio() {
    portfolioRun.mutate({
      symbols: universe
        .split(',')
        .map((s) => s.toUpperCase().trim())
        .filter(Boolean),
      start: startDate,
      end: endDate,
      num_portfolios: Number(trials) || 4000,
      include_frontier: false,
    })
  }

  const gridInvalid = gridText.trim().length > 0 && (() => {
    try {
      JSON.parse(gridText)
      return false
    } catch {
      return true
    }
  })()

  const strategyError = strategyRun.error as ApiError | null
  const portfolioError = portfolioRun.error as ApiError | null
  const result = strategyRun.data
  const walk = result?.walk_forward ?? null

  return (
    <div>
      <PageHeader
        title="Optimize"
        blurb="Grid-search a strategy's parameters, or search for portfolio weights."
      />

      <div className="space-y-6">
        {/* -- parameter grid search ----------------------------------- */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Strategy parameters</CardTitle>
            <CardDescription>
              Every combination faces identical slippage draws, so the ranking
              reflects the parameters rather than the random draw.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-3">
              <div className="space-y-1">
                <Label htmlFor="opt-symbol">Symbol</Label>
                <Input
                  id="opt-symbol"
                  value={symbol}
                  onChange={(e) => setSymbol(e.target.value)}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="opt-strategy">Strategy</Label>
                <select
                  id="opt-strategy"
                  className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
                  value={strategyId}
                  onChange={(e) => {
                    setStrategyId(e.target.value)
                    setGridText('')
                  }}
                >
                  {(catalogue?.strategies ?? []).map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.display_name}
                    </option>
                  ))}
                </select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="opt-metric">Optimize for</Label>
                <select
                  id="opt-metric"
                  className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
                  value={metric}
                  onChange={(e) => setMetric(e.target.value)}
                >
                  {METRICS.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* The risk metrics have a degenerate optimum — the least volatile
                parameter set is usually one that never trades. Said here, not
                only in the API docs, because this is where it is chosen. */}
            {RISK_METRICS.has(metric) ? (
              <Alert>
                <AlertDescription>
                  Ranking by {metric} has a degenerate optimum: the “best” set is
                  usually one that never trades (zero volatility, zero return).
                  Prefer a risk-adjusted metric like Sharpe or Calmar unless you
                  specifically want that.
                </AlertDescription>
              </Alert>
            ) : null}

            <div className="space-y-1">
              <Label htmlFor="opt-grid">Grid</Label>
              <textarea
                id="opt-grid"
                rows={3}
                className="w-full rounded-md border bg-transparent p-2 font-mono text-xs"
                placeholder={
                  defaultGrid
                    ? JSON.stringify(defaultGrid)
                    : '{"window": [10, 20, 30]}'
                }
                value={gridText}
                onChange={(e) => setGridText(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                {defaultGrid
                  ? 'Leave blank to use the registry default shown above.'
                  : 'This strategy has no default sweep — supply one.'}{' '}
                Values are lists, or {'{'}min, max, step{'}'}.
              </p>
              {gridInvalid ? (
                <p className="text-xs text-red-600">Not valid JSON.</p>
              ) : null}
            </div>

            <Button
              onClick={runStrategy}
              disabled={
                strategyRun.isPending ||
                gridInvalid ||
                (!gridText.trim() && !defaultGrid)
              }
            >
              {strategyRun.isPending ? 'Searching…' : 'Run grid search'}
            </Button>

            {strategyError ? (
              <Alert variant="destructive">
                <AlertDescription>{strategyError.detail}</AlertDescription>
              </Alert>
            ) : null}

            {result ? (
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-2 text-sm">
                  <Badge variant="secondary">
                    in-sample best:{' '}
                    {Object.entries(result.best_params)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(', ')}
                  </Badge>
                  <span className="text-muted-foreground">
                    {result.combinations_evaluated} of{' '}
                    {result.combinations_requested} combinations evaluated
                  </span>
                </div>
                <p className="text-xs text-muted-foreground">
                  Idle cash earns a {percent(result.risk_free_rate)} risk-free rate (
                  {result.risk_free_source}); Sharpe is measured against it.
                </p>

                {(result.skipped ?? []).length > 0 ? (
                  <Alert>
                    <AlertDescription>
                      {(result.skipped ?? []).length} combination(s) rejected by the
                      strategy — e.g. {(result.skipped ?? [])[0]?.reason}
                    </AlertDescription>
                  </Alert>
                ) : null}

                {/* Decision 41: the ranking is chosen and measured on one window,
                    a hindsight figure. Walk-forward is what the search would
                    have earned picking each year on the years before it. */}
                {walk ? (
                  <div className="space-y-2 rounded-lg border p-3">
                    <h4 className="font-medium">Out of sample (walk-forward)</h4>
                    <p className="text-sm">
                      Picking each year's parameters on the years before it and trading them
                      from {walk.evaluated_from.slice(0, 10)} to {walk.evaluated_to.slice(0, 10)}{' '}
                      earned a {walk.metric} of{' '}
                      <strong>{metricValue(walk.metric, walk.oos_metrics[walk.metric])}</strong>.
                      The best combination chosen on that same span (
                      {paramText(walk.hindsight_params)}) shows{' '}
                      {metricValue(walk.metric, walk.hindsight_metrics?.[walk.metric])}: hindsight
                      overstates it by {metricValue(walk.metric, walk.hindsight_minus_oos)}.
                    </p>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                          <tr>
                            <th className="py-2 pr-4">Traded</th>
                            <th className="py-2 pr-4">Picked on the years before</th>
                            <th className="py-2 pr-4 text-right">In sample</th>
                            <th className="py-2 pr-4 text-right">Out of sample</th>
                            <th className="py-2 text-right">Return</th>
                          </tr>
                        </thead>
                        <tbody>
                          {walk.folds.map((fold) => (
                            <tr key={fold.validate_start} className="border-b last:border-0">
                              <td className="py-2 pr-4 tabular-nums">
                                {fold.validate_start.slice(0, 10)} → {fold.validate_end.slice(0, 10)}
                              </td>
                              <td className="py-2 pr-4 font-mono text-xs">
                                {paramText(fold.params)}
                              </td>
                              <td className="py-2 pr-4 text-right tabular-nums">
                                {metricValue(walk.metric, fold.train_metric)}
                              </td>
                              <td className="py-2 pr-4 text-right tabular-nums">
                                {metricValue(walk.metric, fold.oos_metrics[walk.metric])}
                              </td>
                              <td className="py-2 text-right tabular-nums">
                                {percent(fold.oos_metrics['Total Return'] as number)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : result.walk_forward_unavailable ? (
                  <Alert>
                    <AlertDescription>
                      No out-of-sample check: {result.walk_forward_unavailable} The ranking
                      below is in-sample only.
                    </AlertDescription>
                  </Alert>
                ) : null}

                <h4 className="font-medium">In-sample ranking</h4>
                <p className="text-xs text-muted-foreground">
                  Each combination is chosen and measured on the same window, so the top
                  rows are partly luck.
                </p>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                      <tr>
                        <th className="py-2 pr-4">Parameters</th>
                        <th className="py-2 pr-4 text-right">{result.metric}</th>
                        <th className="py-2 pr-4 text-right">Total return</th>
                        <th className="py-2 text-right">Trades</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.results.map((row, index) => {
                        const params = Object.entries(row).filter(
                          ([key]) => key in result.best_params,
                        )
                        return (
                          <tr key={index} className="border-b last:border-0">
                            <td className="py-2 pr-4 font-mono text-xs">
                              {params.map(([k, v]) => `${k}=${v}`).join(', ')}
                            </td>
                            <td className="py-2 pr-4 text-right tabular-nums">
                              {ratio(row[result.metric] as number)}
                            </td>
                            <td className="py-2 pr-4 text-right tabular-nums">
                              {percent(row['Total Return'] as number)}
                            </td>
                            <td className="py-2 text-right tabular-nums">
                              {String(row['Trade Count'] ?? '—')}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            ) : null}
          </CardContent>
        </Card>

        {/* -- portfolio weights --------------------------------------- */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Portfolio weights</CardTitle>
            <CardDescription>
              Monte Carlo over random allocations. Seeded, so the same request
              returns the same answer.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-[1fr_160px]">
              <div className="space-y-1">
                <Label htmlFor="opt-universe">Symbols</Label>
                <Input
                  id="opt-universe"
                  value={universe}
                  onChange={(e) => setUniverse(e.target.value)}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="opt-trials">Trials</Label>
                <Input
                  id="opt-trials"
                  inputMode="numeric"
                  value={trials}
                  onChange={(e) => setTrials(e.target.value)}
                />
              </div>
            </div>

            <Button onClick={runPortfolio} disabled={portfolioRun.isPending}>
              {portfolioRun.isPending ? 'Simulating…' : 'Find weights'}
            </Button>

            {portfolioError ? (
              <Alert variant="destructive">
                <AlertDescription>{portfolioError.detail}</AlertDescription>
              </Alert>
            ) : null}

            {portfolioRun.data ? (
              <p className="text-xs text-muted-foreground">
                Scored as a daily-rebalanced backtest without costs: return is compound
                annual growth, and Sharpe is measured against cash earning a{' '}
                {percent(portfolioRun.data.risk_free_rate)} risk-free rate (
                {portfolioRun.data.risk_free_source}), as on every backtest.
              </p>
            ) : null}

            {portfolioRun.data ? (
              <div className="grid gap-4 sm:grid-cols-2">
                {(
                  [
                    ['Max Sharpe', portfolioRun.data.max_sharpe],
                    ['Min volatility', portfolioRun.data.min_volatility],
                  ] as const
                ).map(([label, allocation]) => (
                  <div key={label} className="rounded-lg border p-3">
                    <h4 className="font-medium">{label}</h4>
                    <p className="mt-1 text-sm text-muted-foreground">
                      return {percent(allocation.annualized_return)} · vol{' '}
                      {percent(allocation.annualized_volatility)} · sharpe{' '}
                      {ratio(allocation.sharpe_ratio)}
                    </p>
                    <ul className="mt-2 space-y-1 text-sm">
                      {Object.entries(allocation.weights)
                        .sort((a, b) => b[1] - a[1])
                        .map(([ticker, weight]) => (
                          <li key={ticker} className="flex justify-between">
                            <span>{ticker}</span>
                            <span className="tabular-nums">
                              {(weight * 100).toFixed(1)}%
                            </span>
                          </li>
                        ))}
                    </ul>
                  </div>
                ))}
              </div>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
