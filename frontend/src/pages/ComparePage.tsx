/**
 * pages/ComparePage.tsx
 * Run several strategies on one symbol and rank them.
 *
 * Replaces the Streamlit comparison tab. One symbol, several strategies — that
 * is what makes the equity curves comparable on a single axis.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'

import type { ApiError } from '@/api/client'
import { useCompare, useStrategies } from '@/api/queries'
import { ComparisonChart } from '@/components/charts/ComparisonChart'
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

const METRICS = [
  'Sharpe Ratio',
  'Sortino Ratio',
  'Calmar Ratio',
  'Total Return',
  'Annualized Return',
  'Annualized Volatility',
  'Max Drawdown',
]

const percent = (v: number | null | undefined) =>
  v == null ? '—' : `${(v * 100).toFixed(2)}%`
const ratio = (v: number | null | undefined) => (v == null ? '—' : v.toFixed(3))

export function ComparePage() {
  const { startDate, endDate } = useAppStore()
  const { data: catalogue } = useStrategies('single')
  const compare = useCompare()

  const [symbol, setSymbol] = useState('AAPL')
  const [benchmark, setBenchmark] = useState('')
  const [chosen, setChosen] = useState<string[]>(['ma_crossover', 'mean_reversion'])
  const [metric, setMetric] = useState('Sharpe Ratio')
  const [optimize, setOptimize] = useState(false)

  function run() {
    compare.mutate({
      symbol: symbol.toUpperCase().trim(),
      start: startDate,
      end: endDate,
      strategies: chosen.map((id) => ({ strategy_id: id })),
      benchmark_symbol: benchmark.trim() ? benchmark.toUpperCase().trim() : null,
      optimize,
      metric,
      include_equity_curves: true,
    })
  }

  const result = compare.data
  const error = compare.error as ApiError | null

  // One series per strategy plus the benchmark, on a shared axis — which is
  // the whole reason the endpoint takes one symbol and many strategies.
  const series = result
    ? [
        ...result.results.map((row) => ({
          name: row.strategy_name,
          points: row.equity_curve ?? [],
        })),
        ...(result.benchmark
          ? [
              {
                name: `Benchmark (${result.benchmark.symbol})`,
                points: result.benchmark.equity_curve ?? [],
              },
            ]
          : []),
      ]
    : []

  return (
    <div>
      <PageHeader
        title="Compare strategies"
        blurb="Several strategies, one symbol, one window — ranked."
      />

      <Card className="mb-6">
        <CardHeader>
          <CardTitle className="text-base">Setup</CardTitle>
          <CardDescription>
            Window comes from the shared date range ({startDate} → {endDate}).
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="cmp-symbol">Symbol</Label>
              <Input
                id="cmp-symbol"
                value={symbol}
                onChange={(e) => setSymbol(e.target.value)}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="cmp-benchmark">Benchmark (optional)</Label>
              <Input
                id="cmp-benchmark"
                value={benchmark}
                placeholder="e.g. MSFT"
                onChange={(e) => setBenchmark(e.target.value)}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="cmp-metric">Rank by</Label>
              <select
                id="cmp-metric"
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

          <div className="space-y-2">
            <Label>Strategies</Label>
            <div className="flex flex-wrap gap-2">
              {(catalogue?.strategies ?? []).map((s) => {
                const active = chosen.includes(s.id)
                return (
                  <Button
                    key={s.id}
                    size="sm"
                    variant={active ? 'secondary' : 'outline'}
                    onClick={() =>
                      setChosen((c) =>
                        active ? c.filter((x) => x !== s.id) : [...c, s.id],
                      )
                    }
                  >
                    {s.display_name}
                    {/* default_grid is what a tune would sweep; a strategy
                        without one runs on its params and reports tuned=false. */}
                    {s.default_grid ? ' ⚙' : ''}
                  </Button>
                )
              })}
            </div>
            <p className="text-xs text-muted-foreground">
              ⚙ marks strategies the registry can auto-tune.
            </p>
          </div>

          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={optimize}
              onChange={(e) => setOptimize(e.target.checked)}
            />
            Tune each strategy before comparing, so the ranking is not just
            measuring whose defaults suited this window
          </label>

          <Button
            onClick={run}
            disabled={chosen.length === 0 || compare.isPending}
          >
            {compare.isPending ? 'Running…' : `Compare ${chosen.length} strategies`}
          </Button>
        </CardContent>
      </Card>

      {error ? (
        <Alert variant="destructive" className="mb-6">
          <AlertDescription>{error.detail}</AlertDescription>
        </Alert>
      ) : null}

      {result ? (
        <div className="space-y-6">
          {(result.skipped ?? []).length > 0 ? (
            <Alert>
              <AlertDescription>
                {/* Reported rather than dropped — a comparison missing an entry
                    should say why. */}
                Not compared:{' '}
                {(result.skipped ?? [])
                  .map((s) => `${s.strategy_id} (${s.reason})`)
                  .join('; ')}
              </AlertDescription>
            </Alert>
          ) : null}

          <Card>
            <CardHeader>
              <CardTitle className="text-base">
                Ranked by {result.metric}
              </CardTitle>
              <CardDescription>
                {result.bars} bars · {result.optimized ? 'tuned' : 'default parameters'}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                    <tr>
                      <th className="py-2 pr-4">#</th>
                      <th className="py-2 pr-4">Strategy</th>
                      <th className="py-2 pr-4 text-right">Sharpe</th>
                      <th className="py-2 pr-4 text-right">Total return</th>
                      <th className="py-2 pr-4 text-right">Max DD</th>
                      <th className="py-2 pr-4 text-right">Trades</th>
                      <th className="py-2">Parameters</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.results.map((row, index) => (
                      <tr key={row.strategy_id} className="border-b last:border-0">
                        <td className="py-2 pr-4 text-muted-foreground">
                          {index + 1}
                        </td>
                        <td className="py-2 pr-4 font-medium">
                          {row.strategy_name}
                          {row.tuned ? (
                            <Badge variant="secondary" className="ml-2">
                              tuned ({row.combinations_evaluated})
                            </Badge>
                          ) : null}
                          {row.caveat ? (
                            <div className="text-xs text-amber-600">{row.caveat}</div>
                          ) : null}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {ratio(row.metrics['Sharpe Ratio'] as number)}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(row.metrics['Total Return'] as number)}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(row.metrics['Max Drawdown'] as number)}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {String(row.metrics['Trade Count'] ?? '—')}
                        </td>
                        <td className="py-2 text-xs text-muted-foreground">
                          {Object.entries(row.params)
                            .map(([k, v]) => `${k}=${v}`)
                            .join(', ') || 'none'}
                        </td>
                      </tr>
                    ))}
                    {result.benchmark ? (
                      <tr className="bg-muted/40">
                        <td className="py-2 pr-4" />
                        <td className="py-2 pr-4 font-medium">
                          Benchmark ({result.benchmark.symbol})
                          <Badge variant="outline" className="ml-2">
                            buy &amp; hold
                          </Badge>
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {ratio(result.benchmark.metrics['Sharpe Ratio'] as number)}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(result.benchmark.metrics['Total Return'] as number)}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(result.benchmark.metrics['Max Drawdown'] as number)}
                        </td>
                        <td className="py-2 pr-4" />
                        <td className="py-2" />
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Equity curves</CardTitle>
            </CardHeader>
            <CardContent>
              <ComparisonChart series={series} />
            </CardContent>
          </Card>
        </div>
      ) : null}
    </div>
  )
}
