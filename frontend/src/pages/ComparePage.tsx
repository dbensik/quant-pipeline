/**
 * pages/ComparePage.tsx
 * Run several strategies on one symbol and rank them.
 *
 * Replaces the Streamlit comparison tab. One symbol, several strategies — that
 * is what makes the equity curves comparable on a single axis.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useMemo, useState } from 'react'

import type { ApiError, ComparisonRow } from '@/api/client'
import { useAssets, useCompare, useStrategies } from '@/api/queries'
import { BenchmarkReport } from '@/components/backtest/BenchmarkReport'
import { ComparisonChart } from '@/components/charts/ComparisonChart'
import { DateRangePresets } from '@/components/layout/DateRangePresets'
import { PageHeader } from '@/components/layout/PageHeader'
import { TickerInput } from '@/components/layout/TickerInput'
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
import { Caveat } from '@/components/layout/Caveat'

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

/** One row's benchmark report, chosen from the ranked rows (decision 53). */
function AgainstBenchmark({ rows }: { rows: ComparisonRow[] }) {
  const [picked, setPicked] = useState('')
  const row = rows.find((r) => r.strategy_id === picked) ?? rows[0]
  if (!row || (!row.benchmark_report && !row.benchmark_unavailable)) return null
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Against the benchmark</CardTitle>
        <CardDescription>
          Beta and alpha above are each row&apos;s; the rolling Sharpe and the volatility split
          are for the strategy chosen here.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <select
          aria-label="Strategy for the benchmark report"
          className="h-9 rounded-md border bg-transparent px-3 text-sm"
          value={row.strategy_id}
          onChange={(e) => setPicked(e.target.value)}
        >
          {rows.map((r) => (
            <option key={r.strategy_id} value={r.strategy_id}>
              {r.strategy_name}
            </option>
          ))}
        </select>
        <BenchmarkReport
          report={row.benchmark_report}
          unavailable={row.benchmark_unavailable}
          strategyName={row.strategy_name}
        />
      </CardContent>
    </Card>
  )
}

export function ComparePage() {
  const { startDate, endDate } = useAppStore()
  const { data: catalogue } = useStrategies('single')
  const compare = useCompare()

  // The shared ticker, so a name chosen on another page is the one compared.
  const symbol = useAppStore((s) => s.selectedSymbol) ?? 'AAPL'
  const setSymbol = useAppStore((s) => s.setSymbol)
  const registry = useAssets()
  const suggestions = useMemo(
    () => (registry.data?.assets ?? []).map((a) => a.symbol).sort(),
    [registry.data],
  )
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
          <DateRangePresets />
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="cmp-symbol">Symbol</Label>
              <TickerInput
                id="cmp-symbol"
                value={symbol}
                onCommit={setSymbol}
                suggestions={suggestions}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="cmp-benchmark">Benchmark (optional; SPY by default)</Label>
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
            Tune each strategy before comparing, walk-forward: each year traded with
            the parameters that ranked best on the years before it (needs three whole
            years before the first year compared)
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
                {result.evaluated_from
                  ? `tuned walk-forward; every row and the benchmark measured from ${result.evaluated_from.slice(0, 10)}, out of sample`
                  : `${result.bars} bars · default parameters`}{' '}
                ·
                idle cash earns a {percent(result.risk_free_rate)} risk-free rate (
                {result.risk_free_source}), and every Sharpe is measured against it
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
                      <th className="py-2 pr-4 text-right">Beta</th>
                      <th className="py-2 pr-4 text-right">Alpha (t)</th>
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
                              walk-forward ({row.combinations_evaluated})
                            </Badge>
                          ) : null}
                          <Caveat text={row.caveat} className="mt-1 font-normal" />
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
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {row.benchmark_report ? row.benchmark_report.beta.toFixed(2) : '—'}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {row.benchmark_report
                            ? `${percent(row.benchmark_report.alpha)} (${row.benchmark_report.alpha_t.toFixed(2)})`
                            : '—'}
                        </td>
                        <td className="py-2 text-xs text-muted-foreground">
                          {Object.entries(row.params)
                            .map(([k, v]) => `${k}=${v}`)
                            .join(', ') || 'none'}
                          {(row.picks ?? []).length > 1
                            ? ` (latest of ${(row.picks ?? []).length} yearly picks)`
                            : ''}
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
                        <td className="py-2 pr-4" />
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

          <AgainstBenchmark rows={result.results} />
        </div>
      ) : null}
    </div>
  )
}
