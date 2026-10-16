/**
 * pages/AllocationPage.tsx
 * Backtest a multi-asset allocation rule over a basket, beside the same basket
 * equal-weighted.
 *
 * Until decision 32 (2026-10-15) the allocation strategies were reachable only
 * through POST /api/v1/backtest/portfolio — no page called it, and Compare is
 * single-asset. Trend following on a fixed ETF basket was the one rule the
 * 2010-2026 evaluation found doing what it claims (research/
 * multi-asset-strategies-2026-10-15.md), so it gets a page.
 *
 * Weights are left to the server: each strategy declares its own default
 * (`held_weight`), which is what keeps a run fully invested.
 */

import { useState } from 'react'

import type { ApiError, StrategySchema } from '@/api/client'
import { usePortfolioBacktest, useStrategies } from '@/api/queries'
import { ComparisonChart } from '@/components/charts/ComparisonChart'
import { PageHeader } from '@/components/layout/PageHeader'
import { Alert, AlertDescription } from '@/components/ui/alert'
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
import { ALLOCATION_DEFAULT_START, PRESETS, parseSymbols } from '@/lib/allocation'
import { useAppStore } from '@/store/useAppStore'
import { Caveat } from '@/components/layout/Caveat'

/** The benchmark itself, not a rule to test against it. */
const BENCHMARK_ID = 'index_rebalancing'

const percent = (v: unknown) =>
  typeof v === 'number' ? `${(v * 100).toFixed(2)}%` : '—'
const ratio = (v: unknown) => (typeof v === 'number' ? v.toFixed(3) : '—')

function defaultsOf(spec: StrategySchema | undefined): Record<string, string> {
  return Object.fromEntries(
    (spec?.params ?? []).map((p) => [p.name, String(p.default ?? '')]),
  )
}

function typed(spec: StrategySchema | undefined, raw: Record<string, string>) {
  const out: Record<string, unknown> = {}
  for (const p of spec?.params ?? []) {
    const value = raw[p.name]
    if (value === undefined || value === '') continue
    out[p.name] = p.type === 'str' ? value : Number(value)
  }
  return out
}

export function AllocationPage() {
  const { endDate: sharedEnd } = useAppStore()
  const [startDate, setStartDate] = useState(ALLOCATION_DEFAULT_START)
  const [endDate, setEndDate] = useState(sharedEnd)
  const { data: catalogue } = useStrategies('multi')
  const backtest = usePortfolioBacktest()

  const strategies = (catalogue?.strategies ?? []).filter((s) => s.id !== BENCHMARK_ID)
  const [strategyId, setStrategyId] = useState('asset_class_trend')
  const spec = strategies.find((s) => s.id === strategyId)
  const [symbolsText, setSymbolsText] = useState(PRESETS[0].symbols.join(', '))
  const [paramsFor, setParamsFor] = useState<string>('')
  const [params, setParams] = useState<Record<string, string>>({})

  // Parameters belong to one strategy: picking another starts from its defaults.
  const current = paramsFor === strategyId ? params : defaultsOf(spec)

  function run() {
    backtest.mutate({
      symbols: parseSymbols(symbolsText),
      strategy_id: strategyId,
      start: startDate,
      end: endDate,
      params: typed(spec, current),
    })
  }

  const result = backtest.data
  const error = backtest.error as ApiError | null
  const rows = result
    ? [
        { label: result.strategy.strategy_name, run: result.strategy },
        { label: 'Same basket, equal weight, rebalanced monthly', run: result.benchmark },
      ]
    : []

  return (
    <div>
      <PageHeader
        title="Allocation backtest"
        blurb="A multi-asset rule over a basket, beside the same basket equal-weighted."
      />

      <Card className="mb-6">
        <CardHeader>
          <CardTitle className="text-base">Setup</CardTitle>
          <CardDescription>
            Fills at the next bar's open, with costs; idle cash earns the T-bill rate.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="alloc-symbols">Basket</Label>
            <div className="flex flex-wrap gap-2">
              {PRESETS.map((preset) => (
                <Button
                  key={preset.label}
                  size="sm"
                  variant="outline"
                  onClick={() => setSymbolsText(preset.symbols.join(', '))}
                >
                  {preset.label}
                </Button>
              ))}
            </div>
            <Input
              id="alloc-symbols"
              value={symbolsText}
              onChange={(e) => setSymbolsText(e.target.value)}
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1">
              <Label htmlFor="alloc-start">From</Label>
              <Input
                id="alloc-start"
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="alloc-end">To</Label>
              <Input
                id="alloc-end"
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            These rules decide monthly and need up to a year of history before
            their first decision (a 200-day average), so judge them over several
            years, not one.
          </p>

          <div className="grid gap-3 sm:grid-cols-3">
            <div className="space-y-1">
              <Label htmlFor="alloc-strategy">Strategy</Label>
              <select
                id="alloc-strategy"
                className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
                value={strategyId}
                onChange={(e) => setStrategyId(e.target.value)}
              >
                {strategies.map((s) => (
                  <option key={s.id} value={s.id}>
                    {s.display_name}
                  </option>
                ))}
              </select>
            </div>
            {(spec?.params ?? []).map((p) => (
              <div key={p.name} className="space-y-1">
                <Label htmlFor={`alloc-param-${p.name}`}>{p.label}</Label>
                <Input
                  id={`alloc-param-${p.name}`}
                  type={p.type === 'str' ? 'text' : 'number'}
                  value={current[p.name] ?? ''}
                  onChange={(e) => {
                    setParamsFor(strategyId)
                    setParams({ ...current, [p.name]: e.target.value })
                  }}
                />
              </div>
            ))}
          </div>

          {spec?.description ? (
            <p className="text-sm text-muted-foreground">{spec.description}</p>
          ) : null}
          <Caveat text={spec?.caveat} />

          <Button
            onClick={run}
            disabled={parseSymbols(symbolsText).length < 2 || backtest.isPending}
          >
            {backtest.isPending ? 'Running…' : 'Run backtest'}
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
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Result</CardTitle>
              <CardDescription>
                {result.strategy.bars} bars · Sharpe against a{' '}
                {percent(result.strategy.risk_free_rate)} cash account (
                {result.strategy.risk_free_source})
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                    <tr>
                      <th className="py-2 pr-4">Run</th>
                      <th className="py-2 pr-4 text-right">Sharpe</th>
                      <th className="py-2 pr-4 text-right">Annual return</th>
                      <th className="py-2 pr-4 text-right">Max DD</th>
                      <th className="py-2 pr-4 text-right">Trades</th>
                      <th className="py-2">Weights</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map(({ label, run: r }) => (
                      <tr key={label} className="border-b last:border-0">
                        <td className="py-2 pr-4 font-medium">{label}</td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {ratio(r.metrics['Sharpe Ratio'])}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(r.metrics['Annualized Return'])}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {percent(r.metrics['Max Drawdown'])}
                        </td>
                        <td className="py-2 pr-4 text-right tabular-nums">
                          {String(r.metrics['Trade Count'] ?? '—')}
                        </td>
                        <td className="py-2 text-xs text-muted-foreground">
                          {Object.entries(r.weights)
                            .map(([k, v]) => `${k} ${Number(v).toFixed(2)}`)
                            .join(', ')}
                        </td>
                      </tr>
                    ))}
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
              <ComparisonChart
                series={rows.map(({ label, run: r }) => ({
                  name: label,
                  points: (r.equity_curve ?? []).map((p) => ({
                    time: p.time,
                    total: p.total,
                  })),
                }))}
              />
            </CardContent>
          </Card>
        </div>
      ) : null}
    </div>
  )
}
