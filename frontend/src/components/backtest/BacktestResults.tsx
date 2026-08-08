/**
 * components/backtest/BacktestResults.tsx
 * Equity curve + KPI table for a completed backtest.
 *
 * Phase 4 — React frontend
 */

import type { BacktestResult } from '@/api/backtestResult'
import { EquityCurveChart } from '@/components/charts/EquityCurveChart'
import { Badge } from '@/components/ui/badge'

/**
 * Metrics arrive typed as `unknown` because the API declares them as a free
 * dict — PerformanceAnalyzer's keys are not part of the schema. Numbers are
 * formatted by name; anything else is stringified rather than assumed.
 *
 * Null is a real value here, not an oversight: the API converts NaN/Infinity
 * to null (neither is valid JSON), which is what a Sharpe ratio over a
 * zero-variance curve produces.
 */
const PERCENT_METRICS = new Set([
  'Total Return',
  'Annualized Return',
  'Annualized Volatility',
  'Max Drawdown',
])

const CURRENCY_METRICS = new Set(['Final Value'])

const INTEGER_METRICS = new Set(['Trade Count', 'Max Drawdown Duration (Days)'])

function formatMetric(key: string, value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value !== 'number') return String(value)

  if (PERCENT_METRICS.has(key)) {
    return `${(value * 100).toFixed(2)}%`
  }
  if (CURRENCY_METRICS.has(key)) {
    return value.toLocaleString('en-US', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 2,
    })
  }
  if (INTEGER_METRICS.has(key)) {
    return value.toLocaleString('en-US', { maximumFractionDigits: 0 })
  }
  return value.toFixed(3)
}

/** Green for good, red for bad — only where the sign genuinely means that. */
function toneFor(key: string, value: unknown): string {
  if (typeof value !== 'number') return ''
  if (key === 'Max Drawdown' || key === 'Annualized Volatility') return ''
  if (
    key === 'Total Return' ||
    key === 'Annualized Return' ||
    key === 'Sharpe Ratio' ||
    key === 'Sortino Ratio' ||
    key === 'Calmar Ratio'
  ) {
    return value >= 0 ? 'text-green-600' : 'text-red-600'
  }
  return ''
}

export function BacktestResults({ result }: { result: BacktestResult }) {
  // Read from the top-level field, NOT from params: initial capital is not a
  // strategy parameter, and looking for it there silently yields undefined and
  // draws the break-even line at a hardcoded default.
  const initialCapital = result.initialCapital

  const entries = Object.entries(result.metrics)

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{result.symbol}</Badge>
        <Badge variant="secondary">{result.strategyName}</Badge>
        <Badge variant="secondary">{result.bars} bars</Badge>
        <Badge variant="secondary">{result.tradeCount} trades</Badge>
        {/* Seed is shown because it makes the run reproducible — the same
            request with the same seed returns the same numbers. */}
        {result.seed != null ? (
          <Badge variant="secondary" title="Slippage seed — same seed, same result">
            seed {result.seed}
          </Badge>
        ) : null}
      </div>

      <EquityCurveChart
        points={result.equityCurve}
        initialCapital={initialCapital}
      />

      <div>
        <h4 className="mb-2 text-sm font-medium">Performance</h4>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-2 sm:grid-cols-3">
          {entries.map(([key, value]) => (
            <div key={key} className="flex flex-col border-b py-1.5">
              <dt className="text-xs text-muted-foreground">{key}</dt>
              <dd className={`font-mono text-sm ${toneFor(key, value)}`}>
                {formatMetric(key, value)}
              </dd>
            </div>
          ))}
        </dl>
      </div>

      {Object.keys(result.params).length > 0 ? (
        <p className="text-xs text-muted-foreground">
          Parameters used:{' '}
          {Object.entries(result.params)
            .map(([key, value]) => `${key}=${String(value)}`)
            .join(', ')}
        </p>
      ) : null}
    </div>
  )
}
