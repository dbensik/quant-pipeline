/**
 * components/backtest/benchmarkReportRows.ts
 * Wording and rows for the benchmark report (decision 53,
 * research/benchmark-regime-report-2026-10-20.md). Pure, so it can be tested:
 * charts render nothing under jsdom.
 */

import type { BenchmarkReportOut, RegimeOut } from '@/api/client'
import type { Series } from '@/components/charts/comparisonRows'

/** |t| at or above this is distinguishable from zero at about 95%. */
const T_95 = 1.96

const pct = (v: number) => `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`

export function exposureLine(report: BenchmarkReportOut): string {
  if (report.alpha_t == null) {
    return (
      `Beta ${report.beta.toFixed(2)} to ${report.symbol}. No alpha to test: the run's return ` +
      'never moved against cash, so it held nothing in this window.'
    )
  }
  const verdict =
    Math.abs(report.alpha_t) >= T_95
      ? 'distinguishable from zero at 95%'
      : 'not distinguishable from zero'
  return (
    `Beta ${report.beta.toFixed(2)} to ${report.symbol}. Alpha ${pct(report.alpha)} a year ` +
    `(95% interval ${pct(report.alpha_low)} to ${pct(report.alpha_high)}, ` +
    `t ${report.alpha_t.toFixed(2)}): ${verdict}.`
  )
}

const REGIME_LABELS: Record<string, string> = {
  low: 'Calmest third',
  middle: 'Middle third',
  high: 'Most volatile third',
}

export interface RegimeRow {
  name: string
  days: string
  strategySharpe: string
  benchmarkSharpe: string
  strategyReturn: string
  benchmarkReturn: string
}

const withError = (value: number | null | undefined, error: number | null | undefined) =>
  value == null ? '—' : `${value.toFixed(2)} ± ${error == null ? '—' : error.toFixed(2)}`

const yearly = (v: number | null | undefined) => (v == null ? '—' : pct(v))

export function regimeRows(regimes: RegimeOut[]): RegimeRow[] {
  return regimes.map((r) => ({
    name: REGIME_LABELS[r.name] ?? r.name,
    days: `${r.days} (${(r.share * 100).toFixed(0)}%)`,
    strategySharpe: withError(r.strategy_sharpe, r.strategy_sharpe_error),
    benchmarkSharpe: withError(r.benchmark_sharpe, r.benchmark_sharpe_error),
    strategyReturn: yearly(r.strategy_return),
    benchmarkReturn: yearly(r.benchmark_return),
  }))
}

/** The rolling Sharpe as two chart series; a window with no Sharpe is left out. */
export function rollingSeries(report: BenchmarkReportOut, strategyName: string): Series[] {
  const points = (pick: 'strategy' | 'benchmark') =>
    (report.rolling_sharpe ?? []).flatMap((p) => {
      const value = p[pick]
      return value == null ? [] : [{ time: p.time, total: value }]
    })
  const benchmarkName =
    report.symbol === strategyName ? `${report.symbol} (benchmark)` : report.symbol
  return [
    { name: strategyName, points: points('strategy') },
    { name: benchmarkName, points: points('benchmark') },
  ]
}
