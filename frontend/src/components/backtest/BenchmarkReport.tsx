/**
 * components/backtest/BenchmarkReport.tsx
 * A run against its benchmark (decision 53,
 * research/benchmark-regime-report-2026-10-20.md): its exposure and alpha,
 * its Sharpe over trailing years, and its Sharpe in the benchmark's calm and
 * volatile spells, each with its error. Shown on the backtest, Compare and
 * Allocation pages.
 */

import type { BenchmarkReportOut } from '@/api/client'
import { ComparisonChart } from '@/components/charts/ComparisonChart'

import { exposureLine, regimeRows, rollingSeries } from './benchmarkReportRows'

export function BenchmarkReport({
  report,
  unavailable,
  strategyName,
}: {
  report: BenchmarkReportOut | null | undefined
  unavailable?: string | null
  strategyName: string
}) {
  if (!report) {
    return unavailable ? (
      <p className="text-sm text-muted-foreground">No benchmark report: {unavailable}</p>
    ) : null
  }
  const rows = regimeRows(report.regimes ?? [])
  const years = Math.round(report.rolling_window / report.periods_per_year)
  return (
    <div className="space-y-3">
      <div className="text-sm font-medium">Against {report.symbol}</div>
      <p className="text-sm">{exposureLine(report)}</p>
      <p className="text-xs text-muted-foreground">
        Daily returns above cash, {report.start.slice(0, 10)} to {report.end.slice(0, 10)} (
        {report.bars} days); R² {report.r_squared.toFixed(2)}. Beta is how much of the
        benchmark&apos;s move the run carried; alpha is what it earned beyond that.
      </p>
      {report.rolling_sharpe?.length ? (
        <div>
          <div className="text-sm">
            Sharpe over the trailing {years === 1 ? 'year' : `${years} years`}
          </div>
          <ComparisonChart series={rollingSeries(report, strategyName)} format="number" />
        </div>
      ) : (
        <p className="text-xs text-muted-foreground">
          Too short for a rolling Sharpe ({report.rolling_window} bars).
        </p>
      )}
      {rows.length ? (
        <div className="space-y-1">
          <div className="text-sm">By the benchmark&apos;s volatility before each day</div>
          <table className="w-full text-sm">
            <thead className="border-b text-left text-xs uppercase text-muted-foreground">
              <tr>
                <th className="py-2 pr-4">Days ranked</th>
                <th className="py-2 pr-4">Count</th>
                <th className="py-2 pr-4 text-right">Sharpe</th>
                <th className="py-2 pr-4 text-right">{report.symbol} Sharpe</th>
                <th className="py-2 pr-4 text-right">Excess return a year</th>
                <th className="py-2 text-right">{report.symbol}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.name} className="border-b last:border-0">
                  <td className="py-1 pr-4">{r.name}</td>
                  <td className="py-1 pr-4 tabular-nums text-muted-foreground">{r.days}</td>
                  <td className="py-1 pr-4 text-right tabular-nums">{r.strategySharpe}</td>
                  <td className="py-1 pr-4 text-right tabular-nums">{r.benchmarkSharpe}</td>
                  <td className="py-1 pr-4 text-right tabular-nums">{r.strategyReturn}</td>
                  <td className="py-1 text-right tabular-nums">{r.benchmarkReturn}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="text-xs text-muted-foreground">
            Each day is ranked by the benchmark&apos;s volatility over the month before it, against
            earlier days only. ± is one standard error: two Sharpes closer than about twice it are
            not told apart by this split.
            {report.unranked_days
              ? ` ${report.unranked_days} early days had too little history to rank.`
              : ''}
          </p>
        </div>
      ) : null}
    </div>
  )
}
