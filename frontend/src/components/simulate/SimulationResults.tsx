/**
 * components/simulate/SimulationResults.tsx
 * Fan chart plus the three summary tables for a completed simulation.
 *
 * Presentational. Everything shown here is something the server computed;
 * the request echo is shown as badges so a saved result says what made it.
 */

import type { PercentileSummary, SimulationResponse } from '@/api/client'
import { FanChart } from '@/components/charts/FanChart'
import { buildFanRows } from '@/components/simulate/fanRows'
import { Badge } from '@/components/ui/badge'

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

function percent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  return `${(value * 100).toFixed(digits)}%`
}

function money(value: number): string {
  return currency.format(value)
}

function days(value: number): string {
  return `${Math.round(value).toLocaleString('en-US')} d`
}

const PERCENTILE_COLUMNS: Array<{ key: keyof PercentileSummary; label: string }> = [
  { key: 'p05', label: '5th' },
  { key: 'p25', label: '25th' },
  { key: 'p50', label: 'Median' },
  { key: 'p75', label: '75th' },
  { key: 'p95', label: '95th' },
  { key: 'mean', label: 'Mean' },
]

function PercentileRow({
  label,
  summary,
  format,
}: {
  label: string
  summary: PercentileSummary
  format: (value: number) => string
}) {
  return (
    <tr className="border-b">
      <th scope="row" className="py-1.5 pr-4 text-left text-xs font-medium text-muted-foreground">
        {label}
      </th>
      {PERCENTILE_COLUMNS.map((column) => (
        <td key={column.key} className="py-1.5 pr-4 text-right font-mono text-sm">
          {format(summary[column.key])}
        </td>
      ))}
    </tr>
  )
}

function PercentileTable({ children }: { children: React.ReactNode }) {
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b">
          <th className="py-1.5 pr-4" />
          {PERCENTILE_COLUMNS.map((column) => (
            <th key={column.key} className="py-1.5 pr-4 text-right text-xs font-medium text-muted-foreground">
              {column.label}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  )
}

export function SimulationResults({
  result,
  historicalCurve,
}: {
  result: SimulationResponse
  /** The backtest's equity curve, for the overlay. Empty when none was fetched. */
  historicalCurve: Array<{ total: number }>
}) {
  const rows = buildFanRows({
    bands: result.bands,
    historical: historicalCurve,
    bars: result.bars,
    resampledFrom: result.resampled_from,
    initialCapital: result.initial_capital,
  })
  const { terminal, drawdown, risk } = result

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{result.mode === 'prices' ? 'price paths, strategy re-run' : 'strategy returns'}</Badge>
        <Badge variant="secondary">{result.method}</Badge>
        <Badge variant="secondary">{result.n_paths.toLocaleString('en-US')} paths</Badge>
        <Badge variant="secondary">{result.horizon_days.toLocaleString('en-US')} days</Badge>
        <Badge variant="secondary" title="Daily returns the draws were taken from">
          from {result.resampled_from.toLocaleString('en-US')} returns
        </Badge>
        {result.method === 'stationary' ? (
          <Badge variant="secondary">block {result.block_length_days} d</Badge>
        ) : null}
        {result.seed != null ? <Badge variant="secondary">seed {result.seed}</Badge> : null}
      </div>

      <FanChart rows={rows} initialCapital={result.initial_capital} />

      <div>
        <h4 className="mb-2 text-sm font-medium">Terminal wealth</h4>
        <PercentileTable>
          <PercentileRow label="Final value" summary={terminal.wealth} format={money} />
          <PercentileRow label="Total return" summary={terminal.total_return} format={(v) => percent(v)} />
        </PercentileTable>
        <p className="mt-2 text-xs text-muted-foreground" data-testid="prob-loss">
          P(ending below start): <span className="font-mono">{percent(terminal.prob_loss)}</span>
        </p>
      </div>

      <div>
        <h4 className="mb-2 text-sm font-medium">Drawdown</h4>
        <PercentileTable>
          <PercentileRow label="Max depth" summary={drawdown.depth} format={(v) => percent(v)} />
          <PercentileRow label="Longest underwater" summary={drawdown.duration_bars} format={days} />
        </PercentileTable>
        <p className="mt-2 text-xs text-muted-foreground" data-testid="drawdown-historical">
          Historical: <span className="font-mono">{percent(drawdown.historical_depth)}</span>
          {drawdown.historical_duration_bars != null ? (
            <>
              {' '}over <span className="font-mono">{days(drawdown.historical_duration_bars)}</span>
            </>
          ) : null}
          {drawdown.prob_worse_than_historical != null ? (
            <>
              {' '}· P(worse): <span className="font-mono">{percent(drawdown.prob_worse_than_historical)}</span>
            </>
          ) : null}
        </p>
      </div>

      <div>
        <h4 className="mb-2 text-sm font-medium">Tail risk</h4>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b">
              <th className="py-1.5 pr-4 text-left text-xs font-medium text-muted-foreground">Horizon</th>
              <th className="py-1.5 pr-4 text-right text-xs font-medium text-muted-foreground">VaR 95%</th>
              <th className="py-1.5 pr-4 text-right text-xs font-medium text-muted-foreground">CVaR 95%</th>
              <th className="py-1.5 pr-4 text-right text-xs font-medium text-muted-foreground">VaR 99%</th>
              <th className="py-1.5 pr-4 text-right text-xs font-medium text-muted-foreground">CVaR 99%</th>
            </tr>
          </thead>
          <tbody>
            {risk.rows.map((row) => (
              <tr key={row.horizon_days} className="border-b">
                <th scope="row" className="py-1.5 pr-4 text-left text-xs font-medium text-muted-foreground">
                  {row.horizon_days} day{row.horizon_days === 1 ? '' : 's'}
                </th>
                <td className="py-1.5 pr-4 text-right font-mono text-sm">{percent(row.var_95, 2)}</td>
                <td className="py-1.5 pr-4 text-right font-mono text-sm">{percent(row.cvar_95, 2)}</td>
                <td className="py-1.5 pr-4 text-right font-mono text-sm">{percent(row.var_99, 2)}</td>
                <td className="py-1.5 pr-4 text-right font-mono text-sm">{percent(row.cvar_99, 2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-2 text-xs text-muted-foreground" data-testid="prob-ruin">
          P(ever losing {percent(1 - risk.ruin_threshold, 0)} of start):{' '}
          <span className="font-mono">{percent(risk.prob_ruin)}</span>
        </p>
      </div>
    </div>
  )
}
