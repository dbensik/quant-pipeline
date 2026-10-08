/**
 * components/charts/FanChart.tsx
 * Simulated equity percentiles as nested bands, with the median and the
 * realised curve drawn over them and the starting capital as a reference.
 *
 * Presentational only — rows come from components/simulate/fanRows.ts.
 */

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { FanRow } from '@/components/simulate/fanRows'

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

const LABELS: Record<string, string> = {
  outer: '5th–95th',
  inner: '25th–75th',
  p50: 'Median',
  historical: 'Realised',
}

export function FanChart({ rows, initialCapital }: { rows: FanRow[]; initialCapital: number }) {
  if (rows.length === 0) {
    return (
      <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
        No simulation bands returned.
      </div>
    )
  }

  const hasHistorical = rows.some((row) => row.historical !== undefined)

  return (
    <ResponsiveContainer width="100%" height={320}>
      <ComposedChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="step"
          tick={{ fontSize: 12 }}
          interval={Math.max(0, Math.floor(rows.length / 8) - 1)}
          stroke="currentColor"
          className="text-muted-foreground"
          label={{ value: 'trading days', position: 'insideBottomRight', fontSize: 11, offset: -4 }}
        />
        <YAxis
          tick={{ fontSize: 12 }}
          domain={['auto', 'auto']}
          tickFormatter={(value: number) => currency.format(value)}
          width={80}
          stroke="currentColor"
          className="text-muted-foreground"
        />
        <Tooltip
          formatter={(value, name) => {
            const label = LABELS[String(name)] ?? String(name)
            if (Array.isArray(value)) {
              return [`${currency.format(Number(value[0]))} – ${currency.format(Number(value[1]))}`, label]
            }
            return [typeof value === 'number' ? currency.format(value) : '—', label]
          }}
          labelFormatter={(step) => `Day ${String(step)}`}
          contentStyle={{
            background: 'var(--color-popover)',
            border: '1px solid var(--color-border)',
            borderRadius: 'var(--radius-md)',
            color: 'var(--color-popover-foreground)',
          }}
        />
        <Area dataKey="outer" stroke="none" fill="#2563eb" fillOpacity={0.12} isAnimationActive={false} />
        <Area dataKey="inner" stroke="none" fill="#2563eb" fillOpacity={0.22} isAnimationActive={false} />
        <Line dataKey="p50" stroke="#2563eb" dot={false} strokeWidth={1.5} isAnimationActive={false} />
        {hasHistorical ? (
          <Line
            dataKey="historical"
            stroke="currentColor"
            className="text-foreground"
            dot={false}
            strokeWidth={1.5}
            isAnimationActive={false}
            connectNulls={false}
          />
        ) : null}
        <ReferenceLine
          y={initialCapital}
          strokeDasharray="4 4"
          stroke="currentColor"
          className="text-muted-foreground"
          label={{ value: 'Start', position: 'insideTopLeft', fontSize: 11 }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
