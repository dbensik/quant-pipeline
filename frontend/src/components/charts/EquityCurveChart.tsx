/**
 * components/charts/EquityCurveChart.tsx
 * Portfolio value over the backtest, against the starting capital.
 *
 * Presentational only — it receives points and draws them.
 *
 * Phase 4 — React frontend
 */

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { EquityPoint } from '@/api/client'

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

const currencyExact = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 2,
})

export function EquityCurveChart({
  points,
  initialCapital,
}: {
  points: EquityPoint[]
  initialCapital: number
}) {
  if (points.length === 0) {
    return (
      <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
        No equity curve returned.
      </div>
    )
  }

  const data = points.map((point) => ({
    date: point.time.slice(0, 10),
    total: point.total,
  }))

  const finalValue = data[data.length - 1].total
  // Green when the strategy ended above its starting capital, red below —
  // reading the sign off the line's colour is faster than off the axis.
  const stroke = finalValue >= initialCapital ? '#16a34a' : '#dc2626'

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 12 }}
          interval={Math.max(0, Math.floor(data.length / 8) - 1)}
          stroke="currentColor"
          className="text-muted-foreground"
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
          formatter={(value) => [
            typeof value === 'number' ? currencyExact.format(value) : '—',
            'Portfolio',
          ]}
          contentStyle={{
            background: 'var(--color-popover)',
            border: '1px solid var(--color-border)',
            borderRadius: 'var(--radius-md)',
            color: 'var(--color-popover-foreground)',
          }}
        />
        {/* Break-even line: without it, a curve that never moves looks the
            same as one that doubled, since the Y axis auto-scales. */}
        <ReferenceLine
          y={initialCapital}
          strokeDasharray="4 4"
          stroke="currentColor"
          className="text-muted-foreground"
          label={{
            value: 'Start',
            position: 'insideTopLeft',
            fontSize: 11,
            fill: 'currentColor',
          }}
        />
        <Line
          type="monotone"
          dataKey="total"
          stroke={stroke}
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
