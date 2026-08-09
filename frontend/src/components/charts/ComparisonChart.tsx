/**
 * components/charts/ComparisonChart.tsx
 * Several equity curves on one axis.
 *
 * Presentational only — row building lives in comparisonRows.ts so it can be
 * tested; charts render nothing under jsdom.
 *
 * Phase 5 — React pages for the ported routers
 */

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { colourFor, comparisonRows, type Series } from './comparisonRows'

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

export function ComparisonChart({ series }: { series: Series[] }) {
  const rows = comparisonRows(series)

  if (rows.length === 0) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        No equity curves to draw.
      </p>
    )
  }

  return (
    <div className="h-[360px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis
            dataKey="time"
            tickFormatter={(value: string) => value.slice(0, 10)}
            minTickGap={40}
            className="text-xs"
          />
          <YAxis
            tickFormatter={(value: number) => currency.format(value)}
            width={80}
            className="text-xs"
          />
          <Tooltip
            formatter={(value, name) => [currency.format(Number(value)), String(name)]}
            labelFormatter={(label) => String(label ?? '').slice(0, 10)}
          />
          <Legend />
          {series.map((s, index) => (
            <Line
              key={s.name}
              type="monotone"
              dataKey={s.name}
              stroke={colourFor(index)}
              dot={false}
              strokeWidth={2}
              // Gaps stay gaps rather than being bridged — a missing value is
              // missing data, not a flat hold.
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
