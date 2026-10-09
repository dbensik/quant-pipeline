/**
 * components/charts/SmileChart.tsx
 * Implied vol against log-moneyness, one line per selected expiry. Our IV
 * only — see components/options/smileRows.ts for why Yahoo's is not plotted.
 *
 * Presentational only.
 */

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { SmileSeries } from '@/components/options/smileRows'

const COLORS = ['#2563eb', '#16a34a', '#d97706', '#dc2626', '#7c3aed', '#0891b2', '#be185d', '#4b5563']
const pct = (v: number) => `${(v * 100).toFixed(1)}%`

export function SmileChart({ series }: { series: SmileSeries[] }) {
  if (series.length === 0) {
    return (
      <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">
        Select an expiry to draw its smile.
      </div>
    )
  }
  return (
    <ResponsiveContainer width="100%" height={340}>
      <LineChart margin={{ top: 8, right: 16, bottom: 16, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          type="number"
          dataKey="x"
          domain={['dataMin', 'dataMax']}
          tickFormatter={(v: number) => v.toFixed(2)}
          tick={{ fontSize: 12 }}
          label={{ value: 'ln(K / F)', position: 'insideBottomRight', fontSize: 11, offset: -8 }}
        />
        <YAxis type="number" dataKey="iv" domain={['auto', 'auto']} tickFormatter={pct} tick={{ fontSize: 12 }} width={56} />
        <ReferenceLine x={0} stroke="currentColor" strokeDasharray="4 4" className="text-muted-foreground" />
        <Tooltip
          formatter={(value) => pct(Number(value))}
          labelFormatter={(x) => `ln(K/F) ${Number(x).toFixed(3)}`}
        />
        <Legend />
        {series.map((s, i) => (
          <Line
            key={s.expiry}
            data={s.points}
            dataKey="iv"
            name={`${s.expiry} (${s.days}d)`}
            stroke={COLORS[i % COLORS.length]}
            dot={{ r: 1.5 }}
            strokeWidth={1.5}
            isAnimationActive={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  )
}
