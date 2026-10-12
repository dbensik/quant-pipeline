/**
 * components/charts/ValueHistoryChart.tsx
 * DCF value per share against the price as traded, the day after each
 * 10-K/10-Q. A filing that could not be valued is a gap, not a zero.
 *
 * Presentational only; rows come from components/valuation/valuationRows.ts.
 */

import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

import type { HistoryRow } from '@/components/valuation/valuationRows'

const usd = (v: number) => `$${v.toFixed(0)}`

export function ValueHistoryChart({ rows }: { rows: HistoryRow[] }) {
  if (rows.length === 0) {
    return <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">No priced filing dates.</div>
  }
  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis dataKey="asOf" tick={{ fontSize: 11 }} minTickGap={32} />
        <YAxis tickFormatter={usd} tick={{ fontSize: 12 }} width={64} domain={['auto', 'auto']} />
        <Tooltip formatter={(v) => (v === null || v === undefined ? '—' : `$${Number(v).toFixed(2)}`)} />
        <Legend />
        <Line dataKey="price" name="Price as traded" stroke="#64748b" dot={false} strokeWidth={1.5} isAnimationActive={false} />
        <Line dataKey="value" name="DCF value per share" stroke="#2563eb" dot={{ r: 2 }} strokeWidth={2} connectNulls={false} isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  )
}
