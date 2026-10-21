/**
 * components/charts/PayoffChart.tsx
 * Profit at expiry against the underlying price (decision 54), with the
 * reference price and each breakeven marked.
 *
 * Presentational only: the curve is built in components/options/payoff.ts.
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

import type { PayoffPoint } from '@/components/options/payoff'

const dollars = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})
const price = (v: number) => v.toFixed(2)

export function PayoffChart({
  points,
  reference,
  breakevens,
}: {
  points: PayoffPoint[]
  reference: number
  breakevens: number[]
}) {
  return (
    <ResponsiveContainer width="100%" height={340}>
      <LineChart data={points} margin={{ top: 24, right: 16, bottom: 16, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          type="number"
          dataKey="x"
          domain={['dataMin', 'dataMax']}
          tickFormatter={(v: number) => v.toFixed(0)}
          tick={{ fontSize: 12 }}
          label={{
            value: 'Price at expiry',
            position: 'insideBottomRight',
            fontSize: 11,
            offset: -8,
          }}
        />
        <YAxis
          tickFormatter={(v: number) => dollars.format(v)}
          tick={{ fontSize: 12 }}
          width={80}
        />
        <ReferenceLine y={0} stroke="currentColor" className="text-muted-foreground" />
        <ReferenceLine
          x={reference}
          stroke="#2563eb"
          strokeDasharray="4 4"
          label={{ value: price(reference), position: 'top', fontSize: 11 }}
        />
        {breakevens.map((b) => (
          <ReferenceLine key={b} x={b} stroke="#d97706" strokeDasharray="2 3" />
        ))}
        <Tooltip
          formatter={(value) => dollars.format(Number(value))}
          labelFormatter={(x) => `At ${price(Number(x))}`}
        />
        <Line
          dataKey="profit"
          name="Profit at expiry"
          stroke="#16a34a"
          dot={false}
          strokeWidth={2}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
