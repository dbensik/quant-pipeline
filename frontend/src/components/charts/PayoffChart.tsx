/**
 * components/charts/PayoffChart.tsx
 * Profit at expiry against the underlying price (decision 54), with the
 * reference price and each breakeven marked, and the curve before expiry
 * over it (decision 55).
 *
 * Presentational only: profit at expiry is built in
 * components/options/payoff.ts, the curve before expiry by the API. Each line
 * carries its own data, since the two are drawn on different grids.
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
  before,
  beforeLabel = 'Before expiry',
}: {
  /** Profit at expiry; empty when the legs expire on different dates. */
  points: PayoffPoint[]
  reference: number
  breakevens: number[]
  before?: PayoffPoint[]
  beforeLabel?: string
}) {
  return (
    <ResponsiveContainer width="100%" height={340}>
      <LineChart margin={{ top: 24, right: 16, bottom: 16, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          type="number"
          dataKey="x"
          domain={['dataMin', 'dataMax']}
          tickFormatter={(v: number) => v.toFixed(0)}
          tick={{ fontSize: 12 }}
          label={{
            value: points.length ? 'Price at expiry' : 'Price',
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
        {before?.length ? (
          <Line
            data={before}
            dataKey="profit"
            name={beforeLabel}
            stroke="#7c3aed"
            dot={false}
            strokeWidth={2}
            isAnimationActive={false}
          />
        ) : null}
        <Line
          data={points}
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
