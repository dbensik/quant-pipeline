/**
 * components/charts/GreekChart.tsx
 * One Greek of a position before expiry against the underlying price
 * (decision 55), under the chosen volatility rule and, dashed, the other:
 * the same position shows a different delta under the two.
 *
 * Presentational only: the rows come from components/options/positionCurve.ts.
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

import type { GreekPoint } from '@/components/options/positionCurve'

const figure = (v: number) =>
  Math.abs(v) >= 100 ? v.toFixed(0) : Math.abs(v) >= 1 ? v.toFixed(2) : v.toPrecision(3)

export function GreekChart({
  points,
  reference,
  label,
  ruleLabel,
  otherLabel,
}: {
  points: GreekPoint[]
  reference: number
  label: string
  ruleLabel: string
  otherLabel: string
}) {
  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={points} margin={{ top: 16, right: 16, bottom: 16, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          type="number"
          dataKey="x"
          domain={['dataMin', 'dataMax']}
          tickFormatter={(v: number) => v.toFixed(0)}
          tick={{ fontSize: 12 }}
        />
        <YAxis tickFormatter={figure} tick={{ fontSize: 12 }} width={70} />
        <ReferenceLine y={0} stroke="currentColor" className="text-muted-foreground" />
        <ReferenceLine x={reference} stroke="#2563eb" strokeDasharray="4 4" />
        <Tooltip
          formatter={(value) => figure(Number(value))}
          labelFormatter={(x) => `At ${Number(x).toFixed(2)}`}
        />
        <Legend wrapperStyle={{ fontSize: 12 }} />
        <Line
          dataKey="value"
          name={`${label}, ${ruleLabel}`}
          stroke="#7c3aed"
          dot={false}
          strokeWidth={2}
          isAnimationActive={false}
        />
        <Line
          dataKey="other"
          name={`${label}, ${otherLabel}`}
          stroke="#7c3aed"
          strokeOpacity={0.5}
          strokeDasharray="5 4"
          dot={false}
          strokeWidth={1.5}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
