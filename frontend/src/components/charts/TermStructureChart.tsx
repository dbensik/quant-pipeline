/**
 * components/charts/TermStructureChart.tsx
 * ATM implied vol by days to expiry, drawn over the realised-vol cone
 * (min-max and p25-p75 bands, median, and today's realised value per window).
 * Both on calendar days: components/options/coneRows.ts converts the cone.
 *
 * Presentational only.
 */

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import { termYDomain } from '@/components/options/coneRows'
import type { ConeRow, TermRow } from '@/components/options/coneRows'

const pct = (v: number) => `${(v * 100).toFixed(1)}%`

export function TermStructureChart({ term, cone }: { term: TermRow[]; cone: ConeRow[] }) {
  if (term.length === 0 && cone.length === 0) {
    return <div className="flex h-72 items-center justify-center text-sm text-muted-foreground">No term structure.</div>
  }
  return (
    <ResponsiveContainer width="100%" height={320}>
      <ComposedChart margin={{ top: 8, right: 16, bottom: 16, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          type="number"
          dataKey="days"
          domain={[0, 'dataMax']}
          tickFormatter={(v: number) => String(Math.round(v))}
          tick={{ fontSize: 12 }}
          label={{ value: 'calendar days', position: 'insideBottomRight', fontSize: 11, offset: -8 }}
        />
        <YAxis type="number" domain={termYDomain(term, cone)} allowDataOverflow tickFormatter={pct} tick={{ fontSize: 12 }} width={56} />
        <Tooltip formatter={(value) => (Array.isArray(value) ? value.map((v) => pct(Number(v))).join(' – ') : pct(Number(value)))} />
        <Legend />
        {/* The min-max band runs off the top on purpose: see termYDomain. */}
        {cone.length > 0 ? (
          <>
            <Area data={cone} dataKey="outer" name="Realised min–max" fill="#94a3b8" fillOpacity={0.15} stroke="none" isAnimationActive={false} />
            <Area data={cone} dataKey="inner" name="Realised 25th–75th" fill="#94a3b8" fillOpacity={0.3} stroke="none" isAnimationActive={false} />
            <Line data={cone} dataKey="median" name="Realised median" stroke="#64748b" strokeDasharray="4 4" dot={false} isAnimationActive={false} />
            <Scatter data={cone} dataKey="current" name="Realised now" fill="#0f172a" isAnimationActive={false} />
          </>
        ) : null}
        <Line data={term} dataKey="atmIv" name="ATM implied" stroke="#2563eb" strokeWidth={2} dot={{ r: 2 }} connectNulls={false} isAnimationActive={false} />
      </ComposedChart>
    </ResponsiveContainer>
  )
}
