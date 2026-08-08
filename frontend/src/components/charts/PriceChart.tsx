/**
 * components/charts/PriceChart.tsx
 * Close-price line chart for one symbol.
 *
 * Recharts rather than Plotly for this first chart: react-plotly.js has no
 * official React 19 support and this app is on React 19.2. Plotly earns its
 * place on the candlestick/OHLC view where Recharts has no equivalent — the
 * migration guide lists both for exactly this split.
 *
 * Presentational only: it receives bars and renders them. Fetching lives in
 * api/queries.ts, so this component can be rendered from a test or a story
 * without a server.
 *
 * Phase 4 — React frontend
 */

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { OHLCVBar } from '@/api/client'

interface PriceChartProps {
  bars: OHLCVBar[]
  symbol: string
}

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 2,
})

export function PriceChart({ bars, symbol }: PriceChartProps) {
  // Bars with a null close are real: the migration kept partial rows where OHLC
  // was incomplete. Dropping them here avoids a line that dives to zero.
  const data = bars
    .filter((bar) => bar.close != null)
    .map((bar) => ({
      date: bar.time.slice(0, 10),
      close: bar.close as number,
    }))

  if (data.length === 0) {
    return (
      <div className="flex h-80 items-center justify-center text-sm text-muted-foreground">
        No price data in this range.
      </div>
    )
  }

  return (
    <ResponsiveContainer width="100%" height={340}>
      <LineChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 12 }}
          // ~8 labels regardless of range length, so a 5-year window does not
          // render an unreadable smear of dates.
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
          // Recharts types the value as possibly-undefined, so narrow rather
          // than assert — an undefined slipping through would render "$NaN".
          formatter={(value) => [
            typeof value === 'number' ? currency.format(value) : '—',
            'Close',
          ]}
          labelClassName="text-foreground"
          contentStyle={{
            background: 'var(--color-popover)',
            border: '1px solid var(--color-border)',
            borderRadius: 'var(--radius-md)',
            color: 'var(--color-popover-foreground)',
          }}
        />
        <Line
          type="monotone"
          dataKey="close"
          name={symbol}
          stroke="var(--color-chart-2)"
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
