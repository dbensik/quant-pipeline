/**
 * components/charts/PriceChart.tsx
 * Close-price line for one symbol, with an optional buy/sell signal overlay.
 *
 * Recharts rather than Plotly: react-plotly.js has no official React 19
 * support and this app is on React 19.2. Plotly earns its place on the
 * candlestick/OHLC view where Recharts has no equivalent — the migration guide
 * lists both for exactly this split.
 *
 * Presentational only: it receives bars and signals and renders them. Fetching
 * lives in api/queries.ts, so this can be rendered without a server.
 *
 * Phase 4 — React frontend
 */

import {
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

import type { OHLCVBar } from '@/api/client'
import type { SignalPoint } from '@/api/client'

interface PriceChartProps {
  bars: OHLCVBar[]
  symbol: string
  /** Per-bar strategy signals. Omit for a plain price chart. */
  signals?: SignalPoint[]
}

const currency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 2,
})

const BUY_COLOR = '#16a34a'
const SELL_COLOR = '#dc2626'

interface ChartRow {
  date: string
  close: number
  /** Set only on bars where the signal turns long — drives the buy markers. */
  buy?: number
  /** Set only on bars where the signal turns short/flat-from-long. */
  sell?: number
}

export function PriceChart({ bars, symbol, signals }: PriceChartProps) {
  // Bars with a null close are real: the migration kept partial rows where
  // OHLC was incomplete. Dropping them avoids a line that dives to zero.
  const rows: ChartRow[] = bars
    .filter((bar) => bar.close != null)
    .map((bar) => ({
      date: bar.time.slice(0, 10),
      close: bar.close as number,
    }))

  if (rows.length === 0) {
    return (
      <div className="flex h-80 items-center justify-center text-sm text-muted-foreground">
        No price data in this range.
      </div>
    )
  }

  let buyCount = 0
  let sellCount = 0

  if (signals && signals.length > 0) {
    const byDate = new Map(rows.map((row) => [row.date, row]))

    // Mark TRANSITIONS, not every bar holding a position. A strategy that goes
    // long once and holds emits +1 on hundreds of bars; plotting all of them
    // buries the price line under a solid band of markers. Only the bar where
    // the signal changes is an actual trade decision.
    // Starts at 0, not null: the implicit opening state is flat, so a first
    // signal of +1 IS an entry. Seeding with null instead would silently drop
    // it — Buy and Hold enters on bar one and holds, and would have reported
    // "no signal changes" for a strategy that plainly made one.
    let previous = 0
    for (const point of signals) {
      const value = point.signal
      if (value == null) continue // warm-up bars carry no decision

      if (value !== previous) {
        const row = byDate.get(point.time.slice(0, 10))
        if (row) {
          if (value > previous) {
            row.buy = row.close
            buyCount += 1
          } else {
            row.sell = row.close
            sellCount += 1
          }
        }
      }
      previous = value
    }
  }

  const hasOverlay = buyCount + sellCount > 0

  return (
    <div className="space-y-2">
      <ResponsiveContainer width="100%" height={340}>
        <ComposedChart data={rows} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 12 }}
            // ~8 labels regardless of range length, so a multi-year window does
            // not render an unreadable smear of dates.
            interval={Math.max(0, Math.floor(rows.length / 8) - 1)}
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
            formatter={(value, name) => [
              typeof value === 'number' ? currency.format(value) : '—',
              name,
            ]}
            contentStyle={{
              background: 'var(--color-popover)',
              border: '1px solid var(--color-border)',
              borderRadius: 'var(--radius-md)',
              color: 'var(--color-popover-foreground)',
            }}
          />
          {signals && signals.length > 0 ? <Legend verticalAlign="top" height={28} /> : null}

          <Line
            type="monotone"
            dataKey="close"
            name={symbol}
            stroke="var(--color-chart-2)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />

          {signals && signals.length > 0 ? (
            <>
              <Scatter
                dataKey="buy"
                name="Buy"
                fill={BUY_COLOR}
                shape="triangle"
                isAnimationActive={false}
              />
              <Scatter
                dataKey="sell"
                name="Sell"
                fill={SELL_COLOR}
                shape="triangle"
                isAnimationActive={false}
              />
            </>
          ) : null}
        </ComposedChart>
      </ResponsiveContainer>

      {signals && signals.length > 0 ? (
        <p className="text-xs text-muted-foreground">
          {hasOverlay ? (
            <>
              {buyCount} buy · {sellCount} sell signal
              {buyCount + sellCount === 1 ? '' : 's'} — markers show where the
              signal changes, not every bar it holds.
            </>
          ) : (
            <>
              This strategy produced no signal changes over this range. Try a
              wider window or shorter lookback parameters.
            </>
          )}
        </p>
      ) : null}
    </div>
  )
}
