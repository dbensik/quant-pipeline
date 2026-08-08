/**
 * components/charts/CandlestickChart.tsx
 * OHLC candlestick chart.
 *
 * This is the one place Plotly earns its keep: Recharts has no candlestick
 * trace, and faking one from stacked bars is a well-known dead end. Everything
 * else in this app stays on Recharts.
 *
 * LOAD THIS LAZILY. plotly.js-finance-dist-min is ~1.2 MB — larger than the
 * entire rest of the bundle — so importing it eagerly would more than double
 * the initial download for a view most sessions never open. The lazy boundary
 * lives in PriceCard; keep it there.
 *
 * Phase 4 — React frontend
 */

import Plotly from 'plotly.js-finance-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory'

import type { OHLCVBar } from '@/api/client'
import { buildCandlestickSeries } from '@/components/charts/candlestickData'

// The factory build, not the default export: it binds react-plotly.js to the
// finance dist above rather than pulling in the full plotly.js bundle.
const Plot = createPlotlyComponent(Plotly)

const UP = '#16a34a'
const DOWN = '#dc2626'

export function CandlestickChart({
  bars,
  symbol,
}: {
  bars: OHLCVBar[]
  symbol: string
}) {
  const series = buildCandlestickSeries(bars)

  if (series.x.length === 0) {
    return (
      <div className="flex h-80 items-center justify-center text-sm text-muted-foreground">
        No complete OHLC bars in this range.
      </div>
    )
  }

  return (
    <div className="space-y-2">
      <Plot
        data={[
          {
            type: 'candlestick',
            x: series.x,
            open: series.open,
            high: series.high,
            low: series.low,
            close: series.close,
            name: symbol,
            increasing: { line: { color: UP } },
            decreasing: { line: { color: DOWN } },
          },
        ]}
        layout={{
          autosize: true,
          height: 340,
          margin: { t: 8, r: 16, b: 40, l: 64 },
          // Plotly's default is an opaque white card, which looks wrong inside
          // a themed surface and is unreadable in dark mode.
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          font: { color: 'currentColor', size: 12 },
          showlegend: false,
          xaxis: {
            // The range slider duplicates the date inputs and eats vertical
            // space that the candles need.
            rangeslider: { visible: false },
            gridcolor: 'rgba(128,128,128,0.2)',
          },
          yaxis: { tickprefix: '$', gridcolor: 'rgba(128,128,128,0.2)' },
        }}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: '100%' }}
        useResizeHandler
      />

      {series.droppedIncomplete > 0 ? (
        <p className="text-xs text-muted-foreground">
          {series.droppedIncomplete} bar
          {series.droppedIncomplete === 1 ? '' : 's'} hidden — a candle needs
          all of open/high/low/close, and these are missing at least one. The
          line chart shows them, since it only needs the close.
        </p>
      ) : null}
    </div>
  )
}
