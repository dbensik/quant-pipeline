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
 * Volume is a bar trace on a second y-axis in the bottom fifth of the plot,
 * sharing the x-axis, so zoom and hover stay aligned with the candles.
 *
 * Phase 4 — React frontend
 */

import Plotly from 'plotly.js-finance-dist-min'
import createPlotlyComponent from 'react-plotly.js/factory'

import type { OHLCVBar } from '@/api/client'
import { buildCandlestickSeries } from '@/components/charts/candlestickData'
import {
  buildVolumeSeries,
  missingVolumeNote,
  volumeUnit,
  type VolumeDirection,
} from '@/components/charts/volumeData'

// The factory build, not the default export: it binds react-plotly.js to the
// finance dist above rather than pulling in the full plotly.js bundle.
const Plot = createPlotlyComponent(Plotly)

const UP = '#16a34a'
const DOWN = '#dc2626'
const VOLUME_COLOR: Record<VolumeDirection, string> = {
  up: 'rgba(22,163,74,0.45)',
  down: 'rgba(220,38,38,0.45)',
  unknown: 'rgba(128,128,128,0.45)',
}

export function CandlestickChart({
  bars,
  symbol,
  assetClass,
}: {
  bars: OHLCVBar[]
  symbol: string
  /** Sets the volume unit: shares, or US dollars for crypto. */
  assetClass?: string
}) {
  const series = buildCandlestickSeries(bars)
  const volume = buildVolumeSeries(bars)
  const unit = volumeUnit(assetClass)
  const volumeNote = missingVolumeNote(volume.missing, bars.length)
  const hasVolume = volume.x.length > 0

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
          ...(hasVolume
            ? [
                {
                  type: 'bar' as const,
                  x: volume.x,
                  y: volume.volume,
                  yaxis: 'y2',
                  name: unit.label,
                  marker: { color: volume.direction.map((d) => VOLUME_COLOR[d]) },
                  // Plotly's own number format would print 3.18e+7; the same
                  // compact text as the line view's tooltip instead.
                  text: volume.volume.map(unit.format),
                  hovertemplate: `%{text}<extra>${unit.label}</extra>`,
                  textposition: 'none' as const,
                },
              ]
            : []),
        ]}
        layout={{
          autosize: true,
          height: hasVolume ? 420 : 340,
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
            // Dates under the volume pane, not between the two panes.
            anchor: hasVolume ? 'y2' : 'y',
            gridcolor: 'rgba(128,128,128,0.2)',
          },
          yaxis: {
            tickprefix: '$',
            gridcolor: 'rgba(128,128,128,0.2)',
            domain: hasVolume ? [0.26, 1] : [0, 1],
          },
          yaxis2: {
            domain: [0, 0.2],
            title: { text: unit.label, font: { size: 11 } },
            gridcolor: 'rgba(128,128,128,0.2)',
            nticks: 3,
            tickprefix: assetClass === 'crypto' ? '$' : '',
            // 30B, not Plotly's SI default 30G.
            exponentformat: 'B',
            visible: hasVolume,
          },
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

      {volumeNote ? <p className="text-xs text-muted-foreground">{volumeNote}</p> : null}
    </div>
  )
}
