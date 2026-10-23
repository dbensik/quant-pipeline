/**
 * components/charts/volumeData.ts
 *
 * Volume for the price charts: which bars have it, which way each bar moved,
 * and what unit the number is in.
 *
 * Pure for the same reason as chartRows.ts and candlestickData.ts: neither
 * Recharts nor Plotly renders under jsdom, so this is the only testable place
 * for it.
 *
 * The unit differs by asset class and the axis must say so. Yahoo serves an
 * equity's or ETF's volume in SHARES (split-adjusted at read time, so a split
 * does not step the series) and a coin's in US DOLLARS of the quote currency:
 * BTC-USD trades ~$30B a day, not 30B coins.
 */

import type { OHLCVBar } from '@/api/client'

/**
 * Which way the bar moved, close against its own open — the candle's colour,
 * so the volume pane agrees with the candles above it. `unknown` when the
 * bar has no open: guessing from the previous close would colour a bar by a
 * rule the candle view does not use.
 */
export type VolumeDirection = 'up' | 'down' | 'unknown'

/** The bar's volume, or null when it has none (missing is never zero). */
export function volumeOf(bar: OHLCVBar): number | null {
  const v = bar.volume
  return v != null && Number.isFinite(v) && v >= 0 ? v : null
}

export function volumeDirection(bar: OHLCVBar): VolumeDirection {
  const { open, close } = bar
  if (open == null || close == null || !Number.isFinite(open) || !Number.isFinite(close)) {
    return 'unknown'
  }
  return close >= open ? 'up' : 'down'
}

export interface VolumeUnit {
  /** Axis title, e.g. "Volume (shares)". */
  label: string
  /** Compact tick/tooltip format: 31.8M or $30.5B. */
  format: (value: number) => string
}

const compact = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
})

export function volumeUnit(assetClass: string | undefined): VolumeUnit {
  if (assetClass === 'crypto') {
    return { label: 'Volume (USD)', format: (value) => `$${compact.format(value)}` }
  }
  return { label: 'Volume (shares)', format: (value) => compact.format(value) }
}

export interface VolumeSeries {
  x: string[]
  volume: number[]
  direction: VolumeDirection[]
  /** Bars in the range with no volume: drawn as gaps, never as zero. */
  missing: number
}

/** Every bar that has a volume, for Plotly's bar trace under the candles. */
export function buildVolumeSeries(bars: OHLCVBar[]): VolumeSeries {
  const series: VolumeSeries = { x: [], volume: [], direction: [], missing: 0 }
  for (const bar of bars) {
    const v = volumeOf(bar)
    if (v == null) {
      series.missing += 1
      continue
    }
    series.x.push(bar.time.slice(0, 10))
    series.volume.push(v)
    series.direction.push(volumeDirection(bar))
  }
  return series
}

/** One line under a chart when some bars carry no volume; null when all do. */
export function missingVolumeNote(missing: number, total: number): string | null {
  if (missing === 0) return null
  if (missing === total) return 'No volume is stored for this range.'
  return `${missing} bar${missing === 1 ? ' has' : 's have'} no volume and ${
    missing === 1 ? 'is' : 'are'
  } left blank in the volume pane.`
}
