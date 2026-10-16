/**
 * lib/allocation.ts
 * Basket presets and symbol parsing for the Allocation page (decision 32).
 * Kept out of the page module so it exports only a component.
 */

/** Baskets fixed in advance (Faber, 2007), not chosen on the data they are tested on. */
export const PRESETS: { label: string; symbols: string[] }[] = [
  { label: 'Faber 5', symbols: ['SPY', 'EFA', 'IEF', 'VNQ', 'DBC'] },
  { label: 'Faber 7', symbols: ['SPY', 'EFA', 'IEF', 'VNQ', 'DBC', 'GLD', 'TLT'] },
  { label: 'SPY / TLT', symbols: ['SPY', 'TLT'] },
]

export function parseSymbols(text: string): string[] {
  return [
    ...new Set(
      text
        .split(/[\s,]+/)
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean),
    ),
  ]
}

/**
 * The Allocation page's own default start, not the shared one-year range: these
 * rules decide monthly and warm up for up to a year (a 200-day average), so on
 * one year a trend rule cannot trade at all — seen on the page 2026-10-15, 0
 * trades and cash interest only. 2011 is where the decision 29 measurement
 * started, after the 2010 warm-up.
 */
export const ALLOCATION_DEFAULT_START = '2011-01-01'

