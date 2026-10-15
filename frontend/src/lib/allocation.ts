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
