/**
 * components/valuation/valuationRows.ts
 *
 * Everything the Valuation page computes from an API response, pure so it is
 * testable without a DOM. The page shows numbers only with what produced
 * them: no headline without its inputs, no grid cell without its WACC and
 * growth, and a negative or missing value is EXPLAINED, never shown as a
 * price. (Phase 6 of research/dcf-plan-2026-10-08.md found 35 names whose
 * free cash flow is negative: a DCF has nothing to grow there.)
 */

import type { ValuationGrid, ValuationHistoryPoint, ValuationResponse } from '@/api/client'

export type Verdict =
  | { kind: 'value'; perShare: number; price: number; upside: number }
  | { kind: 'negative'; perShare: number; price: number; why: string }
  | { kind: 'none'; price: number; why: string }

export function verdict(v: ValuationResponse): Verdict {
  const r = v.result
  if (r.per_share === null || r.per_share === undefined) {
    return { kind: 'none', price: v.price, why: r.reason ?? 'no value could be formed' }
  }
  if (r.per_share < 0 || r.inputs.base_fcf <= 0) {
    return {
      kind: 'negative',
      perShare: r.per_share,
      price: v.price,
      why:
        r.inputs.base_fcf <= 0
          ? 'Free cash flow is negative: a DCF has nothing to grow, so this is not a price.'
          : 'Net debt exceeds the value of the cash flows: the model gives the equity no value.',
    }
  }
  return { kind: 'value', perShare: r.per_share, price: v.price, upside: r.per_share / v.price - 1 }
}

export interface BridgeRow {
  label: string
  value: number
  kind: 'flow' | 'subtotal' | 'adjust' | 'result'
}

/** Present value of each year, the terminal value, then EV -> equity -> per share. */
export function bridgeRows(v: ValuationResponse): BridgeRow[] {
  const r = v.result
  if (r.enterprise_value == null || r.equity_value == null || r.per_share == null || r.pv_terminal == null) return []
  const years: BridgeRow[] = r.pv_fcf.map((pv, i) => ({ label: `Year ${i + 1} FCF, discounted`, value: pv, kind: 'flow' }))
  return [
    ...years,
    { label: 'Terminal value, discounted', value: r.pv_terminal, kind: 'flow' },
    { label: 'Enterprise value', value: r.enterprise_value, kind: 'subtotal' },
    { label: r.inputs.net_debt >= 0 ? 'Less net debt' : 'Plus net cash', value: -r.inputs.net_debt, kind: 'adjust' },
    { label: 'Equity value', value: r.equity_value, kind: 'subtotal' },
    { label: `Per share (÷ ${formatCount(r.inputs.shares)} shares)`, value: r.per_share, kind: 'result' },
  ]
}

export interface GridCell {
  growth: number
  value: number | null
  /** value / price - 1, null where there is no value. */
  upside: number | null
  base: boolean
}

export interface GridRow {
  wacc: number
  base: boolean
  cells: GridCell[]
}

/** The sensitivity grid with the base case marked and each cell against the price. */
export function gridRows(grid: ValuationGrid, price: number): GridRow[] {
  const mid = (n: number) => Math.floor(n / 2)
  return grid.wacc.map((w, i) => ({
    wacc: w,
    base: i === mid(grid.wacc.length),
    cells: grid.growth_terminal.map((g, j) => {
      const value = grid.per_share[i]?.[j] ?? null
      return {
        growth: g,
        value,
        upside: value === null ? null : value / price - 1,
        base: i === mid(grid.wacc.length) && j === mid(grid.growth_terminal.length),
      }
    }),
  }))
}

export interface HistoryRow {
  asOf: string
  price: number | null
  value: number | null
}

/**
 * One row per filing date that has a price. A point the API could not value
 * keeps its price and a null value (a gap), and its reason is counted.
 */
export function historyRows(points: ValuationHistoryPoint[]): { rows: HistoryRow[]; skipped: Array<[string, number]> } {
  const rows: HistoryRow[] = []
  const skipped = new Map<string, number>()
  for (const p of points) {
    if (p.price === null || p.price === undefined) {
      const why = (p.reason ?? 'unknown').replace(/ on or before \d{4}-\d{2}-\d{2}/, '').replace(/only \d+ common sessions/, 'too few common sessions')
      skipped.set(why, (skipped.get(why) ?? 0) + 1)
      continue
    }
    rows.push({ asOf: p.as_of, price: p.price, value: p.per_share ?? null })
  }
  return { rows, skipped: [...skipped.entries()].sort((a, b) => b[1] - a[1]) }
}

const LINE_ORDER: Array<[string, string, 'money' | 'pct' | 'count' | 'number']> = [
  ['cfo', 'Operating cash flow', 'money'],
  ['capex', 'Capital expenditure', 'money'],
  ['after_tax_interest', 'After-tax interest (added back)', 'money'],
  ['share_based_comp', 'Stock-based compensation', 'money'],
  ['base_fcf', 'Base free cash flow', 'money'],
  ['growth_first', 'First-year growth', 'pct'],
  ['tax_rate', 'Tax rate', 'pct'],
  ['beta', 'Beta', 'number'],
  ['risk_free', 'Risk-free rate', 'pct'],
  ['total_debt', 'Total debt', 'money'],
  ['net_debt', 'Net debt', 'money'],
  ['shares', 'Shares', 'count'],
  ['ebitda', 'EBITDA (exit multiple base)', 'money'],
]

export interface InputLine {
  key: string
  label: string
  display: string
  source: string
}

/** The inputs in reading order, each with its source as the API reported it. */
export function inputLines(v: ValuationResponse): InputLine[] {
  return LINE_ORDER.filter(([k]) => k in v.lines).map(([k, label, kind]) => {
    const line = v.lines[k]
    return { key: k, label, display: format(line.value, kind), source: line.source }
  })
}

export function format(value: number | null | undefined, kind: 'money' | 'pct' | 'count' | 'number'): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  if (kind === 'pct') return `${(value * 100).toFixed(2)}%`
  if (kind === 'count') return formatCount(value)
  if (kind === 'number') return value.toFixed(3)
  return formatMoney(value)
}

export function formatMoney(v: number): string {
  const a = Math.abs(v)
  const sign = v < 0 ? '−' : ''
  if (a >= 1e12) return `${sign}$${(a / 1e12).toFixed(2)}T`
  if (a >= 1e9) return `${sign}$${(a / 1e9).toFixed(2)}B`
  if (a >= 1e6) return `${sign}$${(a / 1e6).toFixed(1)}M`
  return `${sign}$${a.toFixed(2)}`
}

export function formatCount(v: number): string {
  if (v >= 1e9) return `${(v / 1e9).toFixed(3)}B`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`
  return v.toFixed(0)
}

/** Assumption form text -> request value; percentages are entered as percent. Blank or junk -> undefined. */
export function parseOverride(text: string, pctInput: boolean): number | undefined {
  if (text.trim() === '') return undefined
  const n = Number(text)
  if (!Number.isFinite(n)) return undefined
  return pctInput ? n / 100 : n
}
