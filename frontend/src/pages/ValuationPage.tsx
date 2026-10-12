/**
 * pages/ValuationPage.tsx
 *
 * A discounted cash flow valuation of one equity, as of a date, from SEC
 * filings known by then. Every number is shown with what produced it: the
 * inputs and their sources, the discount rate, the bridge from cash flows to
 * a price, the sensitivity grid, and the value against the price at every
 * past filing. A refusal (a bank, a REIT, no capex on file) shows its reason;
 * a negative or missing value is explained, never shown as a price.
 *
 * Symbol, date and assumption edits are this page's own controls, so they
 * live in component state, not Zustand.
 *
 * Phase 5 of research/dcf-plan-2026-10-08.md
 */

import { useMemo, useState } from 'react'

import { ApiError } from '@/api/client'
import type { ValuationRequest, ValuationResponse } from '@/api/client'
import { useAssets, useValuation, useValuationHistory } from '@/api/queries'
import { ValueHistoryChart } from '@/components/charts/ValueHistoryChart'
import {
  bridgeRows,
  formatMoney,
  gridRows,
  historyRows,
  inputLines,
  parseOverride,
  verdict,
} from '@/components/valuation/valuationRows'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'

/** Today's LOCAL date: toISOString() is UTC and reads tomorrow in a US evening. */
const today = () => {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}
const pct = (v: number | null | undefined, d = 1) => (v === null || v === undefined ? '—' : `${(v * 100).toFixed(d)}%`)

function errorText(error: unknown): string {
  return error instanceof ApiError ? error.detail : String(error)
}

// ---------------------------------------------------------------------------

type OverrideKey = 'growth_first' | 'growth_terminal' | 'wacc' | 'beta' | 'exit_multiple' | 'years'
const OVERRIDES: Array<{ key: OverrideKey; label: string; step: string; pctInput: boolean }> = [
  { key: 'growth_first', label: 'First-year growth (%)', step: '0.1', pctInput: true },
  { key: 'growth_terminal', label: 'Terminal growth (%)', step: '0.1', pctInput: true },
  { key: 'wacc', label: 'Discount rate, WACC (%)', step: '0.1', pctInput: true },
  { key: 'beta', label: 'Beta', step: '0.01', pctInput: false },
  { key: 'exit_multiple', label: 'Exit multiple (× EBITDA)', step: '0.5', pctInput: false },
  { key: 'years', label: 'Projection years', step: '1', pctInput: false },
]

function computedValue(v: ValuationResponse, key: OverrideKey): number | null {
  const x = v.result.inputs
  switch (key) {
    case 'growth_first': return x.growth_first
    case 'growth_terminal': return x.growth_terminal
    case 'wacc': return x.wacc
    case 'beta': return v.lines.beta?.value ?? null
    case 'exit_multiple': return x.exit_multiple ?? null
    case 'years': return x.years
  }
}

function Assumptions({ v, onApply }: { v: ValuationResponse; onApply: (o: Partial<ValuationRequest>) => void }) {
  const [text, setText] = useState<Record<OverrideKey, string>>({
    growth_first: '', growth_terminal: '', wacc: '', beta: '', exit_multiple: '', years: '',
  })
  const apply = () => {
    const out: Partial<ValuationRequest> = {}
    for (const f of OVERRIDES) {
      const n = parseOverride(text[f.key], f.pctInput)
      if (n !== undefined) (out as Record<string, number>)[f.key] = f.key === 'years' ? Math.round(n) : n
    }
    onApply(out)
  }
  return (
    <Card>
      <CardHeader>
        <CardTitle>Assumptions</CardTitle>
        <CardDescription>
          Blank uses the computed value, shown as the placeholder. Overridden: {v.overridden.length ? v.overridden.join(', ') : 'none'}.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid gap-3 sm:grid-cols-3">
          {OVERRIDES.map((f) => {
            const c = computedValue(v, f.key)
            const placeholder = c === null ? '—' : f.pctInput ? (c * 100).toFixed(2) : String(Number(c.toFixed(3)))
            return (
              <div key={f.key} className="space-y-1.5">
                <Label htmlFor={`ov-${f.key}`}>{f.label}</Label>
                <Input
                  id={`ov-${f.key}`}
                  type="number"
                  step={f.step}
                  placeholder={placeholder}
                  value={text[f.key]}
                  onChange={(e) => setText((t) => ({ ...t, [f.key]: e.target.value }))}
                />
              </div>
            )
          })}
        </div>
        <div className="flex gap-2">
          <Button onClick={apply}>Recalculate</Button>
          <Button
            variant="outline"
            onClick={() => {
              setText({ growth_first: '', growth_terminal: '', wacc: '', beta: '', exit_multiple: '', years: '' })
              onApply({})
            }}
          >
            Reset to computed
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

function Headline({ v }: { v: ValuationResponse }) {
  const h = verdict(v)
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          {v.symbol} · as of {v.as_of}
        </CardTitle>
        <CardDescription>
          CIK {v.cik}
          {v.predecessor_ciks.length ? ` (with predecessor ${v.predecessor_ciks.join(', ')})` : ''} · {v.sector ?? 'no sector'} ·
          price ${v.price.toFixed(2)} as traded on {v.price_date}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {h.kind === 'value' ? (
          <div className="text-2xl font-semibold" data-testid="headline">
            ${h.perShare.toFixed(2)} per share{' '}
            <span className={h.upside >= 0 ? 'text-green-600' : 'text-red-600'}>
              ({h.upside >= 0 ? '+' : ''}{(h.upside * 100).toFixed(0)}% vs price)
            </span>
          </div>
        ) : (
          <Alert data-testid="headline">
            <AlertTitle>{h.kind === 'negative' ? 'No meaningful value' : 'No value'}</AlertTitle>
            <AlertDescription>{h.why}</AlertDescription>
          </Alert>
        )}
        <div className="text-sm text-muted-foreground">
          The price implies first-year growth of{' '}
          <strong>{v.implied_growth_first === null || v.implied_growth_first === undefined ? '—' : pct(v.implied_growth_first)}</strong>
          {v.implied_reason ? ` (${v.implied_reason})` : ''}, fading to {pct(v.result.inputs.growth_terminal)} over{' '}
          {v.result.inputs.years} years. Exit-multiple cross-check: {v.result.exit_per_share == null ? '—' : `$${v.result.exit_per_share.toFixed(2)}`}.
        </div>
        {[...v.result.warnings, ...v.notes].map((w) => (
          <Badge key={w} variant="outline" className="mr-1 whitespace-normal text-left">
            {w}
          </Badge>
        ))}
      </CardContent>
    </Card>
  )
}

function Inputs({ v }: { v: ValuationResponse }) {
  const lines = inputLines(v)
  const d = v.discount
  return (
    <Card>
      <CardHeader>
        <CardTitle>Inputs and where each came from</CardTitle>
        <CardDescription>
          WACC {pct(d.wacc, 2)} = {pct(d.weight_equity, 0)} equity at {pct(d.cost_of_equity, 2)} + {pct(d.weight_debt, 0)} debt at{' '}
          {pct(d.cost_of_debt_after_tax, 2)} after tax{d.cost_of_debt_estimated ? ' (cost of debt estimated)' : ''}.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <table className="w-full text-sm" data-testid="inputs">
          <tbody>
            {lines.map((l) => (
              <tr key={l.key} className="align-top">
                <td className="py-0.5 pr-3">{l.label}</td>
                <td className="py-0.5 pr-3 text-right font-mono">{l.display}</td>
                <td className="py-0.5 text-xs text-muted-foreground">{l.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-2 text-xs text-muted-foreground">
          Betas computed:{' '}
          {Object.entries(v.betas)
            .map(([k, b]) => `${k.replace(/_/g, ' ')} ${b ? b.beta.toFixed(2) : '—'}`)
            .join(' · ')}
        </p>
      </CardContent>
    </Card>
  )
}

function Bridge({ v }: { v: ValuationResponse }) {
  const rows = bridgeRows(v)
  if (rows.length === 0) return null
  return (
    <Card>
      <CardHeader>
        <CardTitle>From cash flows to a price</CardTitle>
        <CardDescription>
          Terminal value is {pct(v.result.terminal_share, 0)} of enterprise value.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <table className="w-full text-sm" data-testid="bridge">
          <tbody>
            {rows.map((r) => (
              <tr key={r.label} className={r.kind === 'flow' ? '' : 'font-medium'}>
                <td className="py-0.5">{r.label}</td>
                <td className="py-0.5 text-right font-mono">
                  {r.kind === 'result' ? `$${r.value.toFixed(2)}` : formatMoney(r.value)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

function Sensitivity({ v }: { v: ValuationResponse }) {
  const rows = gridRows(v.sensitivity, v.price)
  const growths = v.sensitivity.growth_terminal
  return (
    <Card>
      <CardHeader>
        <CardTitle>Sensitivity</CardTitle>
        <CardDescription>Value per share by WACC (rows) and terminal growth (columns); colour is against the price. Base case outlined.</CardDescription>
      </CardHeader>
      <CardContent>
        <table className="w-full text-center text-sm" data-testid="sensitivity">
          <thead className="text-muted-foreground">
            <tr>
              <th className="text-left font-normal">WACC \ g</th>
              {growths.map((g) => (
                <th key={g} className="font-normal">{pct(g, 1)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.wacc}>
                <td className="text-left text-muted-foreground">{pct(row.wacc, 1)}</td>
                {row.cells.map((c) => (
                  <td
                    key={c.growth}
                    className={[
                      'font-mono',
                      c.upside === null ? 'text-muted-foreground' : c.upside >= 0 ? 'bg-green-500/15' : 'bg-red-500/15',
                      c.base ? 'outline outline-2 outline-foreground' : '',
                    ].join(' ')}
                    title={c.value === null ? 'WACC at or below growth: no value' : undefined}
                  >
                    {c.value === null ? '—' : `$${c.value.toFixed(0)}`}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

function History({ symbol }: { symbol: string }) {
  const history = useValuationHistory(symbol)
  const { rows, skipped } = useMemo(() => historyRows(history.data?.points ?? []), [history.data])
  return (
    <Card>
      <CardHeader>
        <CardTitle>Value against price over time</CardTitle>
        <CardDescription>
          Re-valued the day after each 10-K and 10-Q, using only what was filed by then.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {history.isLoading ? <Skeleton className="h-64 w-full" /> : null}
        {history.error ? (
          <Alert variant="destructive">
            <AlertDescription>{errorText(history.error)}</AlertDescription>
          </Alert>
        ) : null}
        {history.data ? <ValueHistoryChart rows={rows} /> : null}
        {skipped.length ? (
          <p className="text-xs text-muted-foreground" data-testid="history-skipped">
            Not valued: {skipped.map(([why, n]) => `${n} × ${why}`).join('; ')}.
          </p>
        ) : null}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------

export function ValuationPage() {
  const assets = useAssets(undefined, 'equity')
  const symbols = useMemo(() => (assets.data?.assets ?? []).map((a) => a.symbol).sort(), [assets.data])
  const [symbol, setSymbol] = useState<string | null>(null)
  const [asOf, setAsOf] = useState<string>(today())
  const [overrides, setOverrides] = useState<Partial<ValuationRequest>>({})
  const chosen = symbol ?? (symbols.includes('AAPL') ? 'AAPL' : (symbols[0] ?? null))

  const request: ValuationRequest | null = chosen ? { symbol: chosen, as_of: asOf, ...overrides } : null
  const valuation = useValuation(request)

  return (
    <div className="space-y-4">
      <Card>
        <CardContent className="grid gap-3 pt-4 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label htmlFor="val-symbol">Equity</Label>
            {assets.isLoading ? (
              <Skeleton className="h-8 w-full" />
            ) : (
              <Select
                value={chosen}
                onValueChange={(s) => {
                  setSymbol(s as string | null)
                  setOverrides({})
                }}
              >
                <SelectTrigger id="val-symbol" className="w-full">
                  <SelectValue placeholder="Symbol">{chosen}</SelectValue>
                </SelectTrigger>
                <SelectContent className="max-h-72">
                  {symbols.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="val-asof">As of</Label>
            <Input id="val-asof" type="date" value={asOf} max={today()} onChange={(e) => setAsOf(e.target.value || today())} />
          </div>
        </CardContent>
      </Card>

      {valuation.isLoading ? <Skeleton className="h-40 w-full" data-testid="valuation-loading" /> : null}

      {valuation.error ? (
        <Alert variant={valuation.error instanceof ApiError && valuation.error.status === 422 ? 'default' : 'destructive'} data-testid="refusal">
          <AlertTitle>{chosen} cannot be valued here</AlertTitle>
          <AlertDescription>{errorText(valuation.error)}</AlertDescription>
        </Alert>
      ) : null}

      {valuation.data ? (
        <>
          <Headline v={valuation.data} />
          <Assumptions key={`${chosen}-${asOf}`} v={valuation.data} onApply={setOverrides} />
          <div className="grid gap-4 lg:grid-cols-2">
            <Inputs v={valuation.data} />
            <div className="space-y-4">
              <Bridge v={valuation.data} />
              <Sensitivity v={valuation.data} />
            </div>
          </div>
        </>
      ) : null}

      {chosen && !(valuation.error instanceof ApiError && valuation.error.status === 404) ? <History symbol={chosen} /> : null}
    </div>
  )
}
