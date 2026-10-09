/**
 * pages/OptionsPage.tsx
 *
 * The implied-volatility surface for one archived ticker-day: what was used
 * to build it (rate, dividends, quality), the smile per expiry, the ATM term
 * structure over the realised-vol cone, and a pricer that runs every model
 * side by side.
 *
 * Ticker, day and expiry choices are this page's own controls, so they live
 * in component state (as the Simulate panel's do), not in Zustand.
 *
 * Phase 4 of research/option-pricing-plan-2026-10-08.md
 */

import { useMemo, useState } from 'react'

import { ApiError } from '@/api/client'
import type { OptionsExpiry, OptionsSurfaceResponse } from '@/api/client'
import { useOptionsArchive, useOptionsSurface, usePriceOption } from '@/api/queries'
import { SmileChart } from '@/components/charts/SmileChart'
import { TermStructureChart } from '@/components/charts/TermStructureChart'
import { buildConeRows, buildTermRows } from '@/components/options/coneRows'
import { defaultPricerExpiry, prefillFromExpiry } from '@/components/options/pricerPrefill'
import type { PricerInputs } from '@/components/options/pricerPrefill'
import {
  buildSmileSeries,
  DEFAULT_MIN_ABS_DELTA,
  defaultExpiries,
  expiryFlags,
  vendorGap,
} from '@/components/options/smileRows'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'

const pct = (v: number | null | undefined, digits = 1) =>
  v === null || v === undefined ? '—' : `${(v * 100).toFixed(digits)}%`

function errorText(error: unknown): string {
  return error instanceof ApiError ? error.detail : String(error)
}

// ---------------------------------------------------------------------------

function SurfaceSummary({ surface }: { surface: OptionsSurfaceResponse }) {
  const unconverged = surface.expiries.filter((e) => !e.forward_converged).length
  const q = surface.quality
  return (
    <Card>
      <CardHeader>
        <CardTitle>
          {surface.ticker} · {surface.day}
        </CardTitle>
        <CardDescription>
          Captured {new Date(surface.captured_at).toLocaleString()} ·{' '}
          {surface.cache_hit ? 'served from cache' : `priced now in ${surface.compute_seconds.toFixed(1)} s`}
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-3 text-sm sm:grid-cols-3">
        <div>
          <div className="text-muted-foreground">Risk-free rate</div>
          <div data-testid="rate">
            {surface.rate.series} {surface.rate.quoted_pct.toFixed(3)}% (discount) → {pct(surface.rate.continuous, 3)}{' '}
            continuous, observed {surface.rate.observation_date}
            {surface.rate.stale ? <Badge variant="destructive" className="ml-2">stale</Badge> : null}
          </div>
        </div>
        <div>
          <div className="text-muted-foreground">Dividends priced (projected, discrete)</div>
          <div>
            {surface.dividends.length === 0
              ? 'none in the horizon'
              : surface.dividends.map((d) => `${d.ex_date} $${d.amount.toFixed(3)}`).join(' · ')}
          </div>
        </div>
        <div>
          <div className="text-muted-foreground">Quotes</div>
          <div data-testid="quality">
            {q.priced ?? 0} priced of {q.rows ?? 0} · {q.zero_bid ?? 0} no bid · {q.locked ?? 0} locked ·{' '}
            {q.no_iv ?? 0} no IV
            {unconverged > 0 ? <Badge variant="destructive" className="ml-2">{unconverged} forward unconverged</Badge> : null}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function SmilePanel({ surface }: { surface: OptionsSurfaceResponse }) {
  const sorted = useMemo(() => [...surface.expiries].sort((a, b) => a.T - b.T), [surface])
  // Reset per surface by the parent's `key`, not by an effect.
  const [selected, setSelected] = useState<string[]>(() => defaultExpiries(sorted))
  const [wings, setWings] = useState(false)
  const series = useMemo(
    () => buildSmileSeries(surface.smiles, surface.expiries, selected, wings ? 0 : DEFAULT_MIN_ABS_DELTA),
    [surface, selected, wings],
  )
  const gap = useMemo(() => vendorGap(selected.flatMap((e) => surface.smiles[e] ?? [])), [surface, selected])

  const toggle = (expiry: string) =>
    setSelected((cur) => (cur.includes(expiry) ? cur.filter((e) => e !== expiry) : [...cur, expiry]))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Smile</CardTitle>
        <CardDescription>
          Implied vol from out-of-the-money mids, early-exercise premium removed, priced off each expiry's
          parity forward. Yahoo's own IV is not plotted
          {gap === null ? '.' : `; it differs from ours by a median ${gap.toFixed(2)} vol points on these expiries.`}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex flex-wrap gap-1.5">
          {sorted.map((e) => {
            const flags = expiryFlags(e)
            return (
              <Button
                key={e.expiry}
                size="xs"
                variant={selected.includes(e.expiry) ? 'default' : 'outline'}
                onClick={() => toggle(e.expiry)}
                title={flags.join('; ') || undefined}
                aria-pressed={selected.includes(e.expiry)}
              >
                {e.expiry}
                {flags.length > 0 ? ' *' : ''}
              </Button>
            )
          })}
        </div>
        <SmileChart series={series} />
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>* near a projected ex-date, or forward did not converge.</span>
          <label className="flex items-center gap-1.5">
            <input type="checkbox" checked={wings} onChange={(e) => setWings(e.target.checked)} />
            Show far wings (under {DEFAULT_MIN_ABS_DELTA * 100}-delta)
          </label>
        </div>
      </CardContent>
    </Card>
  )
}

function TermPanel({ surface }: { surface: OptionsSurfaceResponse }) {
  const term = useMemo(() => buildTermRows(surface.term_structure), [surface])
  const cone = useMemo(() => buildConeRows(surface.cone), [surface])
  return (
    <Card>
      <CardHeader>
        <CardTitle>Term structure over realised volatility</CardTitle>
        <CardDescription>
          ATM implied vol by calendar days to expiry, over the {surface.ticker} realised-vol cone (Yang–Zhang,
          full stored history to the capture date; windows of 10, 20, 60 and 120 trading days placed at
          their calendar length).
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {surface.cone === null || surface.cone === undefined ? (
          <Alert>
            <AlertTitle>No realised-vol cone</AlertTitle>
            <AlertDescription data-testid="cone-reason">{surface.cone_reason ?? 'Not available.'}</AlertDescription>
          </Alert>
        ) : null}
        <TermStructureChart term={term} cone={cone} />
        <table className="w-full text-sm">
          <thead className="text-muted-foreground">
            <tr>
              <th className="text-left font-normal">Expiry</th>
              <th className="text-right font-normal">Days</th>
              <th className="text-right font-normal">ATM IV</th>
              <th className="text-right font-normal">25Δ skew</th>
            </tr>
          </thead>
          <tbody>
            {term.map((t) => (
              <tr key={t.expiry}>
                <td>
                  {t.expiry}
                  {t.flagged ? ' *' : ''}
                </td>
                <td className="text-right">{Math.round(t.days)}</td>
                <td className="text-right">{pct(t.atmIv, 2)}</td>
                <td className="text-right">{pct(t.skew, 2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </CardContent>
    </Card>
  )
}

const FIELDS: Array<{ key: keyof PricerInputs; label: string; step: string }> = [
  { key: 'spot', label: 'Spot', step: '0.01' },
  { key: 'strike', label: 'Strike', step: '0.5' },
  { key: 'expiryYears', label: 'Years to expiry (ACT/365)', step: '0.001' },
  { key: 'rate', label: 'Rate (continuous)', step: '0.0001' },
  { key: 'dividendYield', label: 'Dividend yield (continuous)', step: '0.0001' },
  { key: 'sigma', label: 'Volatility', step: '0.001' },
]

function PricerPanel({ surface }: { surface: OptionsSurfaceResponse }) {
  const sorted = useMemo(() => [...surface.expiries].sort((a, b) => a.T - b.T), [surface])
  const [expiry, setExpiry] = useState<string | null>(defaultPricerExpiry(sorted)?.expiry ?? null)
  const chosen: OptionsExpiry | undefined = sorted.find((e) => e.expiry === expiry)
  const [inputs, setInputs] = useState<PricerInputs | null>(chosen ? prefillFromExpiry(surface, chosen) : null)
  const chooseExpiry = (value: string | null) => {
    setExpiry(value)
    const next = sorted.find((e) => e.expiry === value)
    if (next) setInputs(prefillFromExpiry(surface, next))
  }
  const [right, setRight] = useState<'call' | 'put'>('put')
  const [style, setStyle] = useState<'european' | 'american'>('american')
  const pricing = usePriceOption()

  const ready = inputs !== null && FIELDS.every((f) => Number.isFinite(inputs[f.key] as number) && inputs[f.key] !== null)

  const run = () => {
    if (!inputs || inputs.sigma === null) return
    pricing.mutate({
      spot: inputs.spot,
      strike: inputs.strike,
      expiry_years: inputs.expiryYears,
      rate: inputs.rate,
      dividend_yield: inputs.dividendYield,
      sigma: inputs.sigma,
      right,
      style,
      mc_paths: 200_000,
      seed: 42,
    })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pricer</CardTitle>
        <CardDescription>
          Pre-filled from one expiry: its implied spot, the ATM vol, and the continuous dividend yield that
          reproduces its parity forward. The surface itself priced discrete projected dividends; one
          continuous yield approximates that.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-1.5">
            <Label htmlFor="pricer-expiry">Pre-fill from expiry</Label>
            <Select value={expiry} onValueChange={(v) => chooseExpiry(v as string | null)}>
              <SelectTrigger id="pricer-expiry" className="w-full">
                <SelectValue placeholder="Expiry">{expiry}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {sorted.map((e) => (
                  <SelectItem key={e.expiry} value={e.expiry}>
                    {e.expiry}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="pricer-right">Right</Label>
            <Select value={right} onValueChange={(v) => setRight(v as 'call' | 'put')}>
              <SelectTrigger id="pricer-right" className="w-full">
                <SelectValue>{right}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="call">call</SelectItem>
                <SelectItem value="put">put</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="pricer-style">Exercise</Label>
            <Select value={style} onValueChange={(v) => setStyle(v as 'european' | 'american')}>
              <SelectTrigger id="pricer-style" className="w-full">
                <SelectValue>{style}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="american">american</SelectItem>
                <SelectItem value="european">european</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {FIELDS.map((f) => (
            <div key={f.key} className="space-y-1.5">
              <Label htmlFor={`pricer-${f.key}`}>{f.label}</Label>
              <Input
                id={`pricer-${f.key}`}
                type="number"
                step={f.step}
                value={inputs?.[f.key] ?? ''}
                onChange={(e) =>
                  setInputs((cur) => (cur ? { ...cur, [f.key]: e.target.value === '' ? null : Number(e.target.value) } : cur))
                }
              />
            </div>
          ))}
        </div>
        <Button onClick={run} disabled={!ready || pricing.isPending} className="w-full">
          {pricing.isPending ? 'Pricing…' : `Price this ${style} ${right}`}
        </Button>
        {pricing.error ? (
          <Alert variant="destructive">
            <AlertDescription>{errorText(pricing.error)}</AlertDescription>
          </Alert>
        ) : null}
        {pricing.data ? (
          <table className="w-full text-sm" data-testid="quotes">
            <thead className="text-muted-foreground">
              <tr>
                <th className="text-left font-normal">Model</th>
                <th className="text-right font-normal">Price</th>
                <th className="text-right font-normal">± SE</th>
                <th className="text-right font-normal">Delta</th>
                <th className="text-right font-normal">Gamma</th>
                <th className="text-right font-normal">Vega</th>
                <th className="text-right font-normal">Early-ex. premium</th>
              </tr>
            </thead>
            <tbody>
              {pricing.data.quotes.map((q) => (
                <tr key={q.model}>
                  <td>{q.display_name}</td>
                  <td className="text-right">{q.price.toFixed(4)}</td>
                  <td className="text-right">{q.std_error == null ? '—' : q.std_error.toFixed(4)}</td>
                  <td className="text-right">{q.delta == null ? '—' : q.delta.toFixed(4)}</td>
                  <td className="text-right">{q.gamma == null ? '—' : q.gamma.toFixed(5)}</td>
                  <td className="text-right">{q.vega == null ? '—' : q.vega.toFixed(4)}</td>
                  <td className="text-right">{q.early_exercise_premium == null ? '—' : q.early_exercise_premium.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------

export function OptionsPage() {
  const archive = useOptionsArchive()
  const tickers = useMemo(() => archive.data?.tickers ?? [], [archive.data])
  // The user's choices; null until they choose. The effective selection falls
  // back to a default derived during render, so no effect has to sync it.
  const [tickerChoice, setTicker] = useState<string | null>(null)
  const [dayChoice, setDay] = useState<string | null>(null)
  const ticker =
    tickerChoice ?? (tickers.length === 0 ? null : tickers.some((t) => t.ticker === 'SPY') ? 'SPY' : tickers[0].ticker)

  // Complete captures only: the API refuses a partial one (404).
  const days = useMemo(
    () =>
      (tickers.find((t) => t.ticker === ticker)?.captures ?? [])
        .filter((c) => !c.partial)
        .map((c) => c.day)
        .sort()
        .reverse(),
    [tickers, ticker],
  )

  const day = dayChoice !== null && days.includes(dayChoice) ? dayChoice : (days[0] ?? null)

  const surface = useOptionsSurface(ticker, day)

  return (
    <div className="space-y-4">
      <Card>
        <CardContent className="grid gap-3 pt-4 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label htmlFor="options-ticker">Underlying</Label>
            {archive.isLoading ? (
              <Skeleton className="h-8 w-full" />
            ) : (
              <Select value={ticker} onValueChange={(v) => setTicker(v as string | null)}>
                <SelectTrigger id="options-ticker" className="w-full">
                  <SelectValue placeholder="Ticker">{ticker}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {tickers.map((t) => (
                    <SelectItem key={t.ticker} value={t.ticker}>
                      {t.ticker}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="options-day">Capture date</Label>
            <Select value={day} onValueChange={(v) => setDay(v as string | null)}>
              <SelectTrigger id="options-day" className="w-full">
                <SelectValue placeholder="Date">{day}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {days.map((d) => (
                  <SelectItem key={d} value={d}>
                    {d}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {archive.error ? (
        <Alert variant="destructive">
          <AlertTitle>Could not list the option archive</AlertTitle>
          <AlertDescription>{errorText(archive.error)}</AlertDescription>
        </Alert>
      ) : null}

      {surface.isLoading ? (
        <Card>
          <CardContent className="space-y-2 pt-4" data-testid="surface-loading">
            <p className="text-sm">
              Loading the {ticker} surface for {day}. The first request for a day prices the whole chain on
              the server and takes 10–35 seconds; after that it is cached.
            </p>
            <Skeleton className="h-64 w-full" />
          </CardContent>
        </Card>
      ) : null}

      {surface.error ? (
        <Alert variant="destructive">
          <AlertTitle>No surface for {ticker} on {day}</AlertTitle>
          <AlertDescription>{errorText(surface.error)}</AlertDescription>
        </Alert>
      ) : null}

      {surface.data ? (
        <>
          <SurfaceSummary surface={surface.data} />
          <SmilePanel key={`smile-${surface.data.ticker}-${surface.data.day}`} surface={surface.data} />
          <TermPanel surface={surface.data} />
          <PricerPanel key={`pricer-${surface.data.ticker}-${surface.data.day}`} surface={surface.data} />
        </>
      ) : null}
    </div>
  )
}
