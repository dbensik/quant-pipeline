/**
 * pages/PayoffPage.tsx
 *
 * Multi-leg option positions and their profit at expiry (decision 54,
 * research/option-payoff-2026-10-20.md). A template fills in the legs; any
 * leg can then be edited, added or removed, and the chart and figures follow
 * with no request. Premiums come from one archived capture (a buy pays the
 * ask, a sale receives the bid, or mid), or are typed.
 *
 * Calendars are not offered: at the near expiry the far leg is still an
 * option with a price, which needs the pricer and a volatility. That is the
 * Greeks task's curve before expiry, not a payoff.
 *
 * Source, day and expiry are this page's own controls, so they live in
 * component state (as on the Options page), not in Zustand.
 */

import { useMemo, useState } from 'react'
import type { ReactNode } from 'react'

import { ApiError } from '@/api/client'
import type { OptionsQuotesResponse } from '@/api/client'
import { useOptionQuotes, useOptionsArchive } from '@/api/queries'
import { PayoffChart } from '@/components/charts/PayoffChart'
import { analyse, MULTIPLIER, payoffCurve, profitAt } from '@/components/options/payoff'
import type { Leg, LegKind, Side } from '@/components/options/payoff'
import {
  legsOf,
  nearestStrike,
  quotedPremium,
  TEMPLATES,
} from '@/components/options/payoffTemplates'
import type { Basis, DraftLeg } from '@/components/options/payoffTemplates'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'

const SELECT = 'h-9 w-full rounded-md border bg-transparent px-3 text-sm'
const SMALL_SELECT = 'h-8 rounded-md border bg-transparent px-2 text-sm'
const TYPED = ''

const dollars = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
})

function money(v: number): string {
  if (v === Infinity || v === -Infinity) return 'Unlimited'
  return dollars.format(v)
}

function errorText(error: unknown): string {
  return error instanceof ApiError ? error.detail : String(error)
}

interface EditLeg extends DraftLeg {
  id: number
  /** What the person typed over the quote; empty means use the quote. */
  typed: string
}

/** Row keys only; never shown. */
let legIds = 0

function parsed(text: string): number | null {
  if (text.trim() === '') return null
  const v = Number(text)
  return Number.isFinite(v) && v >= 0 ? v : null
}

// ---------------------------------------------------------------------------

function Builder({
  quotes,
  reference,
  basis,
  templateId,
  onTemplate,
}: {
  quotes: OptionsQuotesResponse | null
  reference: number
  basis: Basis
  templateId: string
  onTemplate: (id: string) => void
}) {
  const strikes = useMemo(() => quotes?.quotes.map((q) => q.strike) ?? [], [quotes])
  const withIds = (draft: DraftLeg[]): EditLeg[] =>
    draft.map((l) => ({ ...l, id: legIds++, typed: '' }))
  const fromTemplate = (id: string) =>
    withIds(legsOf(TEMPLATES.find((t) => t.id === id) ?? TEMPLATES[0], reference, strikes))
  // Reset per source and expiry by the parent's `key`, not by an effect.
  const [legs, setLegs] = useState<EditLeg[]>(() => fromTemplate(templateId))

  const quoted = (leg: EditLeg): number | null =>
    quotes
      ? quotedPremium(leg, quotes.quotes, quotes.spot ?? null, basis)
      : leg.kind === 'stock'
        ? reference
        : null
  const premiumOf = (leg: EditLeg) => parsed(leg.typed) ?? quoted(leg)

  const priced: Leg[] = []
  const missing: number[] = []
  legs.forEach((leg, i) => {
    const premium = premiumOf(leg)
    if (premium === null) missing.push(i + 1)
    else priced.push({ ...leg, premium })
  })
  const ready = legs.length > 0 && missing.length === 0
  const summary = ready ? analyse(priced) : null
  const curve = ready ? payoffCurve(priced, reference) : []

  const update = (id: number, patch: Partial<EditLeg>) =>
    setLegs((cur) => cur.map((l) => (l.id === id ? { ...l, ...patch } : l)))
  const remove = (id: number) => setLegs((cur) => cur.filter((l) => l.id !== id))
  const add = () =>
    setLegs((cur) => [
      ...cur,
      ...withIds([
        {
          kind: 'call',
          side: 1,
          quantity: 1,
          strike: nearestStrike(reference, strikes, reference),
        },
      ]),
    ])

  return (
    <div className="grid gap-4 lg:grid-cols-5">
      <Card className="lg:col-span-2">
        <CardHeader>
          <CardTitle>Legs</CardTitle>
          <CardDescription>
            Premiums are per share; an option is 100 shares. A typed premium replaces the quote.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-1">
            <Label htmlFor="payoff-template">Structure</Label>
            <select
              id="payoff-template"
              className={SELECT}
              value={templateId}
              onChange={(e) => {
                onTemplate(e.target.value)
                setLegs(fromTemplate(e.target.value))
              }}
            >
              {TEMPLATES.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.label}
                </option>
              ))}
            </select>
          </div>
          <table className="w-full text-sm">
            <thead className="border-b text-left text-xs uppercase text-muted-foreground">
              <tr>
                <th className="py-1 pr-1">Side</th>
                <th className="py-1 pr-1">Type</th>
                <th className="py-1 pr-1">Qty</th>
                <th className="py-1 pr-1">Strike</th>
                <th className="py-1 pr-1">Premium</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {legs.map((leg, i) => {
                const q = quoted(leg)
                return (
                  <tr key={leg.id} className="border-b last:border-0">
                    <td className="py-1 pr-1">
                      <select
                        aria-label={`Leg ${i + 1} side`}
                        className={SMALL_SELECT}
                        value={leg.side}
                        onChange={(e) => update(leg.id, { side: Number(e.target.value) as Side })}
                      >
                        <option value={1}>Buy</option>
                        <option value={-1}>Sell</option>
                      </select>
                    </td>
                    <td className="py-1 pr-1">
                      <select
                        aria-label={`Leg ${i + 1} type`}
                        className={SMALL_SELECT}
                        value={leg.kind}
                        onChange={(e) => {
                          const kind = e.target.value as LegKind
                          update(leg.id, {
                            kind,
                            quantity:
                              kind === 'stock' ? 100 : leg.kind === 'stock' ? 1 : leg.quantity,
                            strike:
                              kind === 'stock' || leg.strike > 0
                                ? leg.strike
                                : nearestStrike(reference, strikes, reference),
                          })
                        }}
                      >
                        <option value="call">Call</option>
                        <option value="put">Put</option>
                        <option value="stock">Stock</option>
                      </select>
                    </td>
                    <td className="py-1 pr-1">
                      <Input
                        aria-label={`Leg ${i + 1} quantity`}
                        className="h-8 w-16"
                        type="number"
                        min={1}
                        step={1}
                        value={leg.quantity}
                        onChange={(e) =>
                          update(leg.id, {
                            quantity: Math.max(1, Math.round(Number(e.target.value))),
                          })
                        }
                      />
                    </td>
                    <td className="py-1 pr-1">
                      {leg.kind === 'stock' ? (
                        <span className="text-muted-foreground">—</span>
                      ) : strikes.length ? (
                        <select
                          aria-label={`Leg ${i + 1} strike`}
                          className={SMALL_SELECT}
                          value={leg.strike}
                          onChange={(e) => update(leg.id, { strike: Number(e.target.value) })}
                        >
                          {(strikes.includes(leg.strike) ? strikes : [...strikes, leg.strike]).map(
                            (k) => (
                              <option key={k} value={k}>
                                {k}
                              </option>
                            ),
                          )}
                        </select>
                      ) : (
                        <Input
                          aria-label={`Leg ${i + 1} strike`}
                          className="h-8 w-20"
                          type="number"
                          min={0}
                          step="any"
                          value={leg.strike}
                          onChange={(e) => update(leg.id, { strike: Number(e.target.value) })}
                        />
                      )}
                    </td>
                    <td className="py-1 pr-1">
                      <Input
                        aria-label={`Leg ${i + 1} premium`}
                        className="h-8 w-20"
                        type="number"
                        min={0}
                        step="any"
                        placeholder={q === null ? 'no quote' : q.toFixed(2)}
                        value={leg.typed}
                        onChange={(e) => update(leg.id, { typed: e.target.value })}
                      />
                    </td>
                    <td className="py-1">
                      <Button
                        size="xs"
                        variant="ghost"
                        aria-label={`Remove leg ${i + 1}`}
                        onClick={() => remove(leg.id)}
                      >
                        ×
                      </Button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <Button size="sm" variant="outline" onClick={add}>
            Add leg
          </Button>
          <p className="text-xs text-muted-foreground">
            Calendars are not offered: at the near expiry the far leg is still an option with a
            price, which needs a pricer and a volatility. They come with the curve before expiry.
          </p>
        </CardContent>
      </Card>

      <Card className="lg:col-span-3">
        <CardHeader>
          <CardTitle>Profit at expiry</CardTitle>
          <CardDescription>
            The whole position in dollars, from 15% below the lowest strike, breakeven or{' '}
            {reference.toFixed(2)} to 15% above the highest.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {legs.length === 0 ? (
            <p className="text-sm text-muted-foreground">Add a leg to draw a payoff.</p>
          ) : missing.length ? (
            <Alert>
              <AlertDescription>
                No quote for leg {missing.join(', ')}: type its premium.
              </AlertDescription>
            </Alert>
          ) : null}
          {summary ? (
            <>
              <PayoffChart points={curve} reference={reference} breakevens={summary.breakevens} />
              <dl className="grid gap-2 text-sm sm:grid-cols-3" data-testid="payoff-summary">
                <div>
                  <dt className="text-muted-foreground">Net premium</dt>
                  <dd>
                    {summary.netPremium >= 0
                      ? `${money(summary.netPremium)} paid`
                      : `${money(-summary.netPremium)} received`}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Max gain</dt>
                  <dd>{money(summary.maxGain)}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Max loss</dt>
                  <dd>{money(summary.maxLoss)}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Breakevens</dt>
                  <dd>
                    {summary.breakevens.length
                      ? summary.breakevens.map((b) => b.toFixed(2)).join(', ')
                      : 'none'}
                  </dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">At {reference.toFixed(2)}</dt>
                  <dd>{money(profitAt(priced, reference))}</dd>
                </div>
                <div>
                  <dt className="text-muted-foreground">Shares controlled</dt>
                  <dd>
                    {priced
                      .reduce((n, l) => n + l.quantity * MULTIPLIER[l.kind], 0)
                      .toLocaleString()}
                  </dd>
                </div>
              </dl>
            </>
          ) : null}
        </CardContent>
      </Card>
    </div>
  )
}

// ---------------------------------------------------------------------------

export function PayoffPage() {
  const archive = useOptionsArchive()
  const tickers = archive.data?.tickers ?? []
  const [picked, setPicked] = useState<string | null>(null)
  const source = picked ?? (tickers.find((t) => t.ticker === 'SPY') ?? tickers[0])?.ticker ?? TYPED
  const [day, setDay] = useState<string | null>(null)
  const [expiry, setExpiry] = useState<string | null>(null)
  const [basis, setBasis] = useState<Basis>('cross')
  const [typedReference, setTypedReference] = useState('100')
  const [templateId, setTemplateId] = useState('long_straddle')

  const quotes = useOptionQuotes(source === TYPED ? null : source, day, expiry)
  const days = (tickers.find((t) => t.ticker === source)?.captures ?? [])
    .filter((c) => !c.partial)
    .map((c) => c.day)
    .sort()
    .reverse()

  const typed = source === TYPED
  const reference = typed ? parsed(typedReference) : (quotes.data?.spot ?? null)

  let body: ReactNode
  if (archive.isLoading || (!typed && quotes.isLoading)) {
    body = <Skeleton className="h-96 w-full" />
  } else if (!typed && quotes.error) {
    body = (
      <Alert variant="destructive">
        <AlertDescription>{errorText(quotes.error)}</AlertDescription>
      </Alert>
    )
  } else if (!reference || reference <= 0) {
    body = (
      <p className="text-sm text-muted-foreground">
        {typed ? 'Type a reference price above zero.' : 'This capture recorded no spot.'}
      </p>
    )
  } else {
    const data = typed ? null : (quotes.data ?? null)
    body = (
      <Builder
        key={data ? `${data.ticker}|${data.day}|${data.expiry}` : TYPED}
        quotes={data}
        reference={reference}
        basis={basis}
        templateId={templateId}
        onTemplate={setTemplateId}
      />
    )
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Premiums</CardTitle>
          <CardDescription>
            {typed
              ? 'Typed by you, for any underlying.'
              : quotes.data
                ? `${quotes.data.ticker} quotes captured ${new Date(quotes.data.captured_at).toLocaleString()}, spot ${quotes.data.spot?.toFixed(2) ?? '—'} as the capture read it.`
                : 'From an archived option-chain capture.'}
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 sm:grid-cols-4">
          <div className="space-y-1">
            <Label htmlFor="payoff-source">Underlying</Label>
            <select
              id="payoff-source"
              className={SELECT}
              value={source}
              onChange={(e) => {
                setPicked(e.target.value)
                setDay(null)
                setExpiry(null)
              }}
            >
              {tickers.map((t) => (
                <option key={t.ticker} value={t.ticker}>
                  {t.ticker}
                </option>
              ))}
              <option value={TYPED}>None: type premiums</option>
            </select>
          </div>
          {typed ? (
            <div className="space-y-1">
              <Label htmlFor="payoff-reference">Reference price</Label>
              <Input
                id="payoff-reference"
                type="number"
                min={0}
                step="any"
                value={typedReference}
                onChange={(e) => setTypedReference(e.target.value)}
              />
            </div>
          ) : (
            <>
              <div className="space-y-1">
                <Label htmlFor="payoff-day">Capture</Label>
                <select
                  id="payoff-day"
                  className={SELECT}
                  value={day ?? quotes.data?.day ?? ''}
                  onChange={(e) => {
                    setDay(e.target.value)
                    setExpiry(null)
                  }}
                >
                  {days.map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="payoff-expiry">Expiry</Label>
                <select
                  id="payoff-expiry"
                  className={SELECT}
                  value={quotes.data?.expiry ?? ''}
                  onChange={(e) => setExpiry(e.target.value)}
                >
                  {(quotes.data?.expiries ?? []).map((x) => (
                    <option key={x} value={x}>
                      {x}
                    </option>
                  ))}
                </select>
              </div>
              <div className="space-y-1">
                <Label htmlFor="payoff-basis">Fill at</Label>
                <select
                  id="payoff-basis"
                  className={SELECT}
                  value={basis}
                  onChange={(e) => setBasis(e.target.value as Basis)}
                >
                  <option value="cross">Ask to buy, bid to sell</option>
                  <option value="mid">Mid</option>
                </select>
              </div>
            </>
          )}
        </CardContent>
      </Card>
      {body}
    </div>
  )
}
