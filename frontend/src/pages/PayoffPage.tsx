/**
 * pages/PayoffPage.tsx
 *
 * Multi-leg option positions and their profit at expiry (decision 54,
 * research/option-payoff-2026-10-20.md). A template fills in the legs; any
 * leg can then be edited, added or removed, and the chart and figures follow
 * with no request. Premiums come from one archived capture (a buy pays the
 * ask, a sale receives the bid, or mid), or are typed.
 *
 * The curve before expiry and its Greeks (decision 55,
 * research/option-greeks-2026-10-20.md) come from the API, each leg at its
 * own smile vol: a date slider from the capture to the last expiry, and the
 * volatility rule. It needs a capture, so typed premiums get the payoff only.
 * Calendars give a leg its own expiry; their profit at the near expiry is
 * the curve with the slider there, since the far leg still has a price.
 *
 * Source, day and expiry are this page's own controls, so they live in
 * component state (as on the Options page), not in Zustand.
 */

import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'

import { ApiError } from '@/api/client'
import type { OptionsQuotesResponse } from '@/api/client'
import {
  useOptionQuotes,
  useOptionQuotesFor,
  useOptionsArchive,
  usePositionCurve,
} from '@/api/queries'
import { GreekChart } from '@/components/charts/GreekChart'
import { PayoffChart } from '@/components/charts/PayoffChart'
import { analyse, MULTIPLIER, payoffCurve, payoffWindow, profitAt } from '@/components/options/payoff'
import type { LegKind, Side } from '@/components/options/payoff'
import {
  farExpiry,
  legsOf,
  nearestStrike,
  quotedPremium,
  TEMPLATES,
} from '@/components/options/payoffTemplates'
import type { Basis, DraftLeg } from '@/components/options/payoffTemplates'
import {
  addDays,
  atSpot,
  curvePoints,
  curveRequest,
  daysBetween,
  GREEKS,
  greekPoints,
  lastExpiry,
  oneExpiry,
  VOL_RULES,
} from '@/components/options/positionCurve'
import type { DatedLeg, Greek, VolRule } from '@/components/options/positionCurve'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'

const SELECT = 'h-9 w-full rounded-md border bg-transparent px-3 text-sm'
const SMALL_SELECT = 'h-8 rounded-md border bg-transparent px-2 text-sm'
const TYPED = ''
/** The slider asks for a curve once it has rested this long. */
const SLIDER_REST_MS = 250

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

function useRested<T>(value: T, ms: number): T {
  const [rested, setRested] = useState(value)
  useEffect(() => {
    const t = setTimeout(() => setRested(value), ms)
    return () => clearTimeout(t)
  }, [value, ms])
  return rested
}

// ---------------------------------------------------------------------------

interface CurveArgs {
  legs: DatedLeg[]
  quotes: OptionsQuotesResponse
  reference: number
  window: [number, number]
}

const ruleLabel = (r: VolRule) => (r === 'sticky_strike' ? 'sticky strike' : 'sticky moneyness')

/**
 * The curve before expiry under the chosen rule, and the other rule's beside
 * it for the Greek chart. Called on every render; with no args (typed
 * premiums, a missing quote, stock alone) it asks for nothing.
 */
function useCurveBeforeExpiry(args: CurveArgs | null) {
  const [days, setDays] = useState(0)
  const [volRule, setVolRule] = useState<VolRule>('sticky_strike')
  const last = args ? lastExpiry(args.legs, args.quotes.expiry, args.quotes.day) : null
  const maxDays = args && last ? daysBetween(args.quotes.day, last) : 0
  const shown = Math.min(days, maxDays)
  const rested = useRested(shown, SLIDER_REST_MS)
  const other: VolRule = volRule === 'sticky_strike' ? 'sticky_moneyness' : 'sticky_strike'
  const request = (rule: VolRule) =>
    args
      ? curveRequest({
          ticker: args.quotes.ticker,
          day: args.quotes.day,
          legs: args.legs,
          pageExpiry: args.quotes.expiry,
          asOf: addDays(args.quotes.day, Math.min(rested, maxDays)),
          volRule: rule,
          spot: args.reference,
          window: args.window,
        })
      : null
  const chosen = usePositionCurve(request(volRule))
  const contrast = usePositionCurve(request(other))
  return { args, last, maxDays, shown, setDays, volRule, setVolRule, other, chosen, contrast }
}

type Curve = ReturnType<typeof useCurveBeforeExpiry>

function CurveControls({ curve, captureDay }: { curve: Curve; captureDay: string }) {
  return (
    <div className="grid gap-3 sm:grid-cols-2">
      <div className="space-y-1">
        <Label htmlFor="payoff-date">
          Date: {addDays(captureDay, curve.shown)} ({curve.maxDays - curve.shown} days to{' '}
          {curve.last})
        </Label>
        <input
          id="payoff-date"
          type="range"
          className="w-full"
          min={0}
          max={curve.maxDays}
          step={1}
          value={curve.shown}
          onChange={(e) => curve.setDays(Number(e.target.value))}
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="payoff-vol-rule">Volatility as the spot moves</Label>
        <select
          id="payoff-vol-rule"
          className={SELECT}
          value={curve.volRule}
          onChange={(e) => curve.setVolRule(e.target.value as VolRule)}
        >
          {VOL_RULES.map((r) => (
            <option key={r.id} value={r.id}>
              {r.label}
            </option>
          ))}
        </select>
      </div>
      {curve.chosen.error ? (
        <Alert variant="destructive" className="sm:col-span-2">
          <AlertDescription>{errorText(curve.chosen.error)}</AlertDescription>
        </Alert>
      ) : curve.chosen.isLoading ? (
        <p className="text-sm text-muted-foreground sm:col-span-2" data-testid="curve-loading">
          Pricing the curve before expiry. The first curve on an older capture computes its
          surface: 10–35 s.
        </p>
      ) : null}
    </div>
  )
}

function GreeksCard({ curve, reference }: { curve: Curve; reference: number }) {
  const [greek, setGreek] = useState<Greek>('delta')
  const data = curve.chosen.data
  const here = data ? atSpot(data) : undefined
  if (!curve.args || !data || !here) return null
  const { legs, quotes } = curve.args
  const g = GREEKS.find((x) => x.id === greek) ?? GREEKS[0]
  return (
    <Card>
      <CardHeader>
        <CardTitle>Greeks on {data.as_of}</CardTitle>
        <CardDescription>
          The whole position, each leg at its own smile vol. The dashed line is the same position
          under {ruleLabel(curve.other)}.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <dl className="grid gap-2 text-sm sm:grid-cols-6" data-testid="curve-summary">
          <div>
            <dt className="text-muted-foreground">Profit at {reference.toFixed(2)}</dt>
            <dd>{money(here.profit)}</dd>
          </div>
          {GREEKS.map((x) => (
            <div key={x.id}>
              <dt className="text-muted-foreground">{x.label}</dt>
              <dd title={x.unit}>{here[x.id].toFixed(2)}</dd>
            </div>
          ))}
        </dl>
        <div className="flex items-center gap-2">
          <Label htmlFor="payoff-greek">Greek</Label>
          <select
            id="payoff-greek"
            className={SMALL_SELECT}
            value={greek}
            onChange={(e) => setGreek(e.target.value as Greek)}
          >
            {GREEKS.map((x) => (
              <option key={x.id} value={x.id}>
                {x.label}
              </option>
            ))}
          </select>
          <span className="text-xs text-muted-foreground">{g.unit}</span>
        </div>
        <GreekChart
          points={greekPoints(data, curve.contrast.data, greek)}
          reference={reference}
          label={g.label}
          ruleLabel={ruleLabel(curve.volRule)}
          otherLabel={ruleLabel(curve.other)}
        />
        <table className="w-full text-sm" data-testid="leg-vols">
          <thead className="border-b text-left text-xs uppercase text-muted-foreground">
            <tr>
              <th className="py-1 pr-2">Leg</th>
              <th className="py-1 pr-2">Expiry</th>
              <th className="py-1 pr-2">Vol at {reference.toFixed(2)}</th>
              <th className="py-1 pr-2">Model price</th>
              <th className="py-1">Premium</th>
            </tr>
          </thead>
          <tbody>
            {legs.map((leg, i) => {
              const q = data.legs[i]
              return (
                <tr key={i} className="border-b last:border-0">
                  <td className="py-1 pr-2">
                    {leg.side === 1 ? 'Buy' : 'Sell'} {leg.quantity} {leg.kind}
                    {leg.kind === 'stock' ? '' : ` ${leg.strike}`}
                  </td>
                  <td className="py-1 pr-2">
                    {leg.kind === 'stock' ? '—' : (leg.expiry ?? quotes.expiry)}
                  </td>
                  <td className="py-1 pr-2">
                    {q?.iv == null ? '—' : `${(q.iv * 100).toFixed(1)}%`}
                    {q?.vol_source === 'extrapolated' ? ' (past the last quote: flat)' : ''}
                  </td>
                  <td className="py-1 pr-2">{q ? q.model_price.toFixed(2) : '—'}</td>
                  <td className="py-1">{leg.premium.toFixed(2)}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
        <ul className="list-disc space-y-1 pl-5 text-xs text-muted-foreground">
          <li>
            A model, not a forecast: the curve assumes the volatility rule chosen. Real volatility
            will move.
          </li>
          <li>
            American options are priced as European, at vols with the early-exercise premium
            removed. Out of the money the gap is cents. In the money it is not: on SPY&apos;s
            32-day smile of 2026-10-19 an American put 5–10% in the money is worth $2–3 a share
            more than the curve says.
          </li>
          <li>
            Surfaces are cached for the last 7 captures. On an older one the first curve waits
            10–35 s while its surface is computed.
            {data.cache_hit ? '' : ' This one was just computed.'}
          </li>
        </ul>
      </CardContent>
    </Card>
  )
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
  const far = quotes
    ? farExpiry(quotes.expiries, quotes.expiry, quotes.dividend_date_uncertain)
    : null
  const templates = TEMPLATES.filter((t) => far !== null || !t.legs.some((l) => l.far))
  const template = templates.find((t) => t.id === templateId) ?? templates[0]
  const withIds = (draft: DraftLeg[]): EditLeg[] =>
    draft.map((l) => ({ ...l, id: legIds++, typed: '' }))
  const fromTemplate = (id: string) =>
    withIds(legsOf(templates.find((t) => t.id === id) ?? templates[0], reference, strikes, far))
  // Reset per source and expiry by the parent's `key`, not by an effect.
  const [legs, setLegs] = useState<EditLeg[]>(() => fromTemplate(template.id))

  // A leg on another expiry (a calendar's far leg) is priced from that expiry's quotes.
  const otherExpiries = [
    ...new Set(legs.map((l) => l.expiry).filter((x): x is string => !!x && x !== quotes?.expiry)),
  ].sort()
  const others = useOptionQuotesFor(quotes?.ticker ?? null, quotes?.day ?? null, otherExpiries)
  const quotesOf = (leg: EditLeg): OptionsQuotesResponse | null | undefined => {
    if (!quotes || !leg.expiry || leg.expiry === quotes.expiry) return quotes
    return others[otherExpiries.indexOf(leg.expiry)]?.data
  }
  const loadingOthers = others.some((q) => q.isLoading)

  const quoted = (leg: EditLeg): number | null => {
    if (!quotes) return leg.kind === 'stock' ? reference : null
    const own = quotesOf(leg)
    return own ? quotedPremium(leg, own.quotes, quotes.spot ?? null, basis) : null
  }
  const premiumOf = (leg: EditLeg) => parsed(leg.typed) ?? quoted(leg)

  const priced: DatedLeg[] = []
  const missing: number[] = []
  legs.forEach((leg, i) => {
    const premium = premiumOf(leg)
    if (premium === null) missing.push(i + 1)
    else priced.push({ ...leg, premium })
  })
  const ready = legs.length > 0 && missing.length === 0
  const single = quotes ? oneExpiry(priced, quotes.expiry) : true
  const summary = ready ? analyse(priced) : null
  const curve = ready && single ? payoffCurve(priced, reference) : []
  const hasOption = priced.some((l) => l.kind !== 'stock')
  const before = useCurveBeforeExpiry(
    ready && quotes && hasOption
      ? { legs: priced, quotes, reference, window: payoffWindow(priced, reference) }
      : null,
  )

  // Legs expiring within days of a projected ex-date: the surface's forward
  // there can sit on the wrong side of the dividend, and so the model's calls.
  const nearExDate = quotes
    ? [
        ...new Set(
          legs
            .filter((l) => l.kind !== 'stock')
            .map((l) => l.expiry ?? quotes.expiry)
            .filter((x) => quotes.dividend_date_uncertain.includes(x)),
        ),
      ].sort()
    : []

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
          expiry: null,
        },
      ]),
    ])

  return (
    <div className="space-y-4">
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
                value={template.id}
                onChange={(e) => {
                  onTemplate(e.target.value)
                  setLegs(fromTemplate(e.target.value))
                }}
              >
                {templates.map((t) => (
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
                  {quotes ? <th className="py-1 pr-1">Expiry</th> : null}
                  <th className="py-1 pr-1">Premium</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {legs.map((leg, i) => {
                  const q = quoted(leg)
                  const own = quotesOf(leg)?.quotes.map((x) => x.strike) ?? strikes
                  return (
                    <tr key={leg.id} className="border-b last:border-0">
                      <td className="py-1 pr-1">
                        <select
                          aria-label={`Leg ${i + 1} side`}
                          className={SMALL_SELECT}
                          value={leg.side}
                          onChange={(e) =>
                            update(leg.id, { side: Number(e.target.value) as Side })
                          }
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
                        ) : own.length ? (
                          <select
                            aria-label={`Leg ${i + 1} strike`}
                            className={SMALL_SELECT}
                            value={leg.strike}
                            onChange={(e) => update(leg.id, { strike: Number(e.target.value) })}
                          >
                            {(own.includes(leg.strike) ? own : [...own, leg.strike].sort((a, b) => a - b)).map(
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
                      {quotes ? (
                        <td className="py-1 pr-1">
                          {leg.kind === 'stock' ? (
                            <span className="text-muted-foreground">—</span>
                          ) : (
                            <select
                              aria-label={`Leg ${i + 1} expiry`}
                              className={SMALL_SELECT}
                              value={leg.expiry ?? quotes.expiry}
                              onChange={(e) =>
                                update(leg.id, {
                                  expiry: e.target.value === quotes.expiry ? null : e.target.value,
                                })
                              }
                            >
                              {quotes.expiries
                                .filter((x) => x >= quotes.day)
                                .map((x) => (
                                  <option key={x} value={x}>
                                    {x.slice(5)}
                                  </option>
                                ))}
                            </select>
                          )}
                        </td>
                      ) : null}
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
            {nearExDate.length ? (
              <p className="text-xs text-muted-foreground" data-testid="near-ex-date">
                {nearExDate.join(', ')} expires within days of a projected ex-date, so whether its
                options trade before or after the dividend is uncertain: the curve before expiry
                can price its calls well off the quotes.
              </p>
            ) : null}
            {quotes ? null : (
              <p className="text-xs text-muted-foreground">
                The curve before expiry, its Greeks and calendars need a capture's smile: choose an
                underlying above.
              </p>
            )}
          </CardContent>
        </Card>

        <Card className="lg:col-span-3">
          <CardHeader>
            <CardTitle>{single ? 'Profit at expiry' : 'Profit before expiry'}</CardTitle>
            <CardDescription>
              {single
                ? `The whole position in dollars, from 15% below the lowest strike, breakeven or ${reference.toFixed(2)} to 15% above the highest.`
                : 'The legs expire on different dates, so there is no single payoff: move the date to the near expiry to see the profit there, with the far leg still priced.'}
              {before.args ? ' The purple line is the position on the chosen date.' : ''}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {legs.length === 0 ? (
              <p className="text-sm text-muted-foreground">Add a leg to draw a payoff.</p>
            ) : loadingOthers ? (
              <p className="text-sm text-muted-foreground">Loading the far expiry's quotes.</p>
            ) : missing.length ? (
              <Alert>
                <AlertDescription>
                  No quote for leg {missing.join(', ')}: type its premium.
                </AlertDescription>
              </Alert>
            ) : null}
            {before.args && quotes ? <CurveControls curve={before} captureDay={quotes.day} /> : null}
            {summary ? (
              <>
                <PayoffChart
                  points={curve}
                  reference={reference}
                  breakevens={single ? summary.breakevens : []}
                  before={
                    before.args && before.chosen.data ? curvePoints(before.chosen.data) : undefined
                  }
                  beforeLabel={`On ${before.chosen.data?.as_of ?? ''}`}
                />
                <dl className="grid gap-2 text-sm sm:grid-cols-3" data-testid="payoff-summary">
                  <div>
                    <dt className="text-muted-foreground">Net premium</dt>
                    <dd>
                      {summary.netPremium >= 0
                        ? `${money(summary.netPremium)} paid`
                        : `${money(-summary.netPremium)} received`}
                    </dd>
                  </div>
                  {single ? (
                    <>
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
                    </>
                  ) : null}
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
      <GreeksCard curve={before} reference={reference} />
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
