/**
 * pages/ScreenersPage.tsx
 * Filter a universe down by momentum, volatility or fundamentals.
 *
 * Screeners COMPOSE: each step filters the survivors of the last, so the
 * per-step counts show where a screen emptied. That is the whole reason the
 * API returns `steps` rather than only the final list.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'
import { X } from 'lucide-react'

import type { ApiError, ParamSchema } from '@/api/client'
import { useRunScreen, useScreeners, useWatchlists } from '@/api/queries'
import { PageHeader } from '@/components/layout/PageHeader'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useAppStore } from '@/store/useAppStore'

/**
 * Indexes whose membership the API resolves AS OF the window's start
 * (decision 42). The S&P 500 falls back to reconstructed month-end lists
 * before the first daily snapshot; the others are refused before it.
 */
const INDEXES = [
  { id: 'sp500', label: 'S&P 500 as of the start date (reconstructed from 2014-12)' },
  { id: 'dow_jones', label: 'Dow Jones as of the start date (observed from 2026-08-09)' },
  { id: 'top_100_crypto', label: 'Top 100 crypto as of the start date (observed from 2026-08-09)' },
]

interface Step {
  screener_id: string
  params: Record<string, number>
}

function ParamField({
  step,
  spec,
  onChange,
}: {
  step: Step
  spec: ParamSchema
  onChange: (name: string, value: number) => void
}) {
  const value = step.params[spec.name] ?? (spec.default as number)
  return (
    <div className="space-y-1">
      <Label htmlFor={`${step.screener_id}-${spec.name}`} className="text-xs">
        {spec.label}
      </Label>
      <Input
        id={`${step.screener_id}-${spec.name}`}
        inputMode="decimal"
        value={String(value)}
        min={spec.minimum ?? undefined}
        max={spec.maximum ?? undefined}
        onChange={(e) => onChange(spec.name, Number(e.target.value))}
      />
    </div>
  )
}

export function ScreenersPage() {
  const { startDate, endDate } = useAppStore()
  const { data: catalogue } = useScreeners()
  const { data: watchlists } = useWatchlists()
  const run = useRunScreen()

  const [universe, setUniverse] = useState('AAPL, MSFT, NVDA, JPM, XOM, KO, PG')
  const [index, setIndex] = useState('')
  const [includePegged, setIncludePegged] = useState(false)
  const [steps, setSteps] = useState<Step[]>([])

  function addStep(screenerId: string) {
    setSteps((current) => [...current, { screener_id: screenerId, params: {} }])
  }

  function setParam(index: number, name: string, value: number) {
    setSteps((current) =>
      current.map((step, i) =>
        i === index ? { ...step, params: { ...step.params, [name]: value } } : step,
      ),
    )
  }

  const symbols = universe
    .split(',')
    .map((s) => s.toUpperCase().trim())
    .filter(Boolean)

  const error = run.error as ApiError | null
  const result = run.data

  return (
    <div>
      <PageHeader
        title="Screeners"
        blurb="Steps compose — each filters the survivors of the last."
      />

      <Card className="mb-6">
        <CardHeader>
          <CardTitle className="text-base">Universe and filters</CardTitle>
          <CardDescription>
            Window {startDate} → {endDate}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1">
            <Label htmlFor="screen-index">Universe</Label>
            <select
              id="screen-index"
              className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
              value={index}
              onChange={(e) => setIndex(e.target.value)}
            >
              <option value="">Symbols typed below</option>
              {INDEXES.map((i) => (
                <option key={i.id} value={i.id}>
                  {i.label}
                </option>
              ))}
            </select>
          </div>

          {index === 'top_100_crypto' ? (
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={includePegged}
                onChange={(e) => setIncludePegged(e.target.checked)}
              />
              Include stablecoins and tokenized assets (left out by default: they are
              not crypto risk, and a low-volatility screen would rank every dollar first)
            </label>
          ) : null}

          {index ? (
            <p className="text-xs text-muted-foreground">
              Members are taken as they stood on {startDate}, not today, so names that
              have left since are included. The index screen loads every member and
              takes about half a minute.
            </p>
          ) : (
            <div className="space-y-1">
              <Label htmlFor="screen-universe">Symbols</Label>
              <Input
                id="screen-universe"
                value={universe}
                onChange={(e) => setUniverse(e.target.value)}
              />
              <div className="flex flex-wrap gap-2 pt-1">
                {watchlists?.map((w) => (
                  <Button
                    key={w.name}
                    size="sm"
                    variant="outline"
                    onClick={() => setUniverse(w.symbols.join(', '))}
                  >
                    ★ {w.name} ({w.symbols.length})
                  </Button>
                ))}
              </div>
            </div>
          )}

          <div className="space-y-2">
            <Label>Add a filter</Label>
            <div className="flex flex-wrap gap-2">
              {(catalogue?.screeners ?? []).map((s) => (
                <Button
                  key={s.id}
                  size="sm"
                  variant="outline"
                  onClick={() => addStep(s.id)}
                  title={s.description}
                >
                  + {s.display_name}
                </Button>
              ))}
            </div>
          </div>

          {steps.length > 0 ? (
            <ol className="space-y-3">
              {steps.map((step, index) => {
                const spec = catalogue?.screeners.find(
                  (s) => s.id === step.screener_id,
                )
                return (
                  <li key={index} className="rounded-lg border p-3">
                    <div className="mb-2 flex items-center justify-between">
                      <span className="text-sm font-medium">
                        {index + 1}. {spec?.display_name ?? step.screener_id}
                      </span>
                      <Button
                        variant="ghost"
                        size="icon"
                        aria-label={`Remove step ${index + 1}`}
                        onClick={() =>
                          setSteps((c) => c.filter((_, i) => i !== index))
                        }
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                    <div className="grid gap-3 sm:grid-cols-3">
                      {(spec?.params ?? []).map((p) => (
                        <ParamField
                          key={p.name}
                          step={step}
                          spec={p}
                          onChange={(name, value) => setParam(index, name, value)}
                        />
                      ))}
                    </div>
                  </li>
                )
              })}
            </ol>
          ) : (
            <p className="text-sm text-muted-foreground">
              No filters yet — a screen with no steps returns the universe unchanged.
            </p>
          )}

          <Button
            onClick={() =>
              run.mutate(
                index
                  ? {
                      index,
                      include_pegged: includePegged,
                      start: startDate,
                      end: endDate,
                      screeners: steps,
                    }
                  : { symbols, start: startDate, end: endDate, screeners: steps },
              )
            }
            disabled={(!index && symbols.length === 0) || run.isPending}
          >
            {run.isPending
              ? 'Screening…'
              : index
                ? 'Screen the index'
                : `Screen ${symbols.length} symbols`}
          </Button>
        </CardContent>
      </Card>

      {error ? (
        <Alert variant="destructive" className="mb-6">
          <AlertDescription>{error.detail}</AlertDescription>
        </Alert>
      ) : null}

      {result ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Results</CardTitle>
            <CardDescription>
              {result.requested} requested · {result.with_data} had bars ·{' '}
              {result.passed.length} passed
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {result.membership_source ? (
              <Alert>
                <AlertDescription>
                  <span className="font-medium">
                    {result.membership_source === 'reconstructed' ? 'Reconstructed' : 'Observed'}{' '}
                    membership of {result.membership_date?.slice(0, 10)}
                  </span>
                  : {result.with_data} of {result.requested} members have prices in the
                  window.{' '}
                  {result.membership_note}
                  {Object.keys(result.renamed ?? {}).length > 0
                    ? ` ${Object.keys(result.renamed ?? {}).length} listed tickers were mapped to today's.`
                    : ''}
                </AlertDescription>
              </Alert>
            ) : null}

            {(result.excluded_pegged ?? []).length > 0 ? (
              <p className="text-sm text-muted-foreground">
                Left out as stablecoins or tokenized assets:{' '}
                {(result.excluded_pegged ?? []).join(', ')}
              </p>
            ) : null}

            {(result.without_data ?? []).length > 0 ? (
              <details className="text-sm">
                <summary className="cursor-pointer text-muted-foreground">
                  {(result.without_data ?? []).length} without prices in the window (not
                  screened)
                </summary>
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {(result.without_data ?? []).map((symbol) => (
                    <Badge key={symbol} variant="outline">
                      {symbol}
                    </Badge>
                  ))}
                </div>
              </details>
            ) : null}

            {/* Per-step counts, so a screen that ends empty shows WHERE it
                emptied rather than just returning nothing. */}
            <ol className="space-y-1 text-sm">
              <li className="flex items-center justify-between rounded border px-3 py-1.5">
                <span className="text-muted-foreground">Universe with data</span>
                <Badge variant="secondary">{result.with_data}</Badge>
              </li>
              {result.steps.map((step, index) => (
                <li
                  key={index}
                  className="flex items-center justify-between rounded border px-3 py-1.5"
                >
                  <span>
                    {index + 1}. {step.display_name}{' '}
                    <span className="text-xs text-muted-foreground">
                      {Object.entries(step.params)
                        .map(([k, v]) => `${k}=${v}`)
                        .join(', ')}
                    </span>
                  </span>
                  <Badge variant={step.passed === 0 ? 'destructive' : 'secondary'}>
                    {step.passed}
                  </Badge>
                </li>
              ))}
            </ol>

            <div>
              <h4 className="mb-2 text-sm font-medium">
                Survivors ({result.passed.length})
              </h4>
              {result.passed.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  Nothing passed. The step counts above show where the universe
                  emptied.
                </p>
              ) : (
                <div className="flex flex-wrap gap-2">
                  {result.passed.map((symbol) => (
                    <Badge key={symbol} variant="outline">
                      {symbol}
                    </Badge>
                  ))}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}
