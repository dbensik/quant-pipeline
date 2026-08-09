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
              run.mutate({
                symbols,
                start: startDate,
                end: endDate,
                screeners: steps,
              })
            }
            disabled={symbols.length === 0 || run.isPending}
          >
            {run.isPending ? 'Screening…' : `Screen ${symbols.length} symbols`}
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
