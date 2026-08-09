/**
 * pages/StatisticsPage.tsx
 * Stationarity, cointegration, alpha/beta and PCA.
 *
 * The catalogue DESCRIBES each test rather than the UI hardcoding it: arity
 * says how many symbols to ask for, input_kind says whether the test runs on
 * prices or returns. Those differ per test, which is why the registry
 * describes rather than constructs.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useEffect, useState } from 'react'

import type { ApiError, TestSchema } from '@/api/client'
import { useRunStatistic, useStatisticsCatalogue } from '@/api/queries'
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

function arityHint(test: TestSchema): string {
  if (test.arity === 'single') return 'one symbol'
  if (test.arity === 'pair') return 'exactly two symbols, in order'
  return `two or more symbols (min ${test.min_symbols})`
}

/**
 * Renders whatever the test returned.
 *
 * Deliberately generic: the six tests return quite different shapes, and
 * hardcoding each would be a seventh place that has to change when a test
 * does. Scalars are shown as a table, nested objects as JSON.
 */
function ResultBody({ result }: { result: Record<string, unknown> }) {
  const entries = Object.entries(result)
  const scalars = entries.filter(
    ([, v]) => v === null || ['string', 'number', 'boolean'].includes(typeof v),
  )
  return (
    <div className="space-y-4">
      <table className="w-full text-sm">
        <tbody>
          {scalars.map(([key, value]) => (
            <tr key={key} className="border-b last:border-0">
              <td className="py-1.5 pr-4 text-muted-foreground">{key}</td>
              <td className="py-1.5 text-right tabular-nums">
                {typeof value === 'number'
                  ? value.toFixed(Math.abs(value) < 1 ? 4 : 2)
                  : String(value)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {entries
        .filter(([, v]) => v !== null && typeof v === 'object')
        .map(([key, value]) => (
          <details key={key} className="rounded border p-2">
            <summary className="cursor-pointer text-sm font-medium">{key}</summary>
            <pre className="mt-2 overflow-x-auto text-xs">
              {JSON.stringify(value, null, 2)}
            </pre>
          </details>
        ))}
    </div>
  )
}

export function StatisticsPage() {
  const { data: catalogue } = useStatisticsCatalogue()
  const run = useRunStatistic()

  const [testId, setTestId] = useState<string>('adf')
  const [symbols, setSymbols] = useState('AAPL')
  const [start, setStart] = useState('2024-01-01')
  const [end, setEnd] = useState('2026-08-01')

  const test = catalogue?.tests.find((t) => t.id === testId)

  // Prefill a symbol count that suits the chosen test's arity, so switching to
  // a pair test does not immediately 422 on one symbol.
  useEffect(() => {
    if (!test) return
    setSymbols((current) => {
      const count = current.split(',').filter((s) => s.trim()).length
      if (test.arity === 'single' && count !== 1) return 'AAPL'
      if (test.arity === 'pair' && count !== 2) return 'AAPL, MSFT'
      if (test.arity === 'multi' && count < (test.min_symbols ?? 2))
        return 'AAPL, MSFT, JPM, XOM'
      return current
    })
  }, [test])

  const list = symbols
    .split(',')
    .map((s) => s.toUpperCase().trim())
    .filter(Boolean)

  const error = run.error as ApiError | null
  const result = run.data

  return (
    <div>
      <PageHeader
        title="Statistics"
        blurb="Stationarity, cointegration, alpha/beta and PCA."
      />

      <div className="grid gap-6 lg:grid-cols-[320px_1fr]">
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-base">Test</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {(catalogue?.tests ?? []).map((t) => (
              <button
                key={t.id}
                type="button"
                onClick={() => setTestId(t.id)}
                className={`w-full rounded-lg border p-3 text-left transition-colors ${
                  testId === t.id ? 'border-primary bg-muted/50' : 'hover:bg-muted/30'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">{t.display_name}</span>
                  {/* input_kind is surfaced because it changes what the test
                      MEANS: PCA on price levels measures shared trend, not
                      co-movement. */}
                  <Badge variant="outline">{t.input_kind}</Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{t.description}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  Takes {arityHint(t)}
                </p>
              </button>
            ))}
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">
                {test?.display_name ?? testId}
              </CardTitle>
              <CardDescription>{test ? arityHint(test) : null}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="space-y-1 sm:col-span-3">
                  <Label htmlFor="stat-symbols">Symbols</Label>
                  <Input
                    id="stat-symbols"
                    value={symbols}
                    onChange={(e) => setSymbols(e.target.value)}
                  />
                  {/* Order is preserved and matters: OLS treats the first
                      symbol as the asset and the second as the benchmark, so
                      swapping them inverts alpha and beta. */}
                  {test?.arity === 'pair' ? (
                    <p className="text-xs text-muted-foreground">
                      Order matters — the first is the asset, the second the
                      benchmark.
                    </p>
                  ) : null}
                </div>
                <div className="space-y-1">
                  <Label htmlFor="stat-start">Start</Label>
                  <Input
                    id="stat-start"
                    type="date"
                    value={start}
                    onChange={(e) => setStart(e.target.value)}
                  />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="stat-end">End</Label>
                  <Input
                    id="stat-end"
                    type="date"
                    value={end}
                    onChange={(e) => setEnd(e.target.value)}
                  />
                </div>
              </div>

              {test?.caveat ? (
                <Alert>
                  <AlertDescription>{test.caveat}</AlertDescription>
                </Alert>
              ) : null}

              <Button
                onClick={() =>
                  run.mutate({ testId, body: { symbols: list, start, end } })
                }
                disabled={list.length === 0 || run.isPending}
              >
                {run.isPending ? 'Running…' : 'Run test'}
              </Button>

              {error ? (
                <Alert variant="destructive">
                  <AlertDescription>{error.detail}</AlertDescription>
                </Alert>
              ) : null}
            </CardContent>
          </Card>

          {result ? (
            <Card>
              <CardHeader>
                <CardTitle className="text-base">{result.display_name}</CardTitle>
                <CardDescription>
                  {result.symbols.join(', ')} · {result.observations} observations
                  · computed on {result.input_kind}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <ResultBody result={result.result as Record<string, unknown>} />
              </CardContent>
            </Card>
          ) : null}
        </div>
      </div>
    </div>
  )
}
