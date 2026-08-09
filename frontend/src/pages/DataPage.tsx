/**
 * pages/DataPage.tsx
 * Ingest price bars, register tickers, browse index constituents.
 *
 * Replaces the Streamlit Config tab's "Run Data Ingestion Pipeline" and
 * "Manually Add Ticker" controls. Those shelled out to cli/run_pipeline.py,
 * which writes SQLite — a database the API does not read. This page drives
 * /api/v1/ingest, which writes TimescaleDB.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'

import type { ApiError, SymbolResult } from '@/api/client'
import {
  useAddAsset,
  useIngestStatus,
  useRunIngest,
  useUniverse,
  useWatchlists,
} from '@/api/queries'
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

const SOURCES = ['sp500', 'dow_jones', 'nasdaq100', 'top_100_crypto'] as const

function Outcome({ result }: { result: SymbolResult }) {
  return (
    <tr className="border-b last:border-0">
      <td className="py-1.5 pr-4 font-medium">{result.symbol}</td>
      <td className="py-1.5 pr-4 text-right tabular-nums">{result.fetched}</td>
      <td className="py-1.5 pr-4 text-right tabular-nums">{result.written}</td>
      <td className="py-1.5 pr-4 text-right tabular-nums">
        {result.skipped_empty || ''}
      </td>
      <td className="py-1.5">
        {result.error ? (
          <span className="text-xs text-red-600">{result.error}</span>
        ) : result.written === 0 ? (
          // Zero written and no error is ambiguous today: already current, or
          // delisted. See the corporate-actions task.
          <span className="text-xs text-muted-foreground">
            up to date (or no longer trading)
          </span>
        ) : (
          <span className="text-xs text-muted-foreground">
            {result.first_bar?.slice(0, 10)} → {result.last_bar?.slice(0, 10)}
          </span>
        )}
      </td>
    </tr>
  )
}

export function DataPage() {
  const ingest = useRunIngest()
  const addAsset = useAddAsset()
  const { data: watchlists } = useWatchlists()

  const [symbols, setSymbols] = useState('')
  const [fullBackfill, setFullBackfill] = useState(false)
  const [newTicker, setNewTicker] = useState('')
  const [assetClass, setAssetClass] = useState('equity')
  const [source, setSource] = useState<string | null>(null)

  const status = useIngestStatus(ingest.isPending)
  const universe = useUniverse(source)

  const list = symbols
    .split(',')
    .map((s) => s.toUpperCase().trim())
    .filter(Boolean)

  const ingestError = ingest.error as ApiError | null
  const report = ingest.data

  return (
    <div>
      <PageHeader
        title="Data"
        blurb="Fetch new bars into TimescaleDB, register tickers, browse index constituents."
      />

      <div className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Ingest price bars</CardTitle>
            <CardDescription>
              Each symbol resumes from the day after its newest stored bar.
              Leave the list empty to refresh everything registered.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-1">
              <Label htmlFor="ingest-symbols">Symbols (optional)</Label>
              <Input
                id="ingest-symbols"
                value={symbols}
                placeholder="Leave blank for the whole registry"
                onChange={(e) => setSymbols(e.target.value)}
              />
              <div className="flex flex-wrap gap-2 pt-1">
                {watchlists?.map((w) => (
                  <Button
                    key={w.name}
                    size="sm"
                    variant="outline"
                    onClick={() => setSymbols(w.symbols.join(', '))}
                  >
                    ★ {w.name}
                  </Button>
                ))}
              </div>
            </div>

            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={fullBackfill}
                onChange={(e) => setFullBackfill(e.target.checked)}
              />
              Full backfill — refetch from 2015 instead of resuming
            </label>

            <Button
              onClick={() =>
                ingest.mutate({
                  symbols: list.length ? list : null,
                  full_backfill: fullBackfill,
                })
              }
              disabled={ingest.isPending}
            >
              {ingest.isPending
                ? 'Ingesting…'
                : list.length
                  ? `Ingest ${list.length} symbols`
                  : 'Ingest everything'}
            </Button>

            {/* Progress is polled only while a run is in flight; the endpoint
                runs to completion before responding, and a full universe takes
                minutes. */}
            {ingest.isPending && status.data?.total ? (
              <div className="space-y-1">
                <div className="h-2 w-full overflow-hidden rounded bg-muted">
                  <div
                    className="h-full bg-primary transition-all"
                    style={{
                      width: `${(status.data.completed / status.data.total) * 100}%`,
                    }}
                  />
                </div>
                <p className="text-xs text-muted-foreground">
                  {status.data.completed} of {status.data.total}
                  {status.data.current_symbol
                    ? ` · ${status.data.current_symbol}`
                    : null}
                </p>
              </div>
            ) : null}

            {ingestError ? (
              <Alert variant="destructive">
                <AlertDescription>
                  {ingestError.detail}
                  {ingestError.status === 409
                    ? ' Two runs would fetch the same window twice for no benefit.'
                    : null}
                </AlertDescription>
              </Alert>
            ) : null}

            {report ? (
              <div className="space-y-3">
                <div className="flex flex-wrap gap-2">
                  <Badge variant="secondary">
                    {report.written.toLocaleString()} bars written
                  </Badge>
                  <Badge variant="outline">{report.symbols.length} symbols</Badge>
                  {report.failed.length > 0 ? (
                    <Badge variant="destructive">
                      {report.failed.length} failed
                    </Badge>
                  ) : null}
                </div>

                <details className="rounded border p-2">
                  <summary className="cursor-pointer text-sm font-medium">
                    Per-symbol detail
                  </summary>
                  <div className="mt-2 max-h-96 overflow-auto">
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 border-b bg-background text-left text-xs uppercase text-muted-foreground">
                        <tr>
                          <th className="py-2 pr-4">Symbol</th>
                          <th className="py-2 pr-4 text-right">Fetched</th>
                          <th className="py-2 pr-4 text-right">Written</th>
                          <th className="py-2 pr-4 text-right">Empty</th>
                          <th className="py-2">Range</th>
                        </tr>
                      </thead>
                      <tbody>
                        {report.results.map((r) => (
                          <Outcome key={r.symbol} result={r} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              </div>
            ) : null}
          </CardContent>
        </Card>

        <div className="grid gap-6 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Register a ticker</CardTitle>
              <CardDescription>
                Adds it to the universe so ingestion will fetch it. Adding one
                that already exists is not an error.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="space-y-1">
                  <Label htmlFor="new-ticker">Symbol</Label>
                  <Input
                    id="new-ticker"
                    value={newTicker}
                    onChange={(e) => setNewTicker(e.target.value)}
                  />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="asset-class">Asset class</Label>
                  <select
                    id="asset-class"
                    className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
                    value={assetClass}
                    onChange={(e) => setAssetClass(e.target.value)}
                  >
                    <option value="equity">equity</option>
                    <option value="crypto">crypto</option>
                  </select>
                </div>
              </div>
              <Button
                disabled={!newTicker.trim() || addAsset.isPending}
                onClick={() =>
                  addAsset.mutate(
                    { symbol: newTicker.trim(), asset_class: assetClass },
                    { onSuccess: () => setNewTicker('') },
                  )
                }
              >
                Register
              </Button>
              {addAsset.data ? (
                <p className="text-sm text-muted-foreground">
                  {addAsset.data.symbol}{' '}
                  {addAsset.data.created ? 'registered' : 'was already registered'}.
                </p>
              ) : null}
              {addAsset.error ? (
                <Alert variant="destructive">
                  <AlertDescription>
                    {(addAsset.error as ApiError).detail}
                  </AlertDescription>
                </Alert>
              ) : null}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Index constituents</CardTitle>
              <CardDescription>
                Lists members without registering them — you decide what to add.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex flex-wrap gap-2">
                {SOURCES.map((s) => (
                  <Button
                    key={s}
                    size="sm"
                    variant={source === s ? 'secondary' : 'outline'}
                    onClick={() => setSource(s)}
                  >
                    {s}
                  </Button>
                ))}
              </div>

              {universe.isLoading ? (
                <p className="text-sm text-muted-foreground">Fetching…</p>
              ) : null}
              {universe.error ? (
                <Alert variant="destructive">
                  <AlertDescription>
                    {(universe.error as ApiError).detail}
                  </AlertDescription>
                </Alert>
              ) : null}
              {universe.data ? (
                <>
                  <p className="text-sm text-muted-foreground">
                    {universe.data.count} members
                  </p>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setSymbols(universe.data.symbols.join(', '))}
                  >
                    Use as the ingest list
                  </Button>
                  <div className="max-h-48 overflow-auto rounded border p-2 text-xs">
                    {universe.data.symbols.join(', ')}
                  </div>
                </>
              ) : null}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}
