/**
 * pages/ResearchPage.tsx
 * Company profile, financial statements and news.
 *
 * Replaces the Streamlit "Asset Deep Dive" and "Market Intelligence" widgets.
 *
 * These are the only endpoints that reach the network. Note what is NOT here:
 * price history. Streamlit's deep dive pulled 5 years from yfinance, so it
 * could disagree with every other view in the app about what a price was; the
 * chart lives on the Chart & Backtest page and reads TimescaleDB.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'
import { ExternalLink } from 'lucide-react'

import type { ApiError, NewsItem, StatementLine } from '@/api/client'
import {
  useFinancials,
  useNews,
  usePortfolios,
  useProfile,
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
import { Skeleton } from '@/components/ui/skeleton'

/**
 * Missing values render as an em dash, never as 0.
 *
 * Streamlit defaulted these (`info.get('trailingPE', 0)`), so a company with
 * no P/E displayed "0.00" — indistinguishable from a real zero. The API now
 * returns null; the UI has to keep that distinction visible.
 */
function num(value: number | null | undefined, digits = 2): string {
  return value == null ? '—' : value.toFixed(digits)
}

function big(value: number | null | undefined): string {
  if (value == null) return '—'
  const units: [number, string][] = [
    [1e12, 'T'],
    [1e9, 'B'],
    [1e6, 'M'],
  ]
  for (const [scale, suffix] of units) {
    if (Math.abs(value) >= scale) return `$${(value / scale).toFixed(2)}${suffix}`
  }
  return `$${value.toFixed(0)}`
}

function Statement({ title, lines }: { title: string; lines: StatementLine[] }) {
  if (!lines.length) {
    return (
      <div>
        <h4 className="mb-2 text-sm font-medium">{title}</h4>
        <p className="text-sm text-muted-foreground">Not reported.</p>
      </div>
    )
  }
  const periods = Object.keys(lines[0].values)
  return (
    <div>
      <h4 className="mb-2 text-sm font-medium">{title}</h4>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b text-left text-xs uppercase text-muted-foreground">
            <tr>
              <th className="py-2 pr-4">Line item</th>
              {periods.map((period) => (
                <th key={period} className="py-2 pr-4 text-right">
                  {period}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {lines.map((line) => (
              <tr key={line.line_item} className="border-b last:border-0">
                <td className="py-1.5 pr-4">{line.line_item}</td>
                {periods.map((period) => (
                  <td
                    key={period}
                    className="py-1.5 pr-4 text-right tabular-nums"
                  >
                    {big(line.values[period])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function NewsList({ items }: { items: NewsItem[] }) {
  if (!items.length) {
    return <p className="text-sm text-muted-foreground">No stories found.</p>
  }
  return (
    <ul className="space-y-4">
      {items.map((item) => (
        <li key={item.id} className="border-b pb-3 last:border-0">
          <a
            href={item.url ?? undefined}
            target="_blank"
            rel="noreferrer noopener"
            className="font-medium hover:underline"
          >
            {item.title}
            <ExternalLink className="ml-1 inline h-3 w-3" />
          </a>
          <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
            <Badge variant="outline">{item.symbol}</Badge>
            <span>{item.publisher ?? 'Unknown source'}</span>
            {item.published_at ? (
              <span>· {new Date(item.published_at).toLocaleString()}</span>
            ) : null}
          </div>
          {item.summary ? (
            <p className="mt-1 line-clamp-2 text-sm text-muted-foreground">
              {item.summary}
            </p>
          ) : null}
        </li>
      ))}
    </ul>
  )
}

type NewsSource =
  | { kind: 'market' }
  | { kind: 'portfolio'; name: string }
  | { kind: 'watchlist'; name: string }
  | { kind: 'symbol'; name: string }

export function ResearchPage() {
  const [entry, setEntry] = useState('AAPL')
  const [symbol, setSymbol] = useState('AAPL')
  const [quarterly, setQuarterly] = useState(false)
  const [source, setSource] = useState<NewsSource>({ kind: 'market' })

  const profile = useProfile(symbol)
  const financials = useFinancials(symbol, quarterly)
  const { data: portfolios } = usePortfolios()
  const { data: watchlists } = useWatchlists()

  const news = useNews(
    source.kind === 'market'
      ? {}
      : source.kind === 'portfolio'
        ? { portfolio: source.name }
        : source.kind === 'watchlist'
          ? { watchlist: source.name }
          : { symbols: [source.name] },
  )

  const profileError = profile.error as ApiError | null
  const newsError = news.error as ApiError | null

  return (
    <div>
      <PageHeader
        title="Research"
        blurb="Company profile, financial statements and news. The only data here that comes from outside the database."
      />

      <div className="grid gap-6 lg:grid-cols-2">
        {/* -- deep dive ------------------------------------------------ */}
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Asset deep dive</CardTitle>
              <CardDescription>
                Price history is on the Chart page — it comes from the database,
                not from here.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <form
                className="flex gap-2"
                onSubmit={(e) => {
                  e.preventDefault()
                  setSymbol(entry.toUpperCase().trim())
                }}
              >
                <div className="flex-1 space-y-1">
                  <Label htmlFor="research-symbol">Ticker</Label>
                  <Input
                    id="research-symbol"
                    value={entry}
                    onChange={(e) => setEntry(e.target.value)}
                  />
                </div>
                <Button type="submit" className="self-end">
                  Look up
                </Button>
              </form>

              {profile.isLoading ? <Skeleton className="h-24 w-full" /> : null}

              {profileError ? (
                <Alert variant="destructive">
                  <AlertDescription>
                    {profileError.detail}
                    {/* 503 is the upstream provider, not a bad request — worth
                        distinguishing so a Yahoo outage is not read as a typo. */}
                    {profileError.status === 503
                      ? ' (This comes from the upstream data provider.)'
                      : null}
                  </AlertDescription>
                </Alert>
              ) : null}

              {profile.data ? (
                <div className="space-y-3">
                  <div>
                    <h3 className="text-lg font-semibold">
                      {profile.data.long_name ?? profile.data.symbol}
                    </h3>
                    <p className="text-sm text-muted-foreground">
                      {[profile.data.sector, profile.data.industry, profile.data.country]
                        .filter(Boolean)
                        .join(' · ') || 'No classification'}
                    </p>
                  </div>

                  <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                    <div className="rounded-lg border p-3">
                      <div className="text-xs text-muted-foreground">Market cap</div>
                      <div className="mt-1 font-semibold tabular-nums">
                        {big(profile.data.market_cap)}
                      </div>
                    </div>
                    <div className="rounded-lg border p-3">
                      <div className="text-xs text-muted-foreground">P/E (TTM)</div>
                      <div className="mt-1 font-semibold tabular-nums">
                        {num(profile.data.trailing_pe)}
                      </div>
                    </div>
                    <div className="rounded-lg border p-3">
                      <div className="text-xs text-muted-foreground">Forward P/E</div>
                      <div className="mt-1 font-semibold tabular-nums">
                        {num(profile.data.forward_pe)}
                      </div>
                    </div>
                    <div className="rounded-lg border p-3">
                      <div className="text-xs text-muted-foreground">Div. yield</div>
                      <div className="mt-1 font-semibold tabular-nums">
                        {num(profile.data.dividend_yield)}
                      </div>
                    </div>
                  </div>

                  {profile.data.business_summary ? (
                    <details className="rounded-lg border p-3">
                      <summary className="cursor-pointer text-sm font-medium">
                        Business summary
                      </summary>
                      <p className="mt-2 text-sm text-muted-foreground">
                        {profile.data.business_summary}
                      </p>
                    </details>
                  ) : null}
                </div>
              ) : null}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Financial statements</CardTitle>
              <CardDescription>
                <button
                  type="button"
                  className="underline"
                  onClick={() => setQuarterly((q) => !q)}
                >
                  Showing {quarterly ? 'quarterly' : 'annual'} — switch
                </button>
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {financials.isLoading ? <Skeleton className="h-40 w-full" /> : null}
              {financials.error ? (
                <Alert variant="destructive">
                  <AlertDescription>
                    {(financials.error as ApiError).detail}
                  </AlertDescription>
                </Alert>
              ) : null}
              {financials.data ? (
                <>
                  <Statement
                    title="Income statement"
                    lines={financials.data.income_statement}
                  />
                  <Statement
                    title="Balance sheet"
                    lines={financials.data.balance_sheet}
                  />
                  <Statement title="Cash flow" lines={financials.data.cash_flow} />
                </>
              ) : null}
            </CardContent>
          </Card>
        </div>

        {/* -- news ------------------------------------------------------ */}
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-base">News</CardTitle>
            <CardDescription>
              Portfolios and watchlists come from the database, so this tracks
              the same state as the rest of the app.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant={source.kind === 'market' ? 'secondary' : 'outline'}
                onClick={() => setSource({ kind: 'market' })}
              >
                Market
              </Button>
              <Button
                size="sm"
                variant={source.kind === 'symbol' ? 'secondary' : 'outline'}
                onClick={() => setSource({ kind: 'symbol', name: symbol })}
              >
                {symbol}
              </Button>
              {portfolios?.map((p) => (
                <Button
                  key={`p-${p.name}`}
                  size="sm"
                  variant={
                    source.kind === 'portfolio' && source.name === p.name
                      ? 'secondary'
                      : 'outline'
                  }
                  onClick={() => setSource({ kind: 'portfolio', name: p.name })}
                >
                  {p.name}
                </Button>
              ))}
              {watchlists?.map((w) => (
                <Button
                  key={`w-${w.name}`}
                  size="sm"
                  variant={
                    source.kind === 'watchlist' && source.name === w.name
                      ? 'secondary'
                      : 'outline'
                  }
                  onClick={() => setSource({ kind: 'watchlist', name: w.name })}
                >
                  ★ {w.name}
                </Button>
              ))}
            </div>

            {news.isLoading ? <Skeleton className="h-40 w-full" /> : null}

            {newsError ? (
              <Alert variant="destructive">
                <AlertDescription>
                  {newsError.detail}
                  {newsError.status === 503
                    ? ' (Upstream provider — not a problem with this request.)'
                    : null}
                </AlertDescription>
              </Alert>
            ) : null}

            {news.data ? (
              <>
                <p className="text-xs text-muted-foreground">
                  {news.data.source} · {news.data.symbols.join(', ') || 'no symbols'}
                </p>
                {/* Reported rather than silently trimmed, so a long watchlist
                    does not look fully covered when only ten were fetched. */}
                {(news.data.truncated_symbols ?? []).length > 0 ? (
                  <Alert>
                    <AlertDescription>
                      Not covered (10-ticker limit):{' '}
                      {(news.data.truncated_symbols ?? []).join(', ')}
                    </AlertDescription>
                  </Alert>
                ) : null}
                <NewsList items={news.data.items} />
              </>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
