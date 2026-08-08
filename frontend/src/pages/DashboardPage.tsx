/**
 * pages/DashboardPage.tsx
 *
 * The Phase 4 vertical slice: symbol picker -> OHLCV chart.
 *
 * Demonstrates the whole structure end to end — Zustand holds the selection,
 * TanStack Query fetches the bars, a presentational chart renders them, and
 * shadcn supplies the surface. The strategy selector, backtest results and
 * signal overlay build on exactly this shape.
 *
 * Phase 4 — React frontend
 */

import { useAssets, useOhlcv, useStrategies } from '@/api/queries'
import { ApiError } from '@/api/client'
import { PriceChart } from '@/components/charts/PriceChart'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/store/useAppStore'

function SymbolPicker() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol)
  const setSymbol = useAppStore((s) => s.setSymbol)
  const { data, isLoading } = useAssets()

  if (isLoading) return <Skeleton className="h-9 w-full" />

  const assets = data?.assets ?? []

  return (
    <Select value={selectedSymbol ?? undefined} onValueChange={setSymbol}>
      <SelectTrigger id="symbol" className="w-full">
        <SelectValue placeholder="Select a symbol" />
      </SelectTrigger>
      <SelectContent className="max-h-72">
        {assets.map((asset) => (
          <SelectItem key={asset.symbol} value={asset.symbol}>
            {asset.symbol}
            <span className="ml-2 text-xs text-muted-foreground">
              {asset.asset_class}
            </span>
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

function DateRangeInputs() {
  const { startDate, endDate, setDateRange } = useAppStore()
  return (
    <div className="grid grid-cols-2 gap-3">
      <div className="space-y-1.5">
        <Label htmlFor="start">Start</Label>
        <Input
          id="start"
          type="date"
          value={startDate}
          onChange={(e) => setDateRange(e.target.value, endDate)}
        />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="end">End</Label>
        <Input
          id="end"
          type="date"
          value={endDate}
          onChange={(e) => setDateRange(startDate, e.target.value)}
        />
      </div>
    </div>
  )
}

/**
 * Present purely to prove the strategy catalogue reaches the frontend with its
 * parameter schemas — running a backtest from here is the next slice.
 */
function StrategyCatalogue() {
  const { data, isLoading, isError } = useStrategies('single')

  if (isLoading) return <Skeleton className="h-24 w-full" />
  if (isError) return null

  return (
    <div className="flex flex-wrap gap-2">
      {(data?.strategies ?? []).map((strategy) => (
        <Badge
          key={strategy.id}
          variant="secondary"
          title={`${strategy.description} (${strategy.params.length} parameters)`}
        >
          {strategy.display_name}
          {strategy.caveat ? ' ⚠️' : ''}
        </Badge>
      ))}
    </div>
  )
}

function PriceCard() {
  const { selectedSymbol, startDate, endDate } = useAppStore()
  const { data, isLoading, isError, error } = useOhlcv(
    selectedSymbol,
    startDate,
    endDate,
  )

  return (
    <Card className="lg:col-span-2">
      <CardHeader>
        <CardTitle>
          {selectedSymbol ?? 'No symbol'}
          {data ? (
            <span className="ml-2 text-sm font-normal text-muted-foreground">
              {data.count} bars · {data.asset_class}
            </span>
          ) : null}
        </CardTitle>
        <CardDescription>Daily close price</CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? <Skeleton className="h-80 w-full" /> : null}

        {isError ? (
          <Alert variant="destructive">
            <AlertTitle>
              {error instanceof ApiError && error.isNotFound
                ? 'Unknown symbol'
                : 'Could not load prices'}
            </AlertTitle>
            <AlertDescription>
              {error instanceof ApiError ? error.detail : String(error)}
            </AlertDescription>
          </Alert>
        ) : null}

        {data && !isLoading ? (
          <PriceChart bars={data.bars} symbol={data.symbol} />
        ) : null}
      </CardContent>
    </Card>
  )
}

export function DashboardPage() {
  return (
    <div className="grid gap-6 lg:grid-cols-3">
      <Card>
        <CardHeader>
          <CardTitle>Selection</CardTitle>
          <CardDescription>
            Held in Zustand; the data itself is TanStack Query's.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="symbol">Symbol</Label>
            <SymbolPicker />
          </div>
          <DateRangeInputs />
          <div className="space-y-1.5 pt-2">
            <Label>Available strategies</Label>
            <StrategyCatalogue />
          </div>
        </CardContent>
      </Card>

      <PriceCard />
    </div>
  )
}
