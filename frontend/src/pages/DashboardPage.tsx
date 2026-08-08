/**
 * pages/DashboardPage.tsx
 *
 * Symbol picker -> price chart -> backtest.
 *
 * Zustand holds the selection, TanStack Query fetches server state, chart
 * components stay presentational, and shadcn supplies the surface. The signal
 * overlay and candlestick view build on exactly this shape.
 *
 * Phase 4 — React frontend
 */

import { useAssets, useOhlcv } from '@/api/queries'
import { ApiError } from '@/api/client'
import { BacktestPanel } from '@/components/backtest/BacktestPanel'
import { PriceChart } from '@/components/charts/PriceChart'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
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
        </CardContent>
      </Card>

      <PriceCard />

      {/* Backtest spans the full width beneath the chart — the equity curve and
          KPI grid need the room. */}
      <div className="lg:col-span-3">
        <BacktestPanel />
      </div>
    </div>
  )
}
