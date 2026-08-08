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

import { useAssets, useOhlcv, useSignals, useStrategies } from '@/api/queries'
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
import { useDebounced } from '@/lib/useDebounced'
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
  const {
    selectedSymbol,
    startDate,
    endDate,
    strategyId,
    strategyParams,
    showSignals,
    toggleSignals,
  } = useAppStore()

  const { data, isLoading, isError, error } = useOhlcv(
    selectedSymbol,
    startDate,
    endDate,
  )

  const { data: strategyCatalogue } = useStrategies('single')
  const activeStrategy = strategyCatalogue?.strategies.find(
    (s) => s.id === strategyId,
  )

  // Same strategy AND same parameters the backtest uses, so the markers can
  // never disagree with the results below. Empty values (a field cleared
  // mid-edit) are dropped so the server falls back to registry defaults.
  const overlayParams = Object.fromEntries(
    Object.entries(strategyParams).filter(([, value]) => value !== ''),
  )
  // Debounced: typing "0.5" into a number field passes through 0, which several
  // strategies reject — an undebounced query fires per keystroke and flashes a
  // 422 the user never asked for.
  const debouncedParams = useDebounced(overlayParams)
  const signalsQuery = useSignals(
    selectedSymbol,
    strategyId,
    startDate,
    endDate,
    debouncedParams,
    showSignals,
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
        <CardDescription className="flex items-center justify-between gap-4">
          <span>Daily close price</span>
          <label className="flex cursor-pointer items-center gap-2 text-xs">
            <input
              type="checkbox"
              checked={showSignals}
              onChange={toggleSignals}
              disabled={!strategyId}
              className="size-3.5 accent-current"
            />
            {activeStrategy
              ? `Overlay ${activeStrategy.display_name} signals`
              : 'Select a strategy to overlay signals'}
          </label>
        </CardDescription>
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

        {/* The overlay is a separate request, so a signal failure must not
            blank the price chart — surface it and keep drawing prices. */}
        {showSignals && signalsQuery.isError ? (
          <Alert variant="destructive" className="mb-4">
            <AlertTitle>Could not load signals</AlertTitle>
            <AlertDescription>
              {signalsQuery.error instanceof ApiError
                ? signalsQuery.error.detail
                : String(signalsQuery.error)}
            </AlertDescription>
          </Alert>
        ) : null}

        {data && !isLoading ? (
          <PriceChart
            bars={data.bars}
            symbol={data.symbol}
            signals={
              showSignals && signalsQuery.data
                ? signalsQuery.data.signals
                : undefined
            }
          />
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
