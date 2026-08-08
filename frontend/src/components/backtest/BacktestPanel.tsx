/**
 * components/backtest/BacktestPanel.tsx
 * Strategy selection -> run -> results.
 *
 * Phase 4 — React frontend
 */

import { ApiError } from '@/api/client'
import { useRunBacktest } from '@/api/queries'
import { BacktestResults } from '@/components/backtest/BacktestResults'
import { StrategySelector } from '@/components/backtest/StrategySelector'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAppStore } from '@/store/useAppStore'

export function BacktestPanel() {
  const { selectedSymbol, startDate, endDate, strategyId, strategyParams } =
    useAppStore()
  const backtest = useRunBacktest()

  const canRun = Boolean(selectedSymbol && strategyId) && !backtest.isPending

  function handleRun() {
    if (!selectedSymbol || !strategyId) return
    backtest.mutate({
      symbol: selectedSymbol,
      strategy_id: strategyId,
      start: startDate,
      end: endDate,
      // Empty strings come from a parameter field the user cleared mid-edit.
      // Dropping them lets the server fall back to the registry default rather
      // than rejecting '' as the wrong type.
      params: Object.fromEntries(
        Object.entries(strategyParams).filter(([, value]) => value !== ''),
      ),
    })
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Backtest</CardTitle>
          <CardDescription>
            Strategies and their parameters come from the registry — nothing is
            hardcoded here.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <StrategySelector />
          <Button
            onClick={handleRun}
            disabled={!canRun}
            className="w-full"
            aria-busy={backtest.isPending}
          >
            {backtest.isPending
              ? 'Running…'
              : `Run backtest${selectedSymbol ? ` on ${selectedSymbol}` : ''}`}
          </Button>
        </CardContent>
      </Card>

      {backtest.isPending ? (
        <Card>
          <CardContent className="space-y-4 pt-6">
            <Skeleton className="h-72 w-full" />
            <Skeleton className="h-24 w-full" />
          </CardContent>
        </Card>
      ) : null}

      {backtest.isError ? (
        <Alert variant="destructive">
          <AlertTitle>
            {backtest.error instanceof ApiError && backtest.error.status === 422
              ? 'Invalid backtest request'
              : 'Backtest failed'}
          </AlertTitle>
          <AlertDescription>
            {backtest.error instanceof ApiError
              ? backtest.error.detail
              : String(backtest.error)}
          </AlertDescription>
        </Alert>
      ) : null}

      {backtest.data && !backtest.isPending ? (
        <Card>
          <CardHeader>
            <CardTitle>Results</CardTitle>
            <CardDescription>
              {backtest.data.strategy_name} on {backtest.data.symbol}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {backtest.data.caveat ? (
              <Alert variant="destructive" className="mb-4">
                <AlertTitle>Results are not trustworthy</AlertTitle>
                <AlertDescription>{backtest.data.caveat}</AlertDescription>
              </Alert>
            ) : null}
            <BacktestResults result={backtest.data} />
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}
