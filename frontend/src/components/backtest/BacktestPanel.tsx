/**
 * components/backtest/BacktestPanel.tsx
 * Strategy selection -> run -> results, over the websocket or plain REST.
 *
 * The websocket is the default because a multi-year run otherwise leaves the
 * UI blank with no indication of progress or failure — which is exactly why
 * the endpoint exists. REST stays available as a fallback: websockets are the
 * first thing a proxy breaks, and the two paths return identical results (an
 * API test asserts that).
 *
 * Phase 4 — React frontend
 */

import { ApiError } from '@/api/client'
import { fromRest } from '@/api/backtestResult'
import type { BacktestResult } from '@/api/backtestResult'
import { useRunBacktest } from '@/api/queries'
import { useBacktestSocket } from '@/api/useBacktestSocket'
import { BacktestProgress } from '@/components/backtest/BacktestProgress'
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
  const {
    selectedSymbol,
    startDate,
    endDate,
    strategyId,
    strategyParams,
    streamProgress,
    toggleStreamProgress,
  } = useAppStore()

  const socket = useBacktestSocket()
  const rest = useRunBacktest()

  const isRunning = streamProgress ? socket.isRunning : rest.isPending
  const canRun = Boolean(selectedSymbol && strategyId) && !isRunning

  // Whichever transport ran last supplies the result; both normalise to the
  // same view model so nothing downstream branches on transport.
  const result: BacktestResult | null = streamProgress
    ? socket.result
    : rest.data
      ? fromRest(rest.data)
      : null

  const errorMessage = streamProgress
    ? socket.error
    : rest.error
      ? rest.error instanceof ApiError
        ? rest.error.detail
        : String(rest.error)
      : null

  const errorTitle =
    !streamProgress && rest.error instanceof ApiError && rest.error.status === 422
      ? 'Invalid backtest request'
      : 'Backtest failed'

  function handleRun() {
    if (!selectedSymbol || !strategyId) return
    const request = {
      symbol: selectedSymbol,
      strategy_id: strategyId,
      start: startDate,
      end: endDate,
      // Empty strings come from a parameter field cleared mid-edit. Dropping
      // them lets the server fall back to registry defaults rather than
      // rejecting '' as the wrong type.
      params: Object.fromEntries(
        Object.entries(strategyParams).filter(([, value]) => value !== ''),
      ),
    }
    if (streamProgress) {
      rest.reset()
      socket.run(request)
    } else {
      socket.reset()
      rest.mutate(request)
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Backtest</CardTitle>
          <CardDescription className="flex items-center justify-between gap-4">
            <span>
              Strategies and their parameters come from the registry — nothing
              is hardcoded here.
            </span>
            <label className="flex shrink-0 cursor-pointer items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={streamProgress}
                onChange={toggleStreamProgress}
                disabled={isRunning}
                className="size-3.5 accent-current"
              />
              Stream progress
            </label>
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <StrategySelector />
          <Button
            onClick={handleRun}
            disabled={!canRun}
            className="w-full"
            aria-busy={isRunning}
          >
            {isRunning
              ? 'Running…'
              : `Run backtest${selectedSymbol ? ` on ${selectedSymbol}` : ''}`}
          </Button>
        </CardContent>
      </Card>

      {isRunning ? (
        <Card>
          <CardContent className="space-y-4 pt-6">
            {streamProgress ? (
              <BacktestProgress progress={socket.progress} />
            ) : (
              // No progress to show over REST — the request is opaque until it
              // returns, which is the limitation the websocket exists to fix.
              <>
                <Skeleton className="h-72 w-full" />
                <Skeleton className="h-24 w-full" />
              </>
            )}
          </CardContent>
        </Card>
      ) : null}

      {errorMessage && !isRunning ? (
        <Alert variant="destructive">
          <AlertTitle>{errorTitle}</AlertTitle>
          <AlertDescription>{errorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {result && !isRunning ? (
        <Card>
          <CardHeader>
            <CardTitle>Results</CardTitle>
            <CardDescription>
              {result.strategyName} on {result.symbol}
              <span className="ml-2 text-xs">
                via {result.via === 'websocket' ? 'websocket' : 'REST'}
              </span>
            </CardDescription>
          </CardHeader>
          <CardContent>
            {result.caveat ? (
              <Alert variant="destructive" className="mb-4">
                <AlertTitle>Results are not trustworthy</AlertTitle>
                <AlertDescription>{result.caveat}</AlertDescription>
              </Alert>
            ) : null}
            <BacktestResults result={result} />
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}
