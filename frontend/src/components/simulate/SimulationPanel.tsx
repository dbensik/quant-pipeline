/**
 * components/simulate/SimulationPanel.tsx
 * Monte Carlo over a completed backtest.
 *
 * The simulation takes the backtest's own symbol, strategy, parameters,
 * window, capital and seed — the only new inputs are how to resample. It is
 * rendered under the backtest result for exactly that reason.
 *
 * Phase 4 of research/monte-carlo-plan-2026-10-07.md
 */

import { useState } from 'react'

import type { BacktestResult } from '@/api/backtestResult'
import { ApiError } from '@/api/client'
import type { SimulationInput, SimulationResponse } from '@/api/client'
import { useRunSimulation, useSaveResult } from '@/api/queries'
import { useSimulationSocket } from '@/api/useSimulationSocket'
import { BacktestProgress } from '@/components/backtest/BacktestProgress'
import { SimulationResults } from '@/components/simulate/SimulationResults'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useAppStore } from '@/store/useAppStore'

type Mode = SimulationInput['mode']
type Method = SimulationInput['method']

const MODES: Array<{ value: NonNullable<Mode>; label: string; hint: string }> = [
  {
    value: 'returns',
    label: 'Strategy returns',
    hint: 'Resample the realised daily returns. Instant; treats signals as path-independent.',
  },
  {
    value: 'prices',
    label: 'Price paths, re-run',
    hint: 'Resample price returns and re-run the strategy on each path. One backtest per path.',
  },
]

const METHODS: Array<{ value: NonNullable<Method>; label: string }> = [
  { value: 'stationary', label: 'Stationary block bootstrap' },
  { value: 'iid', label: 'iid bootstrap' },
  { value: 'gbm', label: 'Geometric Brownian motion' },
]

const SIMULATION_LABELS = {
  fetching: 'Loading history',
  running: 'Simulating paths',
  summarising: 'Computing bands',
}

function resultName(result: SimulationResponse): string {
  const stamp = new Date().toISOString().slice(0, 19).replace(/[-:]/g, '').replace('T', '-')
  return `simulation-${result.symbol}-${result.strategy_id}-${result.mode}-${stamp}`
}

export function SimulationPanel({ backtest }: { backtest: BacktestResult }) {
  const { startDate, endDate, streamProgress } = useAppStore()
  const [mode, setMode] = useState<NonNullable<Mode>>('returns')
  const [method, setMethod] = useState<NonNullable<Method>>('stationary')
  const [nPaths, setNPaths] = useState('')
  const [horizonDays, setHorizonDays] = useState('')
  const [blockLength, setBlockLength] = useState('20')
  const [allowUnverified, setAllowUnverified] = useState(false)

  const socket = useSimulationSocket()
  const rest = useRunSimulation()
  const save = useSaveResult()

  const isRunning = streamProgress ? socket.isRunning : rest.isPending
  const result: SimulationResponse | null = streamProgress ? socket.result : (rest.data ?? null)
  const errorMessage = streamProgress
    ? socket.error
    : rest.error
      ? rest.error instanceof ApiError
        ? rest.error.detail
        : String(rest.error)
      : null

  function handleRun() {
    const request: SimulationInput = {
      symbol: backtest.symbol,
      strategy_id: backtest.strategyId,
      start: startDate,
      end: endDate,
      params: backtest.params as Record<string, unknown>,
      initial_capital: backtest.initialCapital,
      seed: backtest.seed,
      mode,
      method,
      allow_unverified: allowUnverified,
      ...(nPaths !== '' ? { n_paths: Number(nPaths) } : {}),
      ...(horizonDays !== '' ? { horizon_days: Number(horizonDays) } : {}),
      ...(method === 'stationary' && blockLength !== '' ? { block_length_days: Number(blockLength) } : {}),
    }
    save.reset()
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
          <CardTitle>Simulate</CardTitle>
          <CardDescription>
            Monte Carlo around this backtest: same symbol, strategy, parameters, window and seed.
            Only the resampling is chosen here.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor="sim-mode">Mode</Label>
              <Select value={mode} onValueChange={(value) => setMode(value as NonNullable<Mode>)}>
                <SelectTrigger id="sim-mode" className="w-full">
                  <SelectValue>{MODES.find((m) => m.value === mode)?.label}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {MODES.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">{MODES.find((m) => m.value === mode)?.hint}</p>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="sim-method">Method</Label>
              <Select value={method} onValueChange={(value) => setMethod(value as NonNullable<Method>)}>
                <SelectTrigger id="sim-method" className="w-full">
                  <SelectValue>{METHODS.find((m) => m.value === method)?.label}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {METHODS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {method === 'stationary'
                  ? 'Keeps volatility clustering. The default; iid and GBM understate the tails.'
                  : 'Shown for contrast with the block bootstrap; understates the tails.'}
              </p>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="sim-paths">Paths</Label>
              <Input
                id="sim-paths"
                type="number"
                min={1}
                placeholder={mode === 'prices' ? 'server default (200)' : 'server default (2,000)'}
                value={nPaths}
                onChange={(event) => setNPaths(event.target.value)}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="sim-horizon">Horizon (trading days)</Label>
              <Input
                id="sim-horizon"
                type="number"
                min={1}
                placeholder="history length"
                value={horizonDays}
                onChange={(event) => setHorizonDays(event.target.value)}
              />
            </div>
            {method === 'stationary' ? (
              <div className="space-y-1.5">
                <Label htmlFor="sim-block">Mean block (days)</Label>
                <Input
                  id="sim-block"
                  type="number"
                  min={1}
                  value={blockLength}
                  onChange={(event) => setBlockLength(event.target.value)}
                />
              </div>
            ) : null}
            <label className="flex items-center gap-2 self-end pb-2 text-xs">
              <input
                type="checkbox"
                checked={allowUnverified}
                onChange={() => setAllowUnverified((value) => !value)}
                className="size-3.5 accent-current"
              />
              Allow an asset that is not read-time adjusted
            </label>
          </div>
          <Button onClick={handleRun} disabled={isRunning} className="w-full" aria-busy={isRunning}>
            {isRunning ? 'Simulating…' : `Simulate ${backtest.strategyName} on ${backtest.symbol}`}
          </Button>
        </CardContent>
      </Card>

      {isRunning && streamProgress ? (
        <Card>
          <CardContent className="pt-6">
            <BacktestProgress progress={socket.progress} labels={SIMULATION_LABELS} />
          </CardContent>
        </Card>
      ) : null}

      {errorMessage && !isRunning ? (
        <Alert variant="destructive">
          <AlertTitle>Simulation failed</AlertTitle>
          <AlertDescription>{errorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {result && !isRunning ? (
        <Card>
          <CardHeader>
            <CardTitle>Simulation</CardTitle>
            <CardDescription className="flex items-center justify-between gap-4">
              <span>
                {result.strategy_name} on {result.symbol}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={save.isPending || save.isSuccess}
                onClick={() => save.mutate({ name: resultName(result), payload: result })}
              >
                {save.isSuccess ? 'Saved to Results' : save.isPending ? 'Saving…' : 'Save to Results'}
              </Button>
            </CardDescription>
          </CardHeader>
          <CardContent>
            {result.caveat ? (
              <Alert variant="destructive" className="mb-4">
                <AlertTitle>Read the bands with this in mind</AlertTitle>
                <AlertDescription>{result.caveat}</AlertDescription>
              </Alert>
            ) : null}
            {save.error ? (
              <Alert variant="destructive" className="mb-4">
                <AlertTitle>Could not save</AlertTitle>
                <AlertDescription>
                  {save.error instanceof ApiError ? save.error.detail : String(save.error)}
                </AlertDescription>
              </Alert>
            ) : null}
            <SimulationResults result={result} historicalCurve={backtest.equityCurve} />
          </CardContent>
        </Card>
      ) : null}
    </div>
  )
}
