/**
 * components/backtest/StrategySelector.tsx
 *
 * Strategy picker plus parameter controls, both generated from the registry
 * schema served by GET /api/v1/strategies.
 *
 * Nothing here knows any strategy by name. Registering a strategy in
 * alpha_models/registry.py makes it appear in this dropdown with its own
 * controls, correct types, defaults and bounds — no frontend change. That is
 * the payoff the migration guide describes, and the reason parameters must
 * never be hardcoded in this file.
 *
 * Phase 4 — React frontend
 */

import { useEffect, useMemo } from 'react'

import type { ParamSchema, StrategySchema } from '@/api/client'
import { useStrategies } from '@/api/queries'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
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

/** Defaults for every parameter a strategy declares. */
function defaultsFor(strategy: StrategySchema): Record<string, number | string> {
  const out: Record<string, number | string> = {}
  for (const param of strategy.params) {
    out[param.name] = param.default as number | string
  }
  return out
}

function ParamControl({
  param,
  value,
  onChange,
}: {
  param: ParamSchema
  value: number | string | undefined
  onChange: (value: number | string) => void
}) {
  const current = value ?? (param.default as number | string)
  const isNumeric = param.type === 'int' || param.type === 'float'

  return (
    <div className="space-y-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <Label htmlFor={`param-${param.name}`}>{param.label}</Label>
        {isNumeric && param.minimum != null && param.maximum != null ? (
          <span className="text-xs text-muted-foreground">
            {param.minimum}–{param.maximum}
          </span>
        ) : null}
      </div>
      <Input
        id={`param-${param.name}`}
        type={isNumeric ? 'number' : 'text'}
        value={String(current)}
        // Bounds are advisory hints from the registry; the strategy validates
        // its own inputs and the API returns 422, which the results panel shows.
        min={param.minimum ?? undefined}
        max={param.maximum ?? undefined}
        step={param.type === 'float' ? 0.1 : 1}
        onChange={(event) => {
          const raw = event.target.value
          if (!isNumeric) return onChange(raw)
          // Keep an empty field empty rather than coercing to 0 mid-typing.
          if (raw === '') return onChange('')
          const parsed = param.type === 'int' ? parseInt(raw, 10) : parseFloat(raw)
          onChange(Number.isNaN(parsed) ? raw : parsed)
        }}
      />
      {param.description ? (
        <p className="text-xs text-muted-foreground">{param.description}</p>
      ) : null}
    </div>
  )
}

export function StrategySelector() {
  const { data, isLoading, isError, error } = useStrategies('single')
  const strategyId = useAppStore((s) => s.strategyId)
  const strategyParams = useAppStore((s) => s.strategyParams)
  const setStrategy = useAppStore((s) => s.setStrategy)
  const setStrategyParam = useAppStore((s) => s.setStrategyParam)
  const resetStrategyParams = useAppStore((s) => s.resetStrategyParams)

  // Memoised: `data?.strategies ?? []` allocates a new array on every render,
  // which would change the effect deps below every time and re-run it forever.
  const strategies = useMemo(() => data?.strategies ?? [], [data])
  const selected = strategies.find((s) => s.id === strategyId) ?? null

  // Select the first strategy once the catalogue arrives, so the panel is
  // usable without a click. Only when nothing is chosen yet.
  useEffect(() => {
    if (!strategyId && strategies.length > 0) {
      setStrategy(strategies[0].id)
    }
  }, [strategyId, strategies, setStrategy])

  // Seed params from the schema whenever the strategy changes. setStrategy
  // clears them (names are strategy-specific), so this refills with defaults.
  useEffect(() => {
    if (selected && Object.keys(strategyParams).length === 0 && selected.params.length > 0) {
      resetStrategyParams(defaultsFor(selected))
    }
  }, [selected, strategyParams, resetStrategyParams])

  if (isLoading) return <Skeleton className="h-32 w-full" />

  if (isError) {
    return (
      <Alert variant="destructive">
        <AlertTitle>Could not load strategies</AlertTitle>
        <AlertDescription>{String(error)}</AlertDescription>
      </Alert>
    )
  }

  return (
    <div className="space-y-4">
      <div className="space-y-1.5">
        <Label htmlFor="strategy">Strategy</Label>
        <Select value={strategyId ?? undefined} onValueChange={setStrategy}>
          <SelectTrigger id="strategy" className="w-full">
            {/* Explicit children, not a bare <SelectValue/>: the trigger
                otherwise renders the raw value — the registry id
                ("ma_crossover") rather than "Moving Average Crossover". The
                symbol picker hides this because there the value IS the label. */}
            <SelectValue placeholder="Select a strategy">
              {selected?.display_name}
            </SelectValue>
          </SelectTrigger>
          <SelectContent>
            {strategies.map((strategy) => (
              <SelectItem key={strategy.id} value={strategy.id}>
                {strategy.display_name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {selected?.description ? (
          <p className="text-xs text-muted-foreground">{selected.description}</p>
        ) : null}
      </div>

      {/* Registry-declared caveats are surfaced, not hidden — ml_random_forest
          has known look-ahead bias and its results are not achievable live. */}
      {selected?.caveat ? (
        <Alert variant="destructive">
          <AlertTitle>Known limitation</AlertTitle>
          <AlertDescription>{selected.caveat}</AlertDescription>
        </Alert>
      ) : null}

      {selected && selected.params.length > 0 ? (
        <div className="space-y-3">
          {selected.params.map((param) => (
            <ParamControl
              key={param.name}
              param={param}
              value={strategyParams[param.name]}
              onChange={(value) => setStrategyParam(param.name, value)}
            />
          ))}
        </div>
      ) : selected ? (
        <p className="text-xs text-muted-foreground">
          This strategy takes no parameters.
        </p>
      ) : null}
    </div>
  )
}
