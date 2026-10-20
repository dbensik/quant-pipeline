/**
 * components/portfolios/StrategyCard.tsx
 * A strategy attached to a paper portfolio (decision 50,
 * research/paper-trader-2026-10-20.md): attach one to an empty portfolio,
 * or see what the attached one would trade next and whether what it holds
 * is what it wants. The 06:00 job writes the fills; this card only reads
 * (the run endpoint's dry run) unless "Trade now" is pressed.
 *
 * Decision 51 (research/paper-tracking-2026-10-20.md): the paper record beside
 * a backtest of the same strategy from the attach date, the gap split into
 * what is measured, and RESTATED decisions.
 */

import { useState } from 'react'

import type { ApiError, PortfolioStateOut, StrategySchema } from '@/api/client'
import {
  useAttachStrategy,
  useDetachStrategy,
  useRunStrategy,
  useStrategies,
  useStrategyPreview,
  useTracking,
} from '@/api/queries'
import { ComparisonChart } from '@/components/charts/ComparisonChart'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { parseSymbols } from '@/lib/allocation'

function defaultsOf(spec: StrategySchema | undefined): Record<string, string> {
  return Object.fromEntries((spec?.params ?? []).map((p) => [p.name, String(p.default ?? '')]))
}

function typed(spec: StrategySchema | undefined, raw: Record<string, string>) {
  const out: Record<string, unknown> = {}
  for (const p of spec?.params ?? []) {
    const value = raw[p.name]
    if (value === undefined || value === '') continue
    out[p.name] = p.type === 'str' ? value : Number(value)
  }
  return out
}

function AttachForm({ name }: { name: string }) {
  const { data: catalogue } = useStrategies()
  const attach = useAttachStrategy()
  const strategies = catalogue?.strategies ?? []
  const [strategyId, setStrategyId] = useState('asset_class_trend')
  const spec = strategies.find((s) => s.id === strategyId)
  const [symbolsText, setSymbolsText] = useState('SPY, EFA, IEF, VNQ, GLD')
  const [paramsFor, setParamsFor] = useState('')
  const [params, setParams] = useState<Record<string, string>>({})
  const [costBps, setCostBps] = useState('10')
  const current = paramsFor === strategyId ? params : defaultsOf(spec)
  const error = attach.error as ApiError | null
  return (
    <div className="space-y-3">
      <div className="grid gap-3 sm:grid-cols-3">
        <div className="space-y-1">
          <Label htmlFor="attach-strategy">Strategy</Label>
          <select
            id="attach-strategy"
            className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
            value={strategyId}
            onChange={(e) => setStrategyId(e.target.value)}
          >
            {strategies.map((s) => (
              <option key={s.id} value={s.id}>
                {s.display_name}
              </option>
            ))}
          </select>
        </div>
        <div className="space-y-1 sm:col-span-2">
          <Label htmlFor="attach-symbols">Symbols</Label>
          <Input
            id="attach-symbols"
            value={symbolsText}
            onChange={(e) => setSymbolsText(e.target.value)}
          />
        </div>
        {(spec?.params ?? []).map((p) => (
          <div key={p.name} className="space-y-1">
            <Label htmlFor={`attach-param-${p.name}`}>{p.label}</Label>
            <Input
              id={`attach-param-${p.name}`}
              type={p.type === 'str' ? 'text' : 'number'}
              value={current[p.name] ?? ''}
              onChange={(e) => {
                setParamsFor(strategyId)
                setParams({ ...current, [p.name]: e.target.value })
              }}
            />
          </div>
        ))}
        <div className="space-y-1">
          <Label htmlFor="attach-cost">Cost per fill (bps)</Label>
          <Input
            id="attach-cost"
            type="number"
            value={costBps}
            onChange={(e) => setCostBps(e.target.value)}
          />
        </div>
      </div>
      {spec?.shorts ? (
        <p className="text-xs text-muted-foreground">
          This strategy shorts: the portfolio&apos;s short terms (below) set its borrow and margin.
        </p>
      ) : null}
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error.detail}</AlertDescription>
        </Alert>
      ) : null}
      <Button
        disabled={attach.isPending || !strategyId}
        onClick={() =>
          attach.mutate({
            name,
            body: {
              strategy_id: strategyId,
              symbols: parseSymbols(symbolsText),
              params: typed(spec, current),
              transaction_cost: Number(costBps) / 10_000,
            },
          })
        }
      >
        Attach
      </Button>
    </div>
  )
}

const signedMoney = (v: number | null | undefined) =>
  v == null
    ? '—'
    : `${v >= 0 ? '+' : '-'}${Math.abs(v).toLocaleString(undefined, {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 2,
      })}`

function Tracking({ name }: { name: string }) {
  const { data, error } = useTracking(name, true)
  if (error) {
    return (
      <Alert>
        <AlertDescription>
          Comparison unavailable: {(error as ApiError).detail}
        </AlertDescription>
      </Alert>
    )
  }
  if (!data) return null
  const restated = data.restated ?? []
  return (
    <div className="space-y-2 border-t pt-3">
      <div className="text-sm font-medium">Against its own backtest</div>
      {restated.length ? (
        <Alert variant="destructive">
          <AlertDescription>
            {(data.flags ?? []).map((f) => (
              <div key={f}>{f}</div>
            ))}
            The record is kept as traded; the history it traded on has moved.
          </AlertDescription>
        </Alert>
      ) : null}
      {data.gap == null ? (
        <p className="text-sm text-muted-foreground">
          No close since it was attached yet: nothing to compare.
        </p>
      ) : (
        <>
          <ComparisonChart
            series={[
              { name: 'Paper', points: data.paper },
              { name: 'Backtest from the attach date', points: data.backtest },
            ]}
          />
          <table className="w-full text-sm">
            <tbody>
              <tr className="border-b">
                <td className="py-1 pr-4">Paper less backtest</td>
                <td className="py-1 text-right tabular-nums">{signedMoney(data.gap)}</td>
              </tr>
              <tr className="border-b">
                <td className="py-1 pr-4">Slippage the backtest paid</td>
                <td className="py-1 text-right tabular-nums">
                  {signedMoney(data.backtest_slippage)}
                </td>
              </tr>
              <tr className="border-b">
                <td className="py-1 pr-4">Costs: backtest less paper</td>
                <td className="py-1 text-right tabular-nums">
                  {signedMoney(data.costs_difference)}
                </td>
              </tr>
              <tr>
                <td className="py-1 pr-4">
                  Other (raw-open sizing, dividends as cash, interest method)
                </td>
                <td className="py-1 text-right tabular-nums">{signedMoney(data.other)}</td>
              </tr>
            </tbody>
          </table>
        </>
      )}
    </div>
  )
}

function Attached({ state }: { state: PortfolioStateOut }) {
  const strategy = state.strategy!
  const preview = useStrategyPreview(state.name, true)
  const detach = useDetachStrategy()
  const run = useRunStrategy()
  const data = preview.data
  const pending = (data?.sessions ?? []).flatMap((s) =>
    s.fills.map((f) => ({ ...f, session: s.session })),
  )
  const flags = data?.flags ?? []
  const next = Object.entries(data?.next_signals ?? {})
  const error = (preview.error ?? detach.error ?? run.error) as ApiError | null
  return (
    <div className="space-y-3">
      <p className="text-sm">
        <span className="font-medium">{strategy.strategy}</span> on {strategy.symbols.join(', ')}
        {Object.keys(strategy.params).length
          ? ` (${Object.entries(strategy.params)
              .map(([k, v]) => `${k} ${String(v)}`)
              .join(', ')})`
          : ''}
        . Attached {strategy.attached_on}; traded through {strategy.last_session}.
      </p>
      <p className="text-xs text-muted-foreground">
        Each session fills at its raw open what the close before it decided, at $1 plus{' '}
        {(strategy.transaction_cost * 10_000).toFixed(1)} bps a fill and no slippage draw (the
        backtests&apos; averages 0.025% worse). Manual trades are refused while it is attached.
      </p>
      {flags.length ? (
        <Alert variant="destructive">
          <AlertDescription>
            {flags.map((f) => (
              <div key={f}>{f}</div>
            ))}
          </AlertDescription>
        </Alert>
      ) : null}
      {data?.holdings?.length ? (
        <table className="w-full text-sm">
          <thead className="border-b text-left text-xs uppercase text-muted-foreground">
            <tr>
              <th className="py-2 pr-4">Ticker</th>
              <th className="py-2 pr-4">Wanted</th>
              <th className="py-2 pr-4 text-right">Held</th>
              <th className="py-2 text-right" />
            </tr>
          </thead>
          <tbody>
            {data.holdings.map((h) => (
              <tr key={h.ticker} className="border-b last:border-0">
                <td className="py-2 pr-4 font-medium">{h.ticker}</td>
                <td className="py-2 pr-4">{h.wanted ?? 'no signal since attach'}</td>
                <td className="py-2 pr-4 text-right tabular-nums">{h.held}</td>
                <td className="py-2 text-right">
                  {h.drift ? <Badge variant="destructive">drift</Badge> : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
      {pending.length ? (
        <div className="text-sm">
          The next run writes:
          <ul className="ml-4 list-disc">
            {pending.map((f, i) => (
              <li key={i}>
                {f.session}: {f.action} {f.quantity} {f.ticker} at {f.price.toFixed(2)}
              </li>
            ))}
          </ul>
        </div>
      ) : data ? (
        <p className="text-sm text-muted-foreground">
          Nothing to trade until the next bar
          {next.length
            ? `; its newest close asks for ${next.map(([k, v]) => `${k} ${v}`).join(', ')} at the next open`
            : ''}
          .
        </p>
      ) : null}
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error.detail}</AlertDescription>
        </Alert>
      ) : null}
      <div className="flex gap-2">
        <Button
          variant="outline"
          disabled={!pending.length || run.isPending}
          onClick={() => run.mutate(state.name)}
        >
          Trade now
        </Button>
        <Button variant="outline" onClick={() => detach.mutate(state.name)}>
          Detach
        </Button>
      </div>
      <Tracking name={state.name} />
    </div>
  )
}

export function StrategyCard({ state }: { state: PortfolioStateOut }) {
  const attachable = !state.strategy && state.trade_count === 0
  if (!state.strategy && !attachable) return null
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Strategy</CardTitle>
        <CardDescription>
          {state.strategy
            ? 'Traded forward by the 06:00 job, one session at a time, by its backtest’s rules.'
            : 'Attach a strategy to trade this empty portfolio forward from the next open. Its record is then the strategy’s alone.'}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {state.strategy ? <Attached state={state} /> : <AttachForm name={state.name} />}
      </CardContent>
    </Card>
  )
}
