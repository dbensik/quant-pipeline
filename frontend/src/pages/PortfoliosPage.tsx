/**
 * pages/PortfoliosPage.tsx
 * Trade log, derived positions and P&L, rebalancing previews.
 *
 * Ported from the Streamlit portfolio tab, which read and wrote
 * `portfolios.json`. Portfolios now live in the database behind
 * /api/v1/portfolios, and — the important part — the TRADE LOG IS THE ONLY
 * STORED STATE. Cash, positions, average cost and P&L are derived server-side,
 * so nothing here computes them locally; that is what kept the old file's two
 * copies disagreeing.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useEffect, useState } from 'react'
import { Plus, Trash2 } from 'lucide-react'

import type { ApiError, PositionOut } from '@/api/client'
import {
  useAddTrade,
  useCreatePortfolio,
  useDeletePortfolio,
  useDeleteTrade,
  usePortfolio,
  usePortfolios,
  useRebalancePreview,
  useTrades,
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

const money = (value: number | null | undefined) =>
  value == null
    ? '—'
    : value.toLocaleString(undefined, {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 2,
      })

const signed = (value: number | null | undefined) =>
  value == null ? '—' : `${value >= 0 ? '+' : ''}${money(value)}`

function pnlClass(value: number | null | undefined) {
  if (value == null || value === 0) return 'text-muted-foreground'
  return value > 0 ? 'text-emerald-600' : 'text-red-600'
}

function Metric({
  label,
  value,
  tone,
}: {
  label: string
  value: string
  tone?: string
}) {
  return (
    <div className="rounded-lg border p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={`mt-1 text-lg font-semibold tabular-nums ${tone ?? ''}`}>
        {value}
      </div>
    </div>
  )
}

function Positions({ positions }: { positions: PositionOut[] }) {
  if (positions.length === 0) {
    return <p className="text-sm text-muted-foreground">No open positions.</p>
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b text-left text-xs uppercase text-muted-foreground">
          <tr>
            <th className="py-2 pr-4">Ticker</th>
            <th className="py-2 pr-4 text-right">Qty</th>
            <th className="py-2 pr-4 text-right">Avg cost</th>
            <th className="py-2 pr-4 text-right">Last</th>
            <th className="py-2 pr-4 text-right">Value</th>
            <th className="py-2 text-right">Unrealised</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => (
            <tr key={p.ticker} className="border-b last:border-0">
              <td className="py-2 pr-4 font-medium">
                {p.ticker}
                {/* A short is a negative quantity — there is no direction
                    field. Labelled so it is not read as a data error. */}
                {p.quantity < 0 ? (
                  <Badge variant="outline" className="ml-2">
                    short
                  </Badge>
                ) : null}
              </td>
              <td className="py-2 pr-4 text-right tabular-nums">{p.quantity}</td>
              <td className="py-2 pr-4 text-right tabular-nums">
                {money(p.average_price)}
              </td>
              <td className="py-2 pr-4 text-right tabular-nums">
                {money(p.last_price)}
              </td>
              <td className="py-2 pr-4 text-right tabular-nums">
                {money(p.market_value)}
              </td>
              <td
                className={`py-2 text-right tabular-nums ${pnlClass(p.unrealised_pnl)}`}
              >
                {signed(p.unrealised_pnl)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function PortfoliosPage() {
  const { data: portfolios, isLoading } = usePortfolios()
  const [selected, setSelected] = useState<string | null>(null)

  const { data: state, isLoading: stateLoading } = usePortfolio(selected)
  const { data: trades } = useTrades(selected)

  const createPortfolio = useCreatePortfolio()
  const deletePortfolio = useDeletePortfolio()
  const addTrade = useAddTrade()
  const deleteTrade = useDeleteTrade()
  const rebalance = useRebalancePreview()

  const [newName, setNewName] = useState('')
  const [newCash, setNewCash] = useState('100000')
  const [form, setForm] = useState({
    ticker: '',
    action: 'BUY',
    quantity: '',
    price: '',
    costs: '',
  })
  const [allowOverdraft, setAllowOverdraft] = useState(false)
  const [weights, setWeights] = useState('')

  // Select the first portfolio once the list arrives, so the page is not an
  // empty shell on load.
  useEffect(() => {
    if (!selected && portfolios?.length) setSelected(portfolios[0].name)
  }, [portfolios, selected])

  function submitTrade() {
    if (!selected) return
    addTrade.mutate(
      {
        name: selected,
        trade: {
          ticker: form.ticker.toUpperCase().trim(),
          action: form.action,
          quantity: Number(form.quantity),
          price: Number(form.price),
          costs: form.costs ? Number(form.costs) : 0,
          time: null,
          broker: null,
          notes: null,
        },
        allowOverdraft,
      },
      {
        onSuccess: () =>
          setForm({ ticker: '', action: 'BUY', quantity: '', price: '', costs: '' }),
      },
    )
  }

  function submitRebalance() {
    if (!selected) return
    const parsed: Record<string, number> = {}
    for (const part of weights.split(',')) {
      const [ticker, value] = part.split(':').map((s) => s?.trim())
      if (ticker && value) parsed[ticker.toUpperCase()] = Number(value)
    }
    rebalance.mutate({ name: selected, targetWeights: parsed })
  }

  const tradeError = addTrade.error as ApiError | null
  const overdraft = tradeError?.detail?.includes('Insufficient cash')

  return (
    <div>
      <PageHeader
        title="Portfolios"
        blurb="The trade log is the only stored state — cash, positions and P&L are derived from it."
      />

      <div className="grid gap-6 lg:grid-cols-[260px_1fr]">
        {/* -- picker --------------------------------------------------- */}
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-base">Portfolios</CardTitle>
          </CardHeader>
          <CardContent className="space-y-1">
            {isLoading ? <Skeleton className="h-9 w-full" /> : null}
            {portfolios?.map((p) => (
              <div key={p.name} className="flex items-center gap-1">
                <Button
                  variant={selected === p.name ? 'secondary' : 'ghost'}
                  className="flex-1 justify-start"
                  onClick={() => setSelected(p.name)}
                >
                  <span className="truncate">{p.name}</span>
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={`Delete ${p.name}`}
                  onClick={() =>
                    deletePortfolio.mutate(p.name, {
                      onSuccess: () =>
                        setSelected((current) =>
                          current === p.name ? null : current,
                        ),
                    })
                  }
                >
                  <Trash2 className="h-4 w-4 text-muted-foreground" />
                </Button>
              </div>
            ))}

            <div className="space-y-2 border-t pt-3">
              <Label htmlFor="new-portfolio">New portfolio</Label>
              <Input
                id="new-portfolio"
                value={newName}
                placeholder="Name"
                onChange={(e) => setNewName(e.target.value)}
              />
              <Input
                aria-label="Initial cash"
                value={newCash}
                inputMode="decimal"
                onChange={(e) => setNewCash(e.target.value)}
              />
              <Button
                className="w-full"
                disabled={!newName.trim() || createPortfolio.isPending}
                onClick={() =>
                  createPortfolio.mutate(
                    { name: newName.trim(), initial_cash: Number(newCash) || 100000 },
                    {
                      onSuccess: (created) => {
                        setSelected(created.name)
                        setNewName('')
                      },
                    },
                  )
                }
              >
                <Plus className="mr-2 h-4 w-4" />
                Create
              </Button>
              {createPortfolio.error ? (
                <Alert variant="destructive">
                  <AlertDescription>
                    {(createPortfolio.error as ApiError).detail}
                  </AlertDescription>
                </Alert>
              ) : null}
            </div>
          </CardContent>
        </Card>

        {/* -- detail --------------------------------------------------- */}
        <div className="space-y-6">
          {!selected ? (
            <Card>
              <CardContent className="py-10 text-center text-sm text-muted-foreground">
                Select a portfolio, or create one.
              </CardContent>
            </Card>
          ) : null}

          {selected && stateLoading ? <Skeleton className="h-32 w-full" /> : null}

          {selected && state ? (
            <>
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">{state.name}</CardTitle>
                  <CardDescription>
                    {state.trade_count} trade{state.trade_count === 1 ? '' : 's'}
                    {state.priced_at
                      ? ` · valued at ${new Date(state.priced_at).toLocaleDateString()}`
                      : ' · not priced'}
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-5">
                    <Metric label="Total equity" value={money(state.total_equity)} />
                    <Metric label="Cash" value={money(state.cash)} />
                    <Metric label="Market value" value={money(state.market_value)} />
                    <Metric
                      label="Realised P&L"
                      value={signed(state.realised_pnl)}
                      tone={pnlClass(state.realised_pnl)}
                    />
                    <Metric
                      label="Unrealised P&L"
                      value={signed(state.unrealised_pnl)}
                      tone={pnlClass(state.unrealised_pnl)}
                    />
                  </div>

                  {/* Optional in the generated schema because the field has a
                      default server-side; treated as empty when absent. */}
                  {(state.unpriced ?? []).length > 0 ? (
                    <Alert>
                      <AlertDescription>
                        No price available for {(state.unpriced ?? []).join(', ')} — excluded
                        from market value rather than valued at cost, which would
                        overstate equity.
                      </AlertDescription>
                    </Alert>
                  ) : null}

                  {state.cash < 0 ? (
                    <Alert variant="destructive">
                      <AlertDescription>
                        Cash is negative — this portfolio holds more than it funded.
                      </AlertDescription>
                    </Alert>
                  ) : null}

                  <Positions positions={state.positions} />
                </CardContent>
              </Card>

              {/* -- record a trade ---------------------------------- */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Record a trade</CardTitle>
                  <CardDescription>
                    Selling more than you hold opens a short — there is no separate
                    direction field.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid gap-3 sm:grid-cols-5">
                    <div className="space-y-1">
                      <Label htmlFor="trade-ticker">Ticker</Label>
                      <Input
                        id="trade-ticker"
                        value={form.ticker}
                        onChange={(e) => setForm({ ...form, ticker: e.target.value })}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="trade-action">Action</Label>
                      <select
                        id="trade-action"
                        className="h-9 w-full rounded-md border bg-transparent px-3 text-sm"
                        value={form.action}
                        onChange={(e) => setForm({ ...form, action: e.target.value })}
                      >
                        <option value="BUY">BUY</option>
                        <option value="SELL">SELL</option>
                      </select>
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="trade-qty">Quantity</Label>
                      <Input
                        id="trade-qty"
                        inputMode="decimal"
                        value={form.quantity}
                        onChange={(e) =>
                          setForm({ ...form, quantity: e.target.value })
                        }
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="trade-price">Price</Label>
                      <Input
                        id="trade-price"
                        inputMode="decimal"
                        value={form.price}
                        onChange={(e) => setForm({ ...form, price: e.target.value })}
                      />
                    </div>
                    <div className="space-y-1">
                      <Label htmlFor="trade-costs">Costs</Label>
                      <Input
                        id="trade-costs"
                        inputMode="decimal"
                        value={form.costs}
                        onChange={(e) => setForm({ ...form, costs: e.target.value })}
                      />
                    </div>
                  </div>

                  {tradeError ? (
                    <Alert variant="destructive">
                      <AlertDescription>
                        {tradeError.detail}
                        {/* The API rejects overdrawing buys by default so paper
                            trading cannot run on unlimited leverage, but the same
                            endpoint records real trades that already happened. */}
                        {overdraft ? (
                          <div className="mt-2">
                            <label className="flex items-center gap-2">
                              <input
                                type="checkbox"
                                checked={allowOverdraft}
                                onChange={(e) => setAllowOverdraft(e.target.checked)}
                              />
                              Record it anyway (this already happened elsewhere)
                            </label>
                          </div>
                        ) : null}
                      </AlertDescription>
                    </Alert>
                  ) : null}

                  <Button
                    onClick={submitTrade}
                    disabled={
                      !form.ticker.trim() ||
                      !form.quantity ||
                      !form.price ||
                      addTrade.isPending
                    }
                  >
                    {addTrade.isPending ? 'Recording…' : 'Record trade'}
                  </Button>
                </CardContent>
              </Card>

              {/* -- trade log --------------------------------------- */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Trade log</CardTitle>
                  <CardDescription>Oldest first — order sets cost basis.</CardDescription>
                </CardHeader>
                <CardContent>
                  {!trades?.length ? (
                    <p className="text-sm text-muted-foreground">No trades yet.</p>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                          <tr>
                            <th className="py-2 pr-4">Date</th>
                            <th className="py-2 pr-4">Ticker</th>
                            <th className="py-2 pr-4">Action</th>
                            <th className="py-2 pr-4 text-right">Qty</th>
                            <th className="py-2 pr-4 text-right">Price</th>
                            <th className="py-2 pr-4 text-right">Costs</th>
                            <th className="py-2" />
                          </tr>
                        </thead>
                        <tbody>
                          {trades.map((t) => (
                            <tr key={t.id} className="border-b last:border-0">
                              <td className="py-2 pr-4">
                                {t.time
                                  ? new Date(t.time).toLocaleDateString()
                                  : '—'}
                              </td>
                              <td className="py-2 pr-4 font-medium">{t.ticker}</td>
                              <td className="py-2 pr-4">
                                <Badge
                                  variant={
                                    t.action === 'BUY' ? 'secondary' : 'outline'
                                  }
                                >
                                  {t.action}
                                </Badge>
                              </td>
                              <td className="py-2 pr-4 text-right tabular-nums">
                                {t.quantity}
                              </td>
                              <td className="py-2 pr-4 text-right tabular-nums">
                                {money(t.price)}
                              </td>
                              <td className="py-2 pr-4 text-right tabular-nums">
                                {money(t.costs ?? 0)}
                              </td>
                              <td className="py-2 text-right">
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  aria-label={`Delete trade ${t.id}`}
                                  onClick={() =>
                                    deleteTrade.mutate({
                                      name: selected,
                                      tradeId: t.id,
                                    })
                                  }
                                >
                                  <Trash2 className="h-4 w-4 text-muted-foreground" />
                                </Button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* -- rebalance --------------------------------------- */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Rebalance preview</CardTitle>
                  <CardDescription>
                    Returns orders; it does not record them. Post them as trades to
                    act on them.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-1">
                    <Label htmlFor="weights">Target weights</Label>
                    <Input
                      id="weights"
                      value={weights}
                      placeholder="AAPL: 0.5, MSFT: 0.3"
                      onChange={(e) => setWeights(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      Fractions of total equity. Anything omitted targets zero and is
                      sold down.
                    </p>
                  </div>

                  <Button
                    onClick={submitRebalance}
                    disabled={!weights.trim() || rebalance.isPending}
                  >
                    {rebalance.isPending ? 'Computing…' : 'Preview orders'}
                  </Button>

                  {rebalance.error ? (
                    <Alert variant="destructive">
                      <AlertDescription>
                        {(rebalance.error as ApiError).detail}
                      </AlertDescription>
                    </Alert>
                  ) : null}

                  {rebalance.data ? (
                    <div className="space-y-3">
                      <p className="text-sm text-muted-foreground">
                        Equity {money(rebalance.data.total_equity)} ·{' '}
                        {rebalance.data.orders.length} order
                        {rebalance.data.orders.length === 1 ? '' : 's'}
                      </p>
                      {(rebalance.data.unpriced ?? []).length > 0 ? (
                        <Alert>
                          <AlertDescription>
                            Skipped (no price to size an order):{' '}
                            {(rebalance.data.unpriced ?? []).join(', ')}
                          </AlertDescription>
                        </Alert>
                      ) : null}
                      {rebalance.data.orders.length > 0 ? (
                        <table className="w-full text-sm">
                          <thead className="border-b text-left text-xs uppercase text-muted-foreground">
                            <tr>
                              <th className="py-2 pr-4">Ticker</th>
                              <th className="py-2 pr-4">Action</th>
                              <th className="py-2 pr-4 text-right">Qty</th>
                              <th className="py-2 pr-4 text-right">Value</th>
                              <th className="py-2 text-right">Weight</th>
                            </tr>
                          </thead>
                          <tbody>
                            {rebalance.data.orders.map((o) => (
                              <tr key={o.ticker} className="border-b last:border-0">
                                <td className="py-2 pr-4 font-medium">{o.ticker}</td>
                                <td className="py-2 pr-4">
                                  <Badge
                                    variant={
                                      o.action === 'BUY' ? 'secondary' : 'outline'
                                    }
                                  >
                                    {o.action}
                                  </Badge>
                                </td>
                                <td className="py-2 pr-4 text-right tabular-nums">
                                  {o.quantity}
                                </td>
                                <td className="py-2 pr-4 text-right tabular-nums">
                                  {money(o.value)}
                                </td>
                                <td className="py-2 text-right tabular-nums">
                                  {(o.current_weight * 100).toFixed(1)}% →{' '}
                                  {(o.target_weight * 100).toFixed(1)}%
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : null}
                    </div>
                  ) : null}
                </CardContent>
              </Card>
            </>
          ) : null}
        </div>
      </div>
    </div>
  )
}
