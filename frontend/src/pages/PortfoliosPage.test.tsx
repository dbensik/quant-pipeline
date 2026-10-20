/**
 * PortfoliosPage.
 *
 * The page must never compute cash, positions or P&L itself — the API derives
 * them from the trade log, and a second implementation here is exactly the
 * divergence that made portfolios.json's two copies disagree. So these assert
 * that server values are RENDERED, and that a write invalidates the derived
 * state as well as the trade list.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { PortfoliosPage } from './PortfoliosPage'

const chartProps = vi.fn()
vi.mock('@/components/charts/ComparisonChart', () => ({
  ComparisonChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="comparison-chart" />
  },
}))

const SUMMARY = [
  { name: 'Growth', initial_cash: 100000, created_at: null, metadata: null },
]

const STATE = {
  name: 'Growth',
  initial_cash: 100000,
  cash: 33199,
  positions: [
    {
      ticker: 'AAPL',
      quantity: 30,
      average_price: 180,
      realised_pnl: 600,
      last_price: 209.11,
      market_value: 6273.3,
      unrealised_pnl: 873.3,
    },
    {
      ticker: 'TSLA',
      quantity: -5,
      average_price: 250,
      realised_pnl: 0,
      last_price: 240,
      market_value: -1200,
      unrealised_pnl: 50,
    },
  ],
  realised_pnl: 600,
  unrealised_pnl: 923.3,
  market_value: 5073.3,
  total_equity: 38272.3,
  trade_count: 3,
  unpriced: [],
  priced_at: '2026-08-07T00:00:00Z',
}

const TRADES = [
  {
    id: '1',
    ticker: 'AAPL',
    action: 'BUY',
    quantity: 50,
    price: 180,
    costs: 1,
    time: '2024-01-01T00:00:00Z',
    broker: null,
    notes: null,
  },
]

beforeEach(() => {
  vi.spyOn(api, 'listPortfolios').mockResolvedValue(SUMMARY as never)
  vi.spyOn(api, 'getPortfolio').mockResolvedValue(STATE as never)
  vi.spyOn(api, 'listTrades').mockResolvedValue(TRADES as never)
})

afterEach(() => vi.restoreAllMocks())

describe('PortfoliosPage — derived state is rendered, not recomputed', () => {
  it('shows the equity, cash and P&L the API derived', async () => {
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText('$38,272.30')).toBeInTheDocument()
    expect(screen.getByText('$33,199.00')).toBeInTheDocument()
    expect(screen.getByText('+$600.00')).toBeInTheDocument()
  })

  it('labels a negative quantity as a short rather than showing bad data', async () => {
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText('short')).toBeInTheDocument()
    expect(screen.getByText('-5')).toBeInTheDocument()
  })

  it('warns about positions excluded from market value', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue({
      ...STATE,
      unpriced: ['EMPTY-USD'],
    } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/No price available for EMPTY-USD/)).toBeInTheDocument()
  })

  it('warns when cash has gone negative', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue({
      ...STATE,
      cash: -500,
    } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/Cash is negative/)).toBeInTheDocument()
  })
})

describe('PortfoliosPage — recording trades', () => {
  it('posts the trade and refreshes the DERIVED state, not just the log', async () => {
    const state = vi.spyOn(api, 'getPortfolio')
    const add = vi.spyOn(api, 'addTrade').mockResolvedValue(TRADES[0] as never)

    renderPage(<PortfoliosPage />)
    await screen.findByLabelText('Ticker')
    const before = state.mock.calls.length

    await userEvent.type(screen.getByLabelText('Ticker'), 'msft')
    await userEvent.type(screen.getByLabelText('Quantity'), '10')
    await userEvent.type(screen.getByLabelText('Price'), '400')
    await userEvent.click(screen.getByRole('button', { name: /Record trade/ }))

    await waitFor(() =>
      expect(add).toHaveBeenCalledWith(
        'Growth',
        expect.objectContaining({ ticker: 'MSFT', action: 'BUY', quantity: 10, price: 400 }),
        { allow_overdraft: false },
      ),
    )
    // Cash and positions are computed from the trades, so the state query must
    // refetch too — invalidating only the trade list leaves the summary stale.
    await waitFor(() => expect(state.mock.calls.length).toBeGreaterThan(before))
  })

  it('offers the overdraft opt-in only when the API rejects for cash', async () => {
    vi.spyOn(api, 'addTrade').mockRejectedValue(
      Object.assign(new Error('x'), {
        detail: 'Insufficient cash: the trade costs 2,000,000.00 but only 33,199.00 is available',
      }),
    )
    renderPage(<PortfoliosPage />)

    await userEvent.type(await screen.findByLabelText('Ticker'), 'AAPL')
    await userEvent.type(screen.getByLabelText('Quantity'), '10000')
    await userEvent.type(screen.getByLabelText('Price'), '200')
    await userEvent.click(screen.getByRole('button', { name: /Record trade/ }))

    expect(await screen.findByText(/Insufficient cash/)).toBeInTheDocument()
    expect(screen.getByText(/Record it anyway/)).toBeInTheDocument()
  })

  it('does not offer the overdraft opt-in for an unrelated error', async () => {
    vi.spyOn(api, 'addTrade').mockRejectedValue(
      Object.assign(new Error('x'), { detail: "`action` must be one of ['BUY', 'SELL']" }),
    )
    renderPage(<PortfoliosPage />)

    await userEvent.type(await screen.findByLabelText('Ticker'), 'AAPL')
    await userEvent.type(screen.getByLabelText('Quantity'), '1')
    await userEvent.type(screen.getByLabelText('Price'), '1')
    await userEvent.click(screen.getByRole('button', { name: /Record trade/ }))

    expect(await screen.findByText(/must be one of/)).toBeInTheDocument()
    expect(screen.queryByText(/Record it anyway/)).not.toBeInTheDocument()
  })

  it('will not submit an incomplete trade', async () => {
    renderPage(<PortfoliosPage />)
    const button = await screen.findByRole('button', { name: /Record trade/ })
    expect(button).toBeDisabled()
  })

  it('deleting a trade refreshes the derived state', async () => {
    const state = vi.spyOn(api, 'getPortfolio')
    const remove = vi.spyOn(api, 'deleteTrade').mockResolvedValue(undefined)

    renderPage(<PortfoliosPage />)
    await screen.findByLabelText('Delete trade 1')
    const before = state.mock.calls.length

    await userEvent.click(screen.getByLabelText('Delete trade 1'))

    await waitFor(() =>
      expect(remove).toHaveBeenCalledWith('Growth', '1'),
    )
    await waitFor(() => expect(state.mock.calls.length).toBeGreaterThan(before))
  })
})

describe('PortfoliosPage — rebalancing', () => {
  it('parses weights and shows the preview without recording anything', async () => {
    const preview = vi.spyOn(api, 'previewRebalance').mockResolvedValue({
      name: 'Growth',
      total_equity: 38272.3,
      orders: [
        {
          ticker: 'AAPL',
          action: 'BUY',
          quantity: 100,
          price: 209.11,
          value: 20911,
          current_weight: 0.16,
          target_weight: 0.5,
        },
      ],
      unpriced: [],
    } as never)
    const add = vi.spyOn(api, 'addTrade')

    renderPage(<PortfoliosPage />)
    await userEvent.type(
      await screen.findByLabelText('Target weights'),
      'aapl: 0.5, msft: 0.3',
    )
    await userEvent.click(screen.getByRole('button', { name: /Preview orders/ }))

    await waitFor(() =>
      expect(preview).toHaveBeenCalledWith('Growth', {
        target_weights: { AAPL: 0.5, MSFT: 0.3 },
      }),
    )
    expect(await screen.findByText('16.0% → 50.0%')).toBeInTheDocument()
    // A PREVIEW. The Streamlit tool executed straight from the button.
    expect(add).not.toHaveBeenCalled()
  })

  it('reports tickers it could not price', async () => {
    vi.spyOn(api, 'previewRebalance').mockResolvedValue({
      name: 'Growth',
      total_equity: 1000,
      orders: [],
      unpriced: ['NOSUCH'],
    } as never)

    renderPage(<PortfoliosPage />)
    await userEvent.type(await screen.findByLabelText('Target weights'), 'NOSUCH: 1.0')
    await userEvent.click(screen.getByRole('button', { name: /Preview orders/ }))

    expect(await screen.findByText(/Skipped \(no price to size an order\)/)).toBeInTheDocument()
  })

  it('surfaces a rejected weight set', async () => {
    vi.spyOn(api, 'previewRebalance').mockRejectedValue(
      Object.assign(new Error('x'), {
        detail: 'Target weights sum to 1.4000; they must not exceed 1.0.',
      }),
    )
    renderPage(<PortfoliosPage />)
    await userEvent.type(await screen.findByLabelText('Target weights'), 'A: 0.7, B: 0.7')
    await userEvent.click(screen.getByRole('button', { name: /Preview orders/ }))

    expect(await screen.findByText(/must not exceed 1.0/)).toBeInTheDocument()
  })
})

describe('PortfoliosPage — portfolio CRUD', () => {
  it('creates a portfolio and selects it', async () => {
    const create = vi.spyOn(api, 'createPortfolio').mockResolvedValue({
      name: 'Income',
      initial_cash: 50000,
      created_at: null,
      metadata: null,
    } as never)

    renderPage(<PortfoliosPage />)
    await userEvent.type(await screen.findByLabelText('New portfolio'), 'Income')
    await userEvent.clear(screen.getByLabelText('Initial cash'))
    await userEvent.type(screen.getByLabelText('Initial cash'), '50000')
    await userEvent.click(screen.getByRole('button', { name: /Create/ }))

    await waitFor(() =>
      expect(create).toHaveBeenCalledWith({ name: 'Income', initial_cash: 50000 }),
    )
  })

  it('shows the conflict message for a duplicate name', async () => {
    vi.spyOn(api, 'createPortfolio').mockRejectedValue(
      Object.assign(new Error('x'), {
        detail: "A portfolio named 'Growth' already exists.",
      }),
    )
    renderPage(<PortfoliosPage />)
    await userEvent.type(await screen.findByLabelText('New portfolio'), 'Growth')
    await userEvent.click(screen.getByRole('button', { name: /Create/ }))

    expect(await screen.findByText(/already exists/)).toBeInTheDocument()
  })

  it('deleting the selected portfolio clears the detail pane', async () => {
    vi.spyOn(api, 'deletePortfolio').mockResolvedValue(undefined)
    vi.spyOn(api, 'listPortfolios')
      .mockResolvedValueOnce(SUMMARY as never)
      .mockResolvedValue([] as never)

    renderPage(<PortfoliosPage />)
    await screen.findByLabelText('Delete Growth')
    await userEvent.click(screen.getByLabelText('Delete Growth'))

    expect(
      await screen.findByText(/Select a portfolio, or create one/),
    ).toBeInTheDocument()
  })
})

describe('PortfoliosPage — flows, short terms and margin (decision 49)', () => {
  const TERMS = { borrow_rate: 0.01, max_gross: 2, maintenance: 0.3 }
  const WITH_FLOWS = {
    ...STATE,
    free_cash: 31999,
    long_value: 6273.3,
    short_value: 1200,
    gross_exposure: 7473.3,
    dividends_received: 7.25,
    dividends_paid: 1.1,
    interest: 412.5,
    borrow_paid: 3.2,
    flows: [{ day: '2026-08-10', kind: 'dividend', ticker: 'AAPL', amount: 2.7, quantity: 10 }],
    unhandled_actions: [],
    short_terms: TERMS,
    maintenance_required: 2241.99,
    margin_excess: 36030.31,
    below_maintenance: false,
  }

  it('shows free cash, the derived flows and the terms', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(WITH_FLOWS as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText('$31,999.00')).toBeInTheDocument()
    expect(screen.getByText('$412.50')).toBeInTheDocument()
    expect(screen.getByText('+$2.70')).toBeInTheDocument()
    expect(screen.getByText(/Borrow 1.00% a year/)).toBeInTheDocument()
    expect(screen.queryByText(/Below maintenance/)).toBeNull()
  })

  it('says a portfolio without terms cannot short', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue({ ...WITH_FLOWS, short_terms: null } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/this portfolio cannot short/)).toBeInTheDocument()
  })

  it('flags a portfolio below maintenance', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue({
      ...WITH_FLOWS,
      below_maintenance: true,
      margin_excess: -500,
    } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/Below maintenance/)).toBeInTheDocument()
  })

  it('sends the terms as fractions and refreshes the derived state', async () => {
    const set = vi.spyOn(api, 'setShortTerms').mockResolvedValue(TERMS as never)
    const get = vi.spyOn(api, 'getPortfolio').mockResolvedValue({
      ...WITH_FLOWS,
      short_terms: null,
    } as never)
    renderPage(<PortfoliosPage />)
    await userEvent.type(await screen.findByLabelText('Borrow (% a year)'), '1')
    await userEvent.type(screen.getByLabelText('Max gross (x equity)'), '2')
    await userEvent.type(screen.getByLabelText('Maintenance (% of gross)'), '30')
    const calls = get.mock.calls.length
    await userEvent.click(screen.getByRole('button', { name: 'Set terms' }))
    await waitFor(() => expect(set).toHaveBeenCalled())
    expect(set.mock.calls[0][0]).toBe('Growth')
    expect(set.mock.calls[0][1]).toEqual({ borrow_rate: 0.01, max_gross: 2, maintenance: 0.3 })
    await waitFor(() => expect(get.mock.calls.length).toBeGreaterThan(calls))
  })
})

describe('PortfoliosPage — an attached strategy (decision 50)', () => {
  const BINDING = {
    strategy: 'asset_class_trend',
    params: { window: 200, rebalance_frequency: 'ME' },
    symbols: ['SPY', 'EFA'],
    weights: { SPY: 0.5, EFA: 0.5 },
    transaction_cost: 0.001,
    attached_on: '2026-10-20',
    signal_from: '2010-01-04',
    last_session: '2026-10-20',
  }
  const EMPTY = { ...STATE, positions: [], trade_count: 0, strategy: null }
  const ATTACHED = { ...STATE, strategy: BINDING }
  const RUN = {
    name: 'Growth',
    strategy: 'asset_class_trend',
    written: false,
    newest_bar: '2026-10-21',
    last_session: '2026-10-21',
    sessions: [
      {
        session: '2026-10-21',
        signal_day: '2026-10-20',
        fills: [
          {
            ticker: 'SPY',
            action: 'BUY',
            quantity: 70,
            price: 701.5,
            costs: 50.1,
            time: '2026-10-21T13:30:00Z',
            notes: 'asset_class_trend: signal 1 on 2026-10-20',
          },
        ],
        refused: [],
      },
    ],
    held_sessions: [],
    missing_bars: {},
    holdings: [
      { ticker: 'SPY', wanted: 'long', held: 0, drift: true },
      { ticker: 'EFA', wanted: null, held: 0, drift: false },
    ],
    next_signals: {},
    ruined_on: null,
    flags: ['DRIFT Growth: SPY wanted long, held 0'],
  }

  const TRACKING = {
    name: 'Growth',
    strategy: 'asset_class_trend',
    attached_on: '2026-10-20',
    paper: [
      { time: '2026-10-20', total: 100000 },
      { time: '2026-11-02', total: 100512.25 },
    ],
    backtest: [
      { time: '2026-10-20', total: 100000 },
      { time: '2026-11-02', total: 100480.0 },
    ],
    gap: 32.25,
    backtest_slippage: 24.5,
    costs_difference: 0,
    other: 7.75,
    dividends_received: 0,
    paper_fills: [],
    backtest_fills: [],
    restated: [],
    flags: [],
  }

  beforeEach(() => {
    vi.spyOn(api, 'getTracking').mockResolvedValue(TRACKING as never)
    vi.spyOn(api, 'listStrategies').mockResolvedValue({
      strategies: [
        {
          id: 'asset_class_trend',
          display_name: 'Asset class trend',
          params: [{ name: 'window', label: 'Window', type: 'int', default: 200 }],
        },
      ],
    } as never)
  })

  it('attaches a strategy to an empty portfolio with its typed params and cost', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(EMPTY as never)
    const attach = vi.spyOn(api, 'attachStrategy').mockResolvedValue(BINDING as never)
    renderPage(<PortfoliosPage />)
    const window = await screen.findByLabelText('Window')
    await userEvent.clear(window)
    await userEvent.type(window, '150')
    await userEvent.clear(screen.getByLabelText('Symbols'))
    await userEvent.type(screen.getByLabelText('Symbols'), 'spy, efa')
    await userEvent.click(screen.getByRole('button', { name: 'Attach' }))
    await waitFor(() => expect(attach).toHaveBeenCalled())
    expect(attach.mock.calls[0]).toEqual([
      'Growth',
      {
        strategy_id: 'asset_class_trend',
        symbols: ['SPY', 'EFA'],
        params: { window: 150 },
        transaction_cost: 0.001,
      },
    ])
  })

  it('offers no attach on a portfolio with trades', async () => {
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText('Record a trade')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Attach' })).toBeNull()
  })

  it('shows the dry run, the drift flag, and hides manual trading', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(ATTACHED as never)
    const run = vi.spyOn(api, 'runStrategy').mockResolvedValue(RUN as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/2026-10-21: BUY 70 SPY at 701.50/)).toBeInTheDocument()
    expect(screen.getByText('DRIFT Growth: SPY wanted long, held 0')).toBeInTheDocument()
    expect(screen.getByText('drift')).toBeInTheDocument()
    expect(run.mock.calls[0]).toEqual(['Growth'])
    expect(screen.queryByText('Record a trade')).toBeNull()
    expect(screen.queryByRole('button', { name: /Delete trade/ })).toBeNull()
  })

  it('trading now writes and refreshes the derived state', async () => {
    const get = vi.spyOn(api, 'getPortfolio').mockResolvedValue(ATTACHED as never)
    const run = vi.spyOn(api, 'runStrategy').mockResolvedValue(RUN as never)
    renderPage(<PortfoliosPage />)
    const button = await screen.findByRole('button', { name: 'Trade now' })
    await waitFor(() => expect(button).toBeEnabled())
    const calls = get.mock.calls.length
    await userEvent.click(button)
    await waitFor(() => expect(run).toHaveBeenCalledWith('Growth', true))
    // The state and the dry run are both re-read after the write.
    await waitFor(() => expect(get.mock.calls.length).toBeGreaterThan(calls))
    await waitFor(() => expect(run.mock.calls.filter((c) => c.length === 1).length).toBe(2))
  })

  it('shows the paper record beside its backtest, with the gap split', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(ATTACHED as never)
    vi.spyOn(api, 'runStrategy').mockResolvedValue({ ...RUN, flags: [] } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText('+$32.25')).toBeInTheDocument()
    expect(screen.getByText('+$24.50')).toBeInTheDocument()
    expect(screen.getByText('+$7.75')).toBeInTheDocument()
    const { series } = chartProps.mock.calls.at(-1)![0] as {
      series: { name: string; points: unknown[] }[]
    }
    expect(series.map((x) => x.name)).toEqual(['Paper', 'Backtest from the attach date'])
    expect(series[0].points).toEqual(TRACKING.paper)
    expect(series[1].points).toEqual(TRACKING.backtest)
    expect(screen.queryByText(/history it traded on has moved/)).toBeNull()
  })

  it('raises a restated decision', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(ATTACHED as never)
    vi.spyOn(api, 'runStrategy').mockResolvedValue({ ...RUN, flags: [] } as never)
    const flag = 'RESTATED Growth: the decision of 2026-10-30 was {SPY: 1}; today gives nothing'
    vi.spyOn(api, 'getTracking').mockResolvedValue({
      ...TRACKING,
      restated: [{ day: '2026-10-30', then: { SPY: 1 }, now: {} }],
      flags: [flag],
    } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(flag)).toBeInTheDocument()
    expect(screen.getByText(/history it traded on has moved/)).toBeInTheDocument()
  })

  it('says there is nothing to compare before the first close', async () => {
    vi.spyOn(api, 'getPortfolio').mockResolvedValue(ATTACHED as never)
    vi.spyOn(api, 'runStrategy').mockResolvedValue({ ...RUN, flags: [] } as never)
    vi.spyOn(api, 'getTracking').mockResolvedValue({
      ...TRACKING,
      paper: [],
      backtest: [],
      gap: null,
      other: null,
    } as never)
    renderPage(<PortfoliosPage />)
    expect(await screen.findByText(/nothing to compare/)).toBeInTheDocument()
    expect(screen.queryByTestId('comparison-chart')).toBeNull()
  })
})
