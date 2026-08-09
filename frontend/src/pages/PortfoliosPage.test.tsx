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
