/**
 * OptimizePage — grid search and Monte Carlo weights.
 *
 * Phase 5 — React pages for the ported routers
 */

import { fireEvent, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { OptimizePage } from './OptimizePage'

const CATALOGUE = {
  count: 2,
  strategies: [
    {
      id: 'ma_crossover',
      display_name: 'Moving Average Crossover',
      description: '',
      input_contract: 'single',
      caveat: null,
      params: [],
      default_grid: { short_window: [10, 20, 30], long_window: [50, 100, 200] },
    },
    {
      id: 'trend_following',
      display_name: 'Trend Following',
      description: '',
      input_contract: 'single',
      caveat: null,
      params: [],
      default_grid: null,
    },
  ],
}

const GRID_RESULT = {
  symbol: 'AAPL',
  strategy_id: 'ma_crossover',
  strategy_name: 'Moving Average Crossover',
  start: '2024-01-01',
  end: '2024-12-31',
  bars: 250,
  metric: 'Sharpe Ratio',
  seed: 42,
  initial_capital: 100000,
  combinations_requested: 9,
  combinations_evaluated: 7,
  best_params: { short_window: 20, long_window: 50 },
  best_metrics: { 'Sharpe Ratio': 1.5 },
  results: [
    {
      short_window: 20,
      long_window: 50,
      'Sharpe Ratio': 1.5,
      'Total Return': 0.22,
      'Trade Count': 12,
    },
  ],
  skipped: [],
  caveat: null,
}

const WEIGHTS = {
  symbols: ['AAPL', 'MSFT'],
  start: '2024-01-01',
  end: '2024-12-31',
  bars: 250,
  num_portfolios: 4000,
  risk_free_rate: 0.02,
  seed: 42,
  max_sharpe: {
    annualized_return: 0.343,
    annualized_volatility: 0.188,
    sharpe_ratio: 1.714,
    weights: { AAPL: 0.5, MSFT: 0.5 },
  },
  min_volatility: {
    annualized_return: 0.236,
    annualized_volatility: 0.161,
    sharpe_ratio: 1.347,
    weights: { AAPL: 0.3, MSFT: 0.7 },
  },
  frontier: [],
}

beforeEach(() => {
  vi.spyOn(api, 'listStrategies').mockResolvedValue(CATALOGUE as never)
})

afterEach(() => vi.restoreAllMocks())

describe('OptimizePage — grid search', () => {
  it('uses the registry default grid when none is typed', async () => {
    const run = vi
      .spyOn(api, 'optimizeStrategy')
      .mockResolvedValue(GRID_RESULT as never)

    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Run grid search/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]).toMatchObject({
      symbol: 'AAPL',
      strategy_id: 'ma_crossover',
      grid: { short_window: [10, 20, 30], long_window: [50, 100, 200] },
    })
  })

  it('prefers a typed grid over the default', async () => {
    const run = vi
      .spyOn(api, 'optimizeStrategy')
      .mockResolvedValue(GRID_RESULT as never)

    renderPage(<OptimizePage />)
    // fireEvent.change, not userEvent.type: userEvent reads `{` and `[` as key
    // descriptors, so typing JSON silently produces something else.
    fireEvent.change(await screen.findByLabelText('Grid'), {
      target: { value: '{"window": [5]}' },
    })
    await userEvent.click(screen.getByRole('button', { name: /Run grid search/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0].grid).toEqual({ window: [5] })
  })

  it('blocks a run on malformed grid JSON rather than sending it', async () => {
    const run = vi.spyOn(api, 'optimizeStrategy')
    renderPage(<OptimizePage />)

    fireEvent.change(await screen.findByLabelText('Grid'), {
      target: { value: 'not json' },
    })

    expect(screen.getByText('Not valid JSON.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Run grid search/ })).toBeDisabled()
    expect(run).not.toHaveBeenCalled()
  })

  it('cannot run a strategy that has no default sweep and no typed grid', async () => {
    renderPage(<OptimizePage />)
    // Wait for the catalogue: the <select> exists before its options do, so
    // findByLabelText alone resolves against an empty list.
    await screen.findByRole('option', { name: 'Trend Following' })
    await userEvent.selectOptions(
      screen.getByLabelText('Strategy'),
      'trend_following',
    )
    expect(screen.getByText(/no default sweep/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Run grid search/ })).toBeDisabled()
  })

  it('warns that a risk metric has a degenerate optimum', async () => {
    // Measured server-side: the least volatile parameter set had 0 trades,
    // 0.0 volatility and 0.0 return.
    renderPage(<OptimizePage />)
    await userEvent.selectOptions(
      await screen.findByLabelText('Optimize for'),
      'Annualized Volatility',
    )
    expect(screen.getByText(/degenerate optimum/)).toBeInTheDocument()
  })

  it('does not warn for a risk-adjusted metric', async () => {
    renderPage(<OptimizePage />)
    await screen.findByLabelText('Optimize for')
    expect(screen.queryByText(/degenerate optimum/)).not.toBeInTheDocument()
  })

  it('shows the winner and how much of the grid was searched', async () => {
    vi.spyOn(api, 'optimizeStrategy').mockResolvedValue(GRID_RESULT as never)
    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Run grid search/ }))

    expect(
      await screen.findByText(/best: short_window=20, long_window=50/),
    ).toBeInTheDocument()
    expect(screen.getByText(/7 of 9 combinations evaluated/)).toBeInTheDocument()
  })

  it('reports combinations the strategy rejected', async () => {
    vi.spyOn(api, 'optimizeStrategy').mockResolvedValue({
      ...GRID_RESULT,
      skipped: [
        {
          params: { short_window: 30, long_window: 30 },
          reason: 'The short window must be smaller than the long window.',
        },
      ],
    } as never)
    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Run grid search/ }))

    expect(
      await screen.findByText(/rejected by the strategy/),
    ).toBeInTheDocument()
  })

  it('surfaces an API rejection', async () => {
    vi.spyOn(api, 'optimizeStrategy').mockRejectedValue(
      new ApiError(422, 'Grid has 10000 combinations; the limit is 1000.'),
    )
    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Run grid search/ }))

    expect(await screen.findByText(/the limit is 1000/)).toBeInTheDocument()
  })
})

describe('OptimizePage — portfolio weights', () => {
  it('parses the universe into a symbol list', async () => {
    const run = vi
      .spyOn(api, 'optimizePortfolio')
      .mockResolvedValue(WEIGHTS as never)

    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Find weights/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]).toMatchObject({
      symbols: ['AAPL', 'MSFT', 'JPM', 'XOM'],
      num_portfolios: 4000,
    })
  })

  it('shows both allocations with their weights', async () => {
    vi.spyOn(api, 'optimizePortfolio').mockResolvedValue(WEIGHTS as never)
    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Find weights/ }))

    expect(await screen.findByText('Max Sharpe')).toBeInTheDocument()
    expect(screen.getByText('Min volatility')).toBeInTheDocument()
    expect(screen.getByText('70.0%')).toBeInTheDocument()
  })

  it('surfaces a rejected universe', async () => {
    vi.spyOn(api, 'optimizePortfolio').mockRejectedValue(
      new ApiError(422, 'Weight optimization needs at least 2 symbols; 1 given.'),
    )
    renderPage(<OptimizePage />)
    await userEvent.click(await screen.findByRole('button', { name: /Find weights/ }))

    expect(await screen.findByText(/at least 2 symbols/)).toBeInTheDocument()
  })
})
