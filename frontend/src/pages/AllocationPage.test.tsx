/**
 * AllocationPage (decision 32).
 *
 * The chart is mocked — Recharts renders nothing under jsdom — and its props
 * are asserted instead.
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { PRESETS, parseSymbols } from '@/lib/allocation'
import { AllocationPage } from './AllocationPage'

const chartProps = vi.fn()
vi.mock('@/components/charts/ComparisonChart', () => ({
  ComparisonChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="comparison-chart" />
  },
}))

const param = (name: string, type: string, def: unknown, label: string) => ({
  name,
  type,
  default: def,
  label,
  description: '',
  minimum: null,
  maximum: null,
})

const CATALOGUE = {
  count: 3,
  strategies: [
    {
      id: 'asset_class_trend',
      display_name: 'Asset Class Trend Following',
      description: 'Hold each sleeve above its average.',
      input_contract: 'multi',
      caveat: 'Measured at its defaults on fixed ETF baskets: halved the drawdown.',
      held_weight: 'equal',
      params: [
        param('window', 'int', 200, 'Moving average window'),
        param('rebalance_frequency', 'str', 'ME', 'Rebalance frequency'),
      ],
      default_grid: null,
    },
    {
      id: 'momentum_allocation',
      display_name: 'Momentum Asset Allocation',
      description: '',
      input_contract: 'multi',
      caveat: null,
      held_weight: 'per_top_n',
      params: [param('lookback', 'int', 126, 'Lookback'), param('top_n', 'int', 2, 'Top N')],
      default_grid: null,
    },
    {
      id: 'index_rebalancing',
      display_name: 'Index Rebalancing',
      description: '',
      input_contract: 'multi',
      caveat: null,
      held_weight: 'equal',
      params: [],
      default_grid: null,
    },
  ],
}

function response(strategy_id: string, strategy_name: string, sharpe: number) {
  return {
    symbols: PRESETS[0].symbols,
    strategy_id,
    strategy_name,
    start: '2011-01-01',
    end: '2026-10-14',
    bars: 3970,
    params: {},
    weights: Object.fromEntries(PRESETS[0].symbols.map((s) => [s, 0.2])),
    initial_capital: 100000,
    transaction_cost: 0.001,
    seed: 42,
    risk_free_rate: 0.0123,
    risk_free_source: '^IRX',
    max_trade_risk_pct: 1,
    max_portfolio_drawdown_pct: 1,
    metrics: {
      'Sharpe Ratio': sharpe,
      'Annualized Return': 0.052,
      'Max Drawdown': -0.153,
      'Trade Count': 155,
    },
    risk_metrics: {},
    caveat: null,
    equity_curve: [
      { time: '2011-01-03T00:00:00Z', total: 100000 },
      { time: '2026-10-14T00:00:00Z', total: 222000 },
    ],
  }
}

function mockRuns() {
  return vi.spyOn(api, 'runPortfolioBacktest').mockImplementation(async (body) =>
    (body.strategy_id === 'index_rebalancing'
      ? response('index_rebalancing', 'Index Rebalancing', 0.46)
      : response(body.strategy_id, 'Asset Class Trend Following', 0.45)) as never,
  )
}

beforeEach(() => {
  chartProps.mockClear()
  vi.spyOn(api, 'listStrategies').mockResolvedValue(CATALOGUE as never)
})

afterEach(() => vi.restoreAllMocks())

describe('parseSymbols', () => {
  it('splits on commas and spaces, upper-cases and de-duplicates', () => {
    expect(parseSymbols(' spy, efa  ief,SPY ')).toEqual(['SPY', 'EFA', 'IEF'])
  })
})

describe('AllocationPage — setup', () => {
  it('offers the allocation rules but not the benchmark itself', async () => {
    renderPage(<AllocationPage />)
    expect(
      await screen.findByRole('option', { name: 'Asset Class Trend Following' }),
    ).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'Index Rebalancing' })).toBeNull()
  })

  it('shows the selected strategy’s caveat before it is run', async () => {
    renderPage(<AllocationPage />)
    expect(await screen.findByText(/halved the drawdown/)).toBeInTheDocument()
  })

  it('a preset fills the basket', async () => {
    renderPage(<AllocationPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'Faber 7' }))
    expect(screen.getByLabelText('Basket')).toHaveValue(PRESETS[1].symbols.join(', '))
  })

  it('picking another strategy starts from its own defaults', async () => {
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.selectOptions(screen.getByLabelText('Strategy'), 'momentum_allocation')
    expect(screen.getByLabelText('Top N')).toHaveValue(2)
    expect(screen.queryByLabelText('Moving average window')).toBeNull()
  })
})

describe('AllocationPage — running', () => {
  it('sends the basket, strategy and typed params, and leaves weights to the server', async () => {
    const runs = mockRuns()
    renderPage(<AllocationPage />)
    const window = await screen.findByLabelText('Moving average window')
    await userEvent.clear(window)
    await userEvent.type(window, '100')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    await waitFor(() => expect(runs).toHaveBeenCalledTimes(2))
    const strategyCall = runs.mock.calls.find((c) => c[0].strategy_id === 'asset_class_trend')!
    expect(strategyCall[0]).toMatchObject({
      symbols: PRESETS[0].symbols,
      params: { window: 100, rebalance_frequency: 'ME' },
    })
    // Weights come from the strategy's own default (held_weight), server side.
    expect(strategyCall[0].weights).toBeUndefined()
  })

  it('defaults to a long window of its own, not the shared one-year range', async () => {
    const runs = mockRuns()
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    expect(screen.getByLabelText('From')).toHaveValue('2011-01-01')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))
    await waitFor(() => expect(runs).toHaveBeenCalledTimes(2))
    expect(runs.mock.calls.every((c) => c[0].start === '2011-01-01')).toBe(true)
  })

  it('runs the same basket equal-weighted as the benchmark', async () => {
    const runs = mockRuns()
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    await waitFor(() => expect(runs).toHaveBeenCalledTimes(2))
    const bench = runs.mock.calls.find((c) => c[0].strategy_id === 'index_rebalancing')!
    expect(bench[0]).toMatchObject({ symbols: PRESETS[0].symbols, params: {}, weights: null })
  })

  it('shows both runs side by side and charts both curves', async () => {
    mockRuns()
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    expect(await screen.findByText('0.450')).toBeInTheDocument()
    expect(screen.getByText('0.460')).toBeInTheDocument()
    const last = chartProps.mock.calls.at(-1)![0] as { series: { name: string }[] }
    expect(last.series.map((s) => s.name)).toEqual([
      'Asset Class Trend Following',
      'Same basket, equal weight, rebalanced monthly',
    ])
  })

  it('cannot run on fewer than two symbols', async () => {
    renderPage(<AllocationPage />)
    const basket = await screen.findByLabelText('Basket')
    await userEvent.clear(basket)
    await userEvent.type(basket, 'SPY')
    expect(screen.getByRole('button', { name: 'Run backtest' })).toBeDisabled()
  })

  it('surfaces a refusal from the server', async () => {
    vi.spyOn(api, 'runPortfolioBacktest').mockRejectedValue(
      new ApiError(422, "PairedSwitchingStrategy trades exactly two symbols; got 5."),
    )
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))
    expect(await screen.findByText(/exactly two symbols/)).toBeInTheDocument()
  })
})
