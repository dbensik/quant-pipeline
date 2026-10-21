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
import { REPORT } from '@/components/backtest/benchmarkReportFixture'
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
  count: 4,
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
      id: 'pairs_trading',
      display_name: 'Pairs Trading',
      description: '',
      input_contract: 'multi',
      caveat: null,
      held_weight: 'equal',
      shorts: true,
      params: [param('entry', 'float', 2, 'Entry z')],
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

  it('reports the rule against SPY, with the basket beside it', async () => {
    vi.spyOn(api, 'runPortfolioBacktest').mockImplementation(async (body) =>
      (body.strategy_id === 'index_rebalancing'
        ? {
            ...response('index_rebalancing', 'Index Rebalancing', 0.46),
            benchmark_report: { ...REPORT, beta: 0.593, alpha: -0.0197, alpha_t: -1.46 },
          }
        : {
            ...response(body.strategy_id, 'Asset Class Trend Following', 0.45),
            benchmark_report: REPORT,
          }) as never,
    )
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    expect(await screen.findByText(/^Beta 0\.30 to SPY/)).toBeInTheDocument()
    expect(
      screen.getByText(/The same basket equal-weighted: Beta 0\.59 to SPY\. Alpha -2\.0% a year/),
    ).toBeInTheDocument()
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

describe('AllocationPage — a strategy that shorts (decision 48)', () => {
  const SHORTS = {
    short_terms: { borrow_rate: 0.01, max_gross: 2, maintenance: 0.3, source: 'request' },
    short_exposure: {
      borrow_paid: 1234.4,
      max_gross_exposure: 1.02,
      max_net_exposure: 0.05,
      time_short: 0.25,
      margin_cuts: ['2020-03-17T00:00:00Z'],
      ruined_at: null,
    },
  }

  function mockPairs() {
    return vi.spyOn(api, 'runPortfolioBacktest').mockImplementation(async (body) =>
      (body.strategy_id === 'index_rebalancing'
        ? response('index_rebalancing', 'Index Rebalancing', 0.46)
        : {
            ...response('pairs_trading', 'Pairs Trading', 0.3),
            weights: {},
            ...SHORTS,
          }) as never,
    )
  }

  it('asks for borrow and margin terms only for a strategy that shorts', async () => {
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    expect(screen.queryByLabelText('Borrow (% a year)')).toBeNull()
    await userEvent.selectOptions(screen.getByLabelText('Strategy'), 'pairs_trading')
    expect(screen.getByLabelText('Borrow (% a year)')).toBeInTheDocument()
    expect(screen.getByLabelText('Max gross (x equity)')).toBeInTheDocument()
    expect(screen.getByLabelText('Maintenance (% of gross)')).toBeInTheDocument()
  })

  it('sends typed terms as fractions, leaves blanks to the server, and none to the benchmark', async () => {
    const runs = mockPairs()
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.selectOptions(screen.getByLabelText('Strategy'), 'pairs_trading')
    await userEvent.type(screen.getByLabelText('Borrow (% a year)'), '1.5')
    await userEvent.type(screen.getByLabelText('Maintenance (% of gross)'), '30')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    await waitFor(() => expect(runs).toHaveBeenCalledTimes(2))
    const pairs = runs.mock.calls.find((c) => c[0].strategy_id === 'pairs_trading')![0]
    expect(pairs.borrow_rate).toBeCloseTo(0.015)
    expect(pairs.maintenance).toBeCloseTo(0.3)
    expect('max_gross' in pairs).toBe(false)
    const bench = runs.mock.calls.find((c) => c[0].strategy_id === 'index_rebalancing')![0]
    expect(bench.borrow_rate).toBeUndefined()
    expect(bench.maintenance).toBeUndefined()
  })

  it('reports what the shorts did and the terms they were charged on', async () => {
    mockPairs()
    renderPage(<AllocationPage />)
    await screen.findByLabelText('Moving average window')
    await userEvent.selectOptions(screen.getByLabelText('Strategy'), 'pairs_trading')
    await userEvent.click(screen.getByRole('button', { name: 'Run backtest' }))

    expect(await screen.findByText('$1,234')).toBeInTheDocument()
    expect(screen.getByText('25.00%')).toBeInTheDocument()
    expect(screen.getByText('1.020x equity')).toBeInTheDocument()
    expect(screen.getByText(/Borrow 1.00% a year, gross capped at 2x/)).toBeInTheDocument()
    expect(screen.getByText('sized by the strategy')).toBeInTheDocument()
  })
})
