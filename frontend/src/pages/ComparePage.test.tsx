/**
 * ComparePage.
 *
 * The chart is mocked — Recharts renders nothing under jsdom, so asserting on
 * chart output is vacuous. Its props are asserted instead, and the row-merging
 * logic has its own test in comparisonRows.test.ts.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { ComparePage } from './ComparePage'

const chartProps = vi.fn()
vi.mock('@/components/charts/ComparisonChart', () => ({
  ComparisonChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="comparison-chart" />
  },
}))

const CATALOGUE = {
  count: 3,
  strategies: [
    {
      id: 'ma_crossover',
      display_name: 'Moving Average Crossover',
      description: '',
      input_contract: 'single',
      caveat: null,
      params: [],
      default_grid: { short_window: [10, 20], long_window: [50, 100] },
    },
    {
      id: 'mean_reversion',
      display_name: 'Mean Reversion',
      description: '',
      input_contract: 'single',
      caveat: null,
      params: [],
      default_grid: { window: [10, 20] },
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

const RESULT = {
  symbol: 'AAPL',
  start: '2024-01-01',
  end: '2024-12-31',
  bars: 250,
  metric: 'Sharpe Ratio',
  initial_capital: 100000,
  seed: 42,
  optimized: false,
  results: [
    {
      strategy_id: 'ma_crossover',
      strategy_name: 'Moving Average Crossover',
      params: { short_window: 20, long_window: 50 },
      tuned: false,
      combinations_evaluated: 0,
      metrics: {
        'Sharpe Ratio': 1.103,
        'Total Return': 0.0301,
        'Max Drawdown': -0.05,
        'Trade Count': 14,
      },
      caveat: null,
      equity_curve: [{ time: '2024-01-01T00:00:00Z', total: 100000 }],
    },
  ],
  benchmark: null,
  skipped: [],
}

beforeEach(() => {
  chartProps.mockClear()
  vi.spyOn(api, 'listStrategies').mockResolvedValue(CATALOGUE as never)
})

afterEach(() => vi.restoreAllMocks())

describe('ComparePage — setup', () => {
  it('offers the single-asset strategies', async () => {
    renderPage(<ComparePage />)
    expect(
      await screen.findByRole('button', { name: /Moving Average Crossover/ }),
    ).toBeInTheDocument()
  })

  it('marks which strategies the registry can auto-tune', async () => {
    renderPage(<ComparePage />)
    // ma_crossover has a default_grid; trend_following does not.
    const tunable = await screen.findByRole('button', {
      name: /Moving Average Crossover ⚙/,
    })
    expect(tunable).toBeInTheDocument()
    expect(
      screen.getByRole('button', { name: 'Trend Following' }),
    ).toBeInTheDocument()
  })

  it('sends the selected strategies and window', async () => {
    const compare = vi
      .spyOn(api, 'compareStrategies')
      .mockResolvedValue(RESULT as never)

    renderPage(<ComparePage />)
    await screen.findByRole('button', { name: /Moving Average Crossover/ })
    await userEvent.click(screen.getByRole('button', { name: /^Compare/ }))

    // The FIRST argument only: TanStack Query hands mutationFn a second
    // (context) parameter, so toHaveBeenCalledWith on the body alone fails.
    await waitFor(() => expect(compare).toHaveBeenCalled())
    expect(compare.mock.calls[0][0]).toMatchObject({
      symbol: 'AAPL',
      strategies: [
        { strategy_id: 'ma_crossover' },
        { strategy_id: 'mean_reversion' },
      ],
      optimize: false,
      metric: 'Sharpe Ratio',
    })
  })

  it('passes a benchmark only when one is entered', async () => {
    const compare = vi
      .spyOn(api, 'compareStrategies')
      .mockResolvedValue(RESULT as never)

    renderPage(<ComparePage />)
    await userEvent.type(
      await screen.findByLabelText('Benchmark (optional)'),
      'msft',
    )
    await userEvent.click(screen.getByRole('button', { name: /^Compare/ }))

    await waitFor(() => expect(compare).toHaveBeenCalled())
    expect(compare.mock.calls[0][0]).toMatchObject({ benchmark_symbol: 'MSFT' })
  })

  it('sends null for no benchmark rather than an empty string', async () => {
    const compare = vi
      .spyOn(api, 'compareStrategies')
      .mockResolvedValue(RESULT as never)

    renderPage(<ComparePage />)
    await screen.findByRole('button', { name: /^Compare/ })
    await userEvent.click(screen.getByRole('button', { name: /^Compare/ }))

    await waitFor(() => expect(compare).toHaveBeenCalled())
    expect(compare.mock.calls[0][0]).toMatchObject({ benchmark_symbol: null })
  })

  it('will not run with no strategies selected', async () => {
    renderPage(<ComparePage />)
    // Both defaults deselected, so nothing is left to compare.
    await userEvent.click(
      await screen.findByRole('button', { name: /Moving Average Crossover/ }),
    )
    await userEvent.click(screen.getByRole('button', { name: /Mean Reversion/ }))

    expect(screen.getByRole('button', { name: /^Compare 0/ })).toBeDisabled()
  })
})

describe('ComparePage — results', () => {
  it('renders the ranking', async () => {
    vi.spyOn(api, 'compareStrategies').mockResolvedValue(RESULT as never)
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    expect(await screen.findByText('1.103')).toBeInTheDocument()
    expect(screen.getByText('3.01%')).toBeInTheDocument()
    expect(screen.getByText('short_window=20, long_window=50')).toBeInTheDocument()
  })

  it('badges a tuned row with how many combinations it searched', async () => {
    vi.spyOn(api, 'compareStrategies').mockResolvedValue({
      ...RESULT,
      optimized: true,
      results: [{ ...RESULT.results[0], tuned: true, combinations_evaluated: 9 }],
    } as never)
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    expect(await screen.findByText('tuned (9)')).toBeInTheDocument()
  })

  it('reports strategies that could not be compared', async () => {
    vi.spyOn(api, 'compareStrategies').mockResolvedValue({
      ...RESULT,
      skipped: [
        { strategy_id: 'pairs_trading', reason: "'pairs_trading' is multi-asset" },
      ],
    } as never)
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    // Reported rather than dropped — a comparison missing an entry says why.
    expect(await screen.findByText(/Not compared:/)).toBeInTheDocument()
    expect(screen.getByText(/multi-asset/)).toBeInTheDocument()
  })

  it('shows the benchmark as a separate buy-and-hold row', async () => {
    vi.spyOn(api, 'compareStrategies').mockResolvedValue({
      ...RESULT,
      benchmark: {
        symbol: 'MSFT',
        metrics: { 'Sharpe Ratio': 0.9, 'Total Return': 1.09, 'Max Drawdown': -0.2 },
        equity_curve: [{ time: '2024-01-01T00:00:00Z', total: 100000 }],
      },
    } as never)
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    expect(await screen.findByText(/Benchmark \(MSFT\)/)).toBeInTheDocument()
    expect(screen.getByText('buy & hold')).toBeInTheDocument()
  })

  it('passes one series per strategy plus the benchmark to the chart', async () => {
    vi.spyOn(api, 'compareStrategies').mockResolvedValue({
      ...RESULT,
      benchmark: {
        symbol: 'MSFT',
        metrics: {},
        equity_curve: [{ time: '2024-01-01T00:00:00Z', total: 100000 }],
      },
    } as never)
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    await screen.findByTestId('comparison-chart')
    const props = chartProps.mock.calls.at(-1)?.[0] as { series: { name: string }[] }
    expect(props.series.map((s) => s.name)).toEqual([
      'Moving Average Crossover',
      'Benchmark (MSFT)',
    ])
  })

  it('surfaces an API rejection', async () => {
    vi.spyOn(api, 'compareStrategies').mockRejectedValue(
      new ApiError(422, 'Duplicate strategies: [ma_crossover].'),
    )
    renderPage(<ComparePage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Compare/ }))

    expect(await screen.findByText(/Duplicate strategies/)).toBeInTheDocument()
  })
})
