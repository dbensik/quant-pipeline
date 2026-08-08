import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { BacktestResult } from '@/api/backtestResult'
import { BacktestResults } from './BacktestResults'

// Recharts measures its container with getBoundingClientRect, which is 0x0 in
// jsdom — the real chart renders nothing, so asserting on it would be vacuous.
// Rendering the props as attributes instead tests the wiring that actually
// broke: initial capital reaching the break-even line.
vi.mock('@/components/charts/EquityCurveChart', () => ({
  EquityCurveChart: ({
    initialCapital,
    points,
  }: {
    initialCapital: number
    points: Array<{ time: string; total: number }>
  }) => (
    <div
      data-testid="equity-curve"
      data-initial-capital={initialCapital}
      data-points={points.length}
    />
  ),
}))

const RESULT: BacktestResult = {
  symbol: 'AAPL',
  strategyId: 'ma_crossover',
  strategyName: 'Moving Average Crossover',
  bars: 251,
  tradeCount: 7,
  params: { short_window: 10, long_window: 30 },
  initialCapital: 250_000,
  seed: 42,
  metrics: {
    'Final Value': 123_456.78,
    'Total Return': 0.2345,
    'Sharpe Ratio': 1.234,
    'Sortino Ratio': null,
    'Trade Count': 7,
  },
  caveat: null,
  equityCurve: [
    { time: '2024-01-01T00:00:00Z', total: 250_000 },
    { time: '2024-01-02T00:00:00Z', total: 260_000 },
  ],
  via: 'websocket',
}

describe('BacktestResults — break-even line', () => {
  it('passes the run\'s own initial capital to the chart', () => {
    // THE regression. This read `result.params.initial_capital`, which is
    // always undefined — params carries STRATEGY parameters only — so the
    // break-even line silently fell back to a hardcoded 100,000 and sat in the
    // wrong place for any run with different capital.
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByTestId('equity-curve')).toHaveAttribute(
      'data-initial-capital',
      '250000',
    )
  })

  it('passes the equity curve through', () => {
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByTestId('equity-curve')).toHaveAttribute('data-points', '2')
  })
})

describe('BacktestResults — summary badges', () => {
  it('shows the symbol, strategy, bar count and trade count', () => {
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByText('AAPL')).toBeInTheDocument()
    expect(screen.getByText('Moving Average Crossover')).toBeInTheDocument()
    expect(screen.getByText('251 bars')).toBeInTheDocument()
    expect(screen.getByText('7 trades')).toBeInTheDocument()
  })

  it('shows the seed, because it is what makes the run reproducible', () => {
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByText('seed 42')).toBeInTheDocument()
  })

  it('omits the seed badge for an unseeded run', () => {
    render(<BacktestResults result={{ ...RESULT, seed: null }} />)
    expect(screen.queryByText(/^seed/)).not.toBeInTheDocument()
  })
})

describe('BacktestResults — metric formatting', () => {
  it('formats returns as percentages', () => {
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByText('23.45%')).toBeInTheDocument()
  })

  it('formats currency metrics as currency', () => {
    render(<BacktestResults result={RESULT} />)
    expect(screen.getByText('$123,456.78')).toBeInTheDocument()
  })

  it('renders a null metric as an em dash, not zero', () => {
    // Null is a real value: the API converts NaN/Infinity to null (neither is
    // valid JSON), which is what Sortino over a zero-variance curve produces.
    // Showing 0.000 would present "undefined" as a real number.
    render(<BacktestResults result={RESULT} />)
    const sortino = screen.getByText('Sortino Ratio').parentElement
    expect(sortino).toHaveTextContent('—')
    expect(sortino).not.toHaveTextContent('0.000')
  })

  it('shows integer metrics without decimals', () => {
    render(<BacktestResults result={RESULT} />)
    const tradeCount = screen.getByText('Trade Count').parentElement
    expect(tradeCount).toHaveTextContent('7')
  })

  it('lists the parameters that produced the result', () => {
    render(<BacktestResults result={RESULT} />)
    expect(
      screen.getByText(/short_window=10, long_window=30/),
    ).toBeInTheDocument()
  })
})
