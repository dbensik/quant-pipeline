import { render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { SimulationResponse } from '@/api/client'
import { SimulationResults } from './SimulationResults'

// Recharts renders nothing in jsdom (0x0 container). The chart is mocked and
// its PROPS asserted — the alignment of the overlay is what can go wrong.
vi.mock('@/components/charts/FanChart', () => ({
  FanChart: ({ rows, initialCapital }: { rows: Array<{ historical?: number }>; initialCapital: number }) => (
    <div
      data-testid="fan-chart"
      data-initial-capital={initialCapital}
      data-rows={rows.length}
      data-historical={rows.map((row) => row.historical ?? 'x').join(',')}
    />
  ),
}))

function summary(mid: number, spread: number) {
  return { p05: mid - 2 * spread, p25: mid - spread, p50: mid, p75: mid + spread, p95: mid + 2 * spread, mean: mid }
}

const RESULT: SimulationResponse = {
  symbol: 'AAPL',
  strategy_id: 'buy_and_hold',
  strategy_name: 'Buy and Hold',
  start: '2024-01-01T00:00:00Z',
  end: '2024-12-31T00:00:00Z',
  bars: 4,
  params: {},
  initial_capital: 250_000,
  seed: 42,
  mode: 'returns',
  method: 'stationary',
  n_paths: 2000,
  horizon_days: 3,
  block_length_days: 20,
  resampled_from: 4,
  historical: { 'Max Drawdown': -0.12 },
  bands: [0, 1, 2, 3].map((step) => ({
    step,
    p05: 240_000 + step,
    p25: 245_000 + step,
    p50: 250_000 + step,
    p75: 255_000 + step,
    p95: 260_000 + step,
  })),
  terminal: {
    wealth: summary(260_000, 10_000),
    total_return: summary(0.04, 0.04),
    prob_loss: 0.1234,
  },
  drawdown: {
    depth: summary(-0.15, 0.05),
    duration_bars: summary(40, 10),
    historical_depth: -0.12,
    historical_duration_bars: 33,
    prob_worse_than_historical: 0.44,
  },
  risk: {
    rows: [
      { horizon_days: 1, var_95: 0.0227, cvar_95: 0.031, var_99: 0.04, cvar_99: 0.05 },
      { horizon_days: 21, var_95: 0.095, cvar_95: 0.13, var_99: 0.16, cvar_99: 0.2 },
    ],
    prob_ruin: 0.025,
    ruin_threshold: 0.5,
  },
  sample_paths: [],
  caveat: null,
}

const CURVE = [250_000, 255_000, 252_000, 258_000].map((total) => ({ total }))

describe('SimulationResults', () => {
  it('hands the chart aligned rows and the run\'s own starting capital', () => {
    render(<SimulationResults result={RESULT} historicalCurve={CURVE} />)
    const chart = screen.getByTestId('fan-chart')
    expect(chart).toHaveAttribute('data-initial-capital', '250000')
    expect(chart).toHaveAttribute('data-rows', '4')
    // Invested from bar 0: step 0 = start, step j = bar j - 1.
    expect(chart).toHaveAttribute('data-historical', '250000,250000,255000,252000')
  })

  it('formats the probabilities the server computed', () => {
    render(<SimulationResults result={RESULT} historicalCurve={[]} />)
    expect(within(screen.getByTestId('prob-loss')).getByText('12.3%')).toBeInTheDocument()
    expect(within(screen.getByTestId('prob-ruin')).getByText('2.5%')).toBeInTheDocument()
    expect(screen.getByTestId('prob-ruin')).toHaveTextContent('losing 50% of start')
    expect(screen.getByTestId('drawdown-historical')).toHaveTextContent('-12.0%')
    expect(screen.getByTestId('drawdown-historical')).toHaveTextContent('P(worse): 44.0%')
  })

  it('lists one tail-risk row per horizon with losses as positive percentages', () => {
    render(<SimulationResults result={RESULT} historicalCurve={[]} />)
    expect(screen.getByText('1 day')).toBeInTheDocument()
    expect(screen.getByText('21 days')).toBeInTheDocument()
    expect(screen.getByText('2.27%')).toBeInTheDocument()
    expect(screen.getByText('9.50%')).toBeInTheDocument()
  })

  it('says which mode and method produced the bands', () => {
    render(<SimulationResults result={{ ...RESULT, mode: 'prices', method: 'gbm' }} historicalCurve={[]} />)
    expect(screen.getByText('price paths, strategy re-run')).toBeInTheDocument()
    expect(screen.getByText('gbm')).toBeInTheDocument()
    expect(screen.queryByText(/^block /)).not.toBeInTheDocument()
  })
})
