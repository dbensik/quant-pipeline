/** A benchmark report for the tests (decision 53). */

import type { BenchmarkReportOut } from '@/api/client'

export const REPORT: BenchmarkReportOut = {
  symbol: 'SPY',
  start: '2011-01-03T00:00:00Z',
  end: '2026-10-14T00:00:00Z',
  bars: 3970,
  periods_per_year: 252,
  beta: 0.297,
  alpha: -0.0002,
  alpha_t: -0.01,
  alpha_low: -0.031,
  alpha_high: 0.0306,
  r_squared: 0.41,
  rolling_window: 252,
  rolling_sharpe: [
    { time: '2012-01-03T00:00:00Z', strategy: 0.5, benchmark: 0.9 },
    { time: '2012-01-04T00:00:00Z', strategy: null, benchmark: 0.8 },
  ],
  regimes: [
    {
      name: 'high',
      days: 1300,
      share: 0.327,
      strategy_return: 0.041,
      benchmark_return: 0.12,
      strategy_sharpe: 0.61,
      benchmark_sharpe: 0.71,
      strategy_sharpe_error: 0.46,
      benchmark_sharpe_error: 0.459,
    },
  ],
  unranked_days: 26,
}
