import { describe, expect, it } from 'vitest'

import { REPORT } from './benchmarkReportFixture'
import { exposureLine, regimeRows, rollingSeries } from './benchmarkReportRows'

describe('exposureLine', () => {
  it('says beta, alpha with its interval, and whether it is told from zero', () => {
    expect(exposureLine(REPORT)).toBe(
      'Beta 0.30 to SPY. Alpha -0.0% a year (95% interval -3.1% to +3.1%, t -0.01): ' +
        'not distinguishable from zero.',
    )
  })

  it('a |t| of 1.96 or more is distinguishable', () => {
    expect(exposureLine({ ...REPORT, alpha_t: -2.1 })).toMatch(
      /: distinguishable from zero at 95%\.$/,
    )
  })
})

describe('regimeRows', () => {
  it('prints each Sharpe with its error and names the third', () => {
    expect(regimeRows(REPORT.regimes!)).toEqual([
      {
        name: 'Most volatile third',
        days: '1300 (33%)',
        strategySharpe: '0.61 ± 0.46',
        benchmarkSharpe: '0.71 ± 0.46',
        strategyReturn: '+4.1%',
        benchmarkReturn: '+12.0%',
      },
    ])
  })

  it('a missing Sharpe is a dash, not 0', () => {
    const [row] = regimeRows([{ ...REPORT.regimes![0], strategy_sharpe: null }])
    expect(row.strategySharpe).toBe('—')
  })
})

describe('rollingSeries', () => {
  it('two series, a window without a Sharpe left out', () => {
    expect(rollingSeries(REPORT, 'Trend')).toEqual([
      { name: 'Trend', points: [{ time: '2012-01-03T00:00:00Z', total: 0.5 }] },
      {
        name: 'SPY',
        points: [
          { time: '2012-01-03T00:00:00Z', total: 0.9 },
          { time: '2012-01-04T00:00:00Z', total: 0.8 },
        ],
      },
    ])
  })

  it('keeps the names apart when the strategy is named like its benchmark', () => {
    expect(rollingSeries(REPORT, 'SPY').map((s) => s.name)).toEqual(['SPY', 'SPY (benchmark)'])
  })
})
