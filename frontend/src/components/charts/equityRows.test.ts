import { describe, expect, it } from 'vitest'

import { equityRows } from './equityRows'

const P = (time: string, total: number) => ({ time: `${time}T00:00:00Z`, total })

describe('equityRows', () => {
  it('pairs each strategy date with buy and hold on the same date', () => {
    expect(
      equityRows(
        [P('2024-01-02', 100), P('2024-01-03', 110)],
        [P('2024-01-02', 100), P('2024-01-03', 105)],
      ),
    ).toEqual([
      { date: '2024-01-02', total: 100, benchmark: 100 },
      { date: '2024-01-03', total: 110, benchmark: 105 },
    ])
  })

  it('leaves a missing benchmark day empty rather than carrying the last value', () => {
    const rows = equityRows([P('2024-01-02', 100), P('2024-01-03', 110)], [P('2024-01-02', 100)])
    expect(rows[1].benchmark).toBeNull()
  })

  it('works without a benchmark', () => {
    expect(equityRows([P('2024-01-02', 100)])).toEqual([
      { date: '2024-01-02', total: 100, benchmark: null },
    ])
  })
})
