/**
 * OptionsPage. Charts are mocked (Recharts draws nothing under jsdom) and
 * their PROPS asserted: which series are plotted, and that the cone and the
 * term structure arrive on the same calendar-day axis.
 */

import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { surfaceFixture } from '@/test/optionsFixture'
import { OptionsPage } from './OptionsPage'

vi.mock('@/components/charts/SmileChart', () => ({
  SmileChart: ({ series }: { series: Array<{ expiry: string; points: Array<Record<string, unknown>> }> }) => (
    <div
      data-testid="smile-chart"
      data-expiries={series.map((s) => s.expiry).join(',')}
      data-keys={[...new Set(series.flatMap((s) => s.points.flatMap((p) => Object.keys(p))))].sort().join(',')}
    />
  ),
}))

vi.mock('@/components/charts/TermStructureChart', () => ({
  TermStructureChart: ({ term, cone }: { term: Array<{ atmIv: number | null }>; cone: Array<{ days: number }> }) => (
    <div
      data-testid="term-chart"
      data-term={term.map((t) => (t.atmIv === null ? 'null' : t.atmIv)).join(',')}
      data-cone-days={cone.map((c) => c.days.toFixed(2)).join(',')}
    />
  ),
}))

const ARCHIVE = {
  total_rows: 3,
  tickers: [
    { ticker: 'AAPL', captures: [{ day: '2026-10-08', rows: 1, partial: false }] },
    {
      ticker: 'SPY',
      captures: [
        { day: '2026-10-07', rows: 1, partial: false },
        { day: '2026-10-08', rows: 1, partial: false },
        { day: '2026-10-09', rows: 1, partial: true },
      ],
    },
  ],
}

beforeEach(() => {
  vi.spyOn(api, 'getOptionsArchive').mockResolvedValue(ARCHIVE as never)
})
afterEach(() => vi.restoreAllMocks())

describe('OptionsPage', () => {
  it('opens SPY on its latest COMPLETE capture, never the partial one', async () => {
    const surface = vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    renderPage(<OptionsPage />)
    await waitFor(() => expect(surface).toHaveBeenCalled())
    expect(surface).toHaveBeenCalledWith('SPY', '2026-10-08')
    expect(surface).not.toHaveBeenCalledWith('SPY', '2026-10-09')
  })

  it('says how long a first request takes while the chain is priced', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockReturnValue(new Promise(() => {}))
    renderPage(<OptionsPage />)
    expect(await screen.findByTestId('surface-loading')).toHaveTextContent('10–35 seconds')
  })

  it('plots our IV only, for the default spread of expiries', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    renderPage(<OptionsPage />)
    const chart = await screen.findByTestId('smile-chart')
    expect(chart.dataset.expiries).toBe('2026-10-30,2026-12-18,2027-01-15')
    expect(chart.dataset.keys).toBe('isCall,iv,strike,x')
    expect(screen.getByText(/differs from ours by a median 1\.50 vol points/)).toBeInTheDocument()
  })

  it('toggles an expiry off the smile', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    renderPage(<OptionsPage />)
    await screen.findByTestId('smile-chart')
    await userEvent.click(screen.getByRole('button', { name: /2026-12-18/ }))
    expect(screen.getByTestId('smile-chart').dataset.expiries).toBe('2026-10-30,2027-01-15')
  })

  it('draws the term structure with gaps for nulls, over the cone at calendar days', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    renderPage(<OptionsPage />)
    const chart = await screen.findByTestId('term-chart')
    expect(chart.dataset.term).toBe('0.12,null,0.137')
    expect(chart.dataset.coneDays).toBe(['14.48', '28.97', '86.90', '173.81'].join(','))
  })

  it('without a cone, draws the term structure alone and says why', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(
      surfaceFixture({ cone: null, cone_reason: 'SPY has 12 usable bars up to 2026-10-08; the cone needs more' }),
    )
    renderPage(<OptionsPage />)
    expect(await screen.findByTestId('cone-reason')).toHaveTextContent('12 usable bars')
    expect(screen.getByTestId('term-chart').dataset.coneDays).toBe('')
  })

  it('shows the rate it used and the quote quality', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    renderPage(<OptionsPage />)
    expect(await screen.findByTestId('rate')).toHaveTextContent('^IRX 4.043% (discount)')
    expect(screen.getByTestId('quality')).toHaveTextContent('4489 priced of 5404')
    expect(screen.getByTestId('quality')).toHaveTextContent('1 forward unconverged')
  })

  it('does not retry a 409 (no rate stored): it is an answer', async () => {
    const surface = vi
      .spyOn(api, 'getOptionsSurface')
      .mockRejectedValue(new ApiError(409, 'No ^IRX observation on or before 2026-10-08'))
    renderPage(<OptionsPage />)
    expect(await screen.findByText(/No \^IRX observation/)).toBeInTheDocument()
    await new Promise((r) => setTimeout(r, 1200))
    expect(surface).toHaveBeenCalledTimes(1)
  })

  it('does retry a server error once', async () => {
    const surface = vi.spyOn(api, 'getOptionsSurface').mockRejectedValue(new ApiError(500, 'boom'))
    renderPage(<OptionsPage />)
    await waitFor(() => expect(surface).toHaveBeenCalledTimes(2), { timeout: 3000 })
  })

  it('prices with every model, starting from the expiry’s implied spot and forward', async () => {
    vi.spyOn(api, 'getOptionsSurface').mockResolvedValue(surfaceFixture())
    const price = vi.spyOn(api, 'priceOption').mockResolvedValue({
      style: 'american',
      quotes: [
        { model: 'crr_tree', display_name: 'Cox-Ross-Rubinstein tree', style: 'american', price: 9.1234, delta: -0.45, gamma: 0.01, early_exercise_premium: 0.31 },
      ],
    } as never)
    renderPage(<OptionsPage />)
    await screen.findByTestId('smile-chart')
    // Default prefill expiry: nearest a month out, 2026-10-30 (22 days).
    expect(screen.getByLabelText('Spot')).toHaveValue(778.1)
    await userEvent.click(screen.getByRole('button', { name: /Price this american put/ }))
    await waitFor(() => expect(price).toHaveBeenCalled())
    const body = price.mock.calls[0][0]
    expect(Math.abs(body.spot * Math.exp((body.rate - (body.dividend_yield ?? 0)) * body.expiry_years) - 780.0)).toBeLessThan(1e-4)
    expect(body.sigma).toBe(0.12)
    const table = await screen.findByTestId('quotes')
    expect(within(table).getByText('9.1234')).toBeInTheDocument()
    expect(within(table).getByText('0.3100')).toBeInTheDocument()
  })
})
