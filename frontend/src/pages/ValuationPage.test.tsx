/**
 * ValuationPage with the API mocked by real responses and the chart mocked
 * (Recharts draws nothing under jsdom); the chart's props are asserted.
 */

import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { AAPL_HISTORY, AAPL_VALUATION, ORCL_VALUATION } from '@/test/valuationFixture'
import { ValuationPage } from './ValuationPage'

vi.mock('@/components/charts/ValueHistoryChart', () => ({
  ValueHistoryChart: ({ rows }: { rows: Array<{ value: number | null; price: number | null }> }) => (
    <div data-testid="history-chart" data-rows={rows.length} data-gaps={rows.filter((r) => r.value === null).length} />
  ),
}))

beforeEach(() => {
  vi.spyOn(api, 'listAssets').mockResolvedValue({ count: 3, assets: [{ symbol: 'AAPL' }, { symbol: 'ORCL' }, { symbol: 'JPM' }] } as never)
  vi.spyOn(api, 'getValuationHistory').mockResolvedValue(AAPL_HISTORY)
})
afterEach(() => vi.restoreAllMocks())

describe('ValuationPage', () => {
  it('values AAPL by default, as of today, with every input and its source', async () => {
    const value = vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    await waitFor(() => expect(value).toHaveBeenCalled())
    expect(value.mock.calls[0][0]).toMatchObject({ symbol: 'AAPL' })
    expect(await screen.findByTestId('headline')).toHaveTextContent(`$${AAPL_VALUATION.result.per_share!.toFixed(2)} per share`)
    const inputs = screen.getByTestId('inputs')
    expect(within(inputs).getAllByText(AAPL_VALUATION.lines.cfo.source).length).toBeGreaterThan(0)
    expect(within(inputs).getByText(AAPL_VALUATION.lines.beta.source)).toBeInTheDocument()
    expect(within(screen.getByTestId('bridge')).getByText('Plus net cash')).toBeInTheDocument()
  })

  it('a floored WACC says so and shows the CAPM figure it replaced', async () => {
    const floored = {
      ...AAPL_VALUATION,
      discount: { ...AAPL_VALUATION.discount, wacc: 0.07, wacc_unfloored: 0.0487, floored: true },
    }
    vi.spyOn(api, 'valueCompany').mockResolvedValue(floored)
    renderPage(<ValuationPage />)
    const line = await screen.findByTestId('wacc-line')
    expect(line).toHaveTextContent('WACC 7.00%: the floor, because CAPM weights give 4.87%')
  })

  it('an unfloored WACC is stated as the weighted sum', async () => {
    vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    const line = await screen.findByTestId('wacc-line')
    expect(line).toHaveTextContent(/^WACC 9\.01% = /)
    expect(line).not.toHaveTextContent('floor')
  })

  it('a negative-cash-flow company gets an explanation, not a price', async () => {
    vi.spyOn(api, 'listAssets').mockResolvedValue({ count: 1, assets: [{ symbol: 'ORCL' }] } as never)
    vi.spyOn(api, 'valueCompany').mockResolvedValue(ORCL_VALUATION)
    renderPage(<ValuationPage />)
    const head = await screen.findByTestId('headline')
    expect(head).toHaveTextContent('nothing to grow')
    expect(head).not.toHaveTextContent('per share')
  })

  it('a refusal shows its reason and is not retried', async () => {
    vi.spyOn(api, 'listAssets').mockResolvedValue({ count: 1, assets: [{ symbol: 'JPM' }] } as never)
    const value = vi.spyOn(api, 'valueCompany').mockRejectedValue(
      new ApiError(422, 'JPM is in Financials: free cash flow is not meaningful for banks and insurers'))
    renderPage(<ValuationPage />)
    expect(await screen.findByTestId('refusal')).toHaveTextContent('banks and insurers')
    await new Promise((r) => setTimeout(r, 1200))
    expect(value).toHaveBeenCalledTimes(1)
  })

  it('an override is sent as a decimal and reported back', async () => {
    const value = vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    await screen.findByTestId('headline')
    await userEvent.type(screen.getByLabelText('Discount rate, WACC (%)'), '9')
    await userEvent.click(screen.getByRole('button', { name: 'Recalculate' }))
    await waitFor(() => expect(value.mock.calls.some((c) => c[0].wacc === 0.09)).toBe(true))
  })

  it('draws the history from priced filings and says why the rest were not valued', async () => {
    vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    const chart = await screen.findByTestId('history-chart')
    const priced = AAPL_HISTORY.points.filter((p) => p.price !== null)
    expect(Number(chart.dataset.rows)).toBe(priced.length)
    expect(Number(chart.dataset.gaps)).toBe(priced.filter((p) => p.per_share === null).length)
    if (AAPL_HISTORY.points.length > priced.length) {
      expect(screen.getByTestId('history-skipped')).toHaveTextContent('Not valued')
    }
  })
})


describe('ValuationPage date', () => {
  it('defaults to the LOCAL date, not UTC (a US evening is already tomorrow in UTC)', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    // 2026-10-11 21:30 in New York is 2026-10-12 01:30 UTC.
    vi.setSystemTime(new Date(2026, 9, 11, 21, 30))
    try {
      const value = vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
      renderPage(<ValuationPage />)
      await waitFor(() => expect(value).toHaveBeenCalled())
      const d = new Date(2026, 9, 11, 21, 30)
      const local = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
      expect(value.mock.calls[0][0].as_of).toBe(local)
    } finally {
      vi.useRealTimers()
    }
  })
})

describe('ValuationPage — the shared ticker (2026-10-15)', () => {
  it('opens on the ticker chosen on another page', async () => {
    const { useAppStore } = await import('@/store/useAppStore')
    useAppStore.getState().setSymbol('ORCL')
    const value = vi.spyOn(api, 'valueCompany').mockResolvedValue(ORCL_VALUATION)
    renderPage(<ValuationPage />)
    await waitFor(() => expect(value).toHaveBeenCalled())
    expect(value.mock.calls[0][0]).toMatchObject({ symbol: 'ORCL' })
  })

  it('a ticker it cannot value falls back to the default and says so, keeping the shared one', async () => {
    const { useAppStore } = await import('@/store/useAppStore')
    useAppStore.getState().setSymbol('BTC-USD')
    const value = vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    expect(await screen.findByTestId('not-an-equity')).toHaveTextContent('BTC-USD is not an equity')
    expect(value.mock.calls[0][0]).toMatchObject({ symbol: 'AAPL' })
    expect(useAppStore.getState().selectedSymbol).toBe('BTC-USD')
  })

  it('choosing a ticker here sets the shared one', async () => {
    const { useAppStore } = await import('@/store/useAppStore')
    vi.spyOn(api, 'valueCompany').mockResolvedValue(AAPL_VALUATION)
    renderPage(<ValuationPage />)
    const box = await screen.findByRole('combobox')
    await userEvent.clear(box)
    await userEvent.type(box, 'jpm{Enter}')
    expect(useAppStore.getState().selectedSymbol).toBe('JPM')
  })
})
