/**
 * PayoffPage (decision 54). The chart is mocked (Recharts draws nothing
 * under jsdom) and its props asserted; the arithmetic has its own tests in
 * components/options/payoff.test.ts.
 */

import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { PayoffPage } from './PayoffPage'

const chartProps = vi.fn()
vi.mock('@/components/charts/PayoffChart', () => ({
  PayoffChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="payoff-chart" />
  },
}))

const ARCHIVE = {
  total_rows: 2,
  tickers: [
    { ticker: 'AAPL', captures: [{ day: '2026-10-20', rows: 1, partial: false }] },
    {
      ticker: 'SPY',
      captures: [
        { day: '2026-10-19', rows: 1, partial: false },
        { day: '2026-10-20', rows: 1, partial: false },
      ],
    },
  ],
}

const QUOTES = {
  ticker: 'SPY',
  day: '2026-10-20',
  captured_at: '2026-10-20T19:45:04Z',
  expiries: ['2026-10-26', '2026-11-20'],
  expiry: '2026-11-20',
  spot: 768.5,
  quotes: [
    { strike: 730, call_bid: 40, call_ask: 41, put_bid: 0.9, put_ask: 1.0 },
    { strike: 750, call_bid: 21, call_ask: 21.2, put_bid: 2.9, put_ask: 3.0 },
    { strike: 765, call_bid: 9, call_ask: 9.2, put_bid: 5, put_ask: 5.2 },
    { strike: 770, call_bid: 6.0, call_ask: 6.2, put_bid: 7.1, put_ask: 7.3 },
    { strike: 790, call_bid: 1.0, call_ask: 1.1, put_bid: null, put_ask: null },
    { strike: 805, call_bid: 0.3, call_ask: 0.4, put_bid: 37, put_ask: 38 },
  ],
}

const summary = () => screen.findByTestId('payoff-summary')

beforeEach(() => {
  chartProps.mockClear()
  vi.spyOn(api, 'getOptionsArchive').mockResolvedValue(ARCHIVE as never)
})
afterEach(() => vi.restoreAllMocks())

describe('PayoffPage', () => {
  it('opens SPY on its latest capture, a straddle at the strike nearest spot', async () => {
    const get = vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)

    const s = within(await summary())
    expect(get).toHaveBeenCalledWith('SPY', null, null)
    // Buying both 770s at the ask: 6.20 + 7.30 a share.
    expect(s.getByText('$1,350 paid')).toBeInTheDocument()
    expect(s.getByText('Unlimited')).toBeInTheDocument()
    expect(s.getByText('-$1,350')).toBeInTheDocument()
    expect(s.getByText('756.50, 783.50')).toBeInTheDocument()
    const props = chartProps.mock.calls.at(-1)?.[0] as { reference: number; breakevens: number[] }
    expect(props.reference).toBe(768.5)
    expect(props.breakevens).toEqual([756.5, 783.5])
  })

  it('fills at the mid when asked', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Fill at'), 'mid')
    expect(within(await summary()).getByText('$1,330 paid')).toBeInTheDocument()
  })

  it('lets a typed premium replace the quote', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    const premium = screen.getByLabelText('Leg 1 premium')
    expect(premium).toHaveAttribute('placeholder', '6.20')
    await userEvent.type(premium, '5')
    expect(within(await summary()).getByText('$1,230 paid')).toBeInTheDocument()
  })

  it('builds an iron condor from the listed strikes, selling at the bid', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Structure'), 'iron_condor')

    const strikes = [1, 2, 3, 4].map(
      (i) => (screen.getByLabelText(`Leg ${i} strike`) as HTMLSelectElement).value,
    )
    expect(strikes).toEqual(['730', '750', '790', '805'])
    // 2.90 + 1.00 received, 1.00 + 0.40 paid; the put wing is 20 wide.
    const s = within(await summary())
    expect(s.getByText('$250 received')).toBeInTheDocument()
    expect(s.getByText('Max gain').nextSibling).toHaveTextContent(/^\$250$/)
    expect(s.getByText('Max loss').nextSibling).toHaveTextContent(/^-\$1,750$/)
  })

  it('asks for a premium the capture did not quote', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Leg 2 strike'), '790')

    expect(screen.getByText(/No quote for leg 2/)).toBeInTheDocument()
    expect(screen.queryByTestId('payoff-summary')).not.toBeInTheDocument()
    await userEvent.type(screen.getByLabelText('Leg 2 premium'), '22')
    expect(within(await summary()).getByText('$2,820 paid')).toBeInTheDocument()
  })

  it('works on typed premiums with no capture', async () => {
    const get = vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    const calls = get.mock.calls.length
    await userEvent.selectOptions(screen.getByLabelText('Underlying'), '')

    expect(screen.getByText(/No quote for leg 1, 2/)).toBeInTheDocument()
    expect((screen.getByLabelText('Leg 1 strike') as HTMLInputElement).value).toBe('100')
    await userEvent.type(screen.getByLabelText('Leg 1 premium'), '3')
    await userEvent.type(screen.getByLabelText('Leg 2 premium'), '2')
    expect(within(await summary()).getByText('95.00, 105.00')).toBeInTheDocument()
    expect(get.mock.calls.length).toBe(calls)
  })

  it('asks for another expiry when one is chosen', async () => {
    const get = vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Expiry'), '2026-10-26')
    await waitFor(() => expect(get).toHaveBeenCalledWith('SPY', null, '2026-10-26'))
  })

  it('surfaces a missing capture', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockRejectedValue(
      new ApiError(404, 'No complete captures for SPY'),
    )
    renderPage(<PayoffPage />)
    expect(await screen.findByText('No complete captures for SPY')).toBeInTheDocument()
  })
})
