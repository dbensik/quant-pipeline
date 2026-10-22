/**
 * PayoffPage (decisions 54 and 55). The charts are mocked (Recharts draws
 * nothing under jsdom) and their props asserted; the arithmetic has its own
 * tests in components/options/payoff.test.ts and positionCurve.test.ts, and
 * the curve before expiry is priced by the API (mocked here).
 */

import { fireEvent, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import type { OptionPositionCurveRequest } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { PayoffPage } from './PayoffPage'

const chartProps = vi.fn()
vi.mock('@/components/charts/PayoffChart', () => ({
  PayoffChart: (props: unknown) => {
    chartProps(props)
    return <div data-testid="payoff-chart" />
  },
}))
const greekProps = vi.fn()
vi.mock('@/components/charts/GreekChart', () => ({
  GreekChart: (props: unknown) => {
    greekProps(props)
    return <div data-testid="greek-chart" />
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
  expiries: ['2026-10-26', '2026-11-20', '2026-12-18'],
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
  dividend_date_uncertain: [] as string[],
}

const FAR_QUOTES = {
  ...QUOTES,
  expiry: '2026-12-18',
  quotes: [{ strike: 770, call_bid: 14.0, call_ask: 14.3, put_bid: 15.0, put_ask: 15.4 }],
}

/** A stand-in curve: each rule and date gives its own delta, so a test can tell them apart. */
function fakeCurve(body: OptionPositionCurveRequest) {
  const delta = (body.vol_rule === 'sticky_moneyness' ? 20 : 10) + Number(body.as_of?.slice(8))
  const spots = [body.low as number, body.spot as number, body.high as number]
  return {
    ticker: body.ticker,
    day: body.day,
    as_of: body.as_of,
    last_expiry: '2026-11-20',
    vol_rule: body.vol_rule,
    spot: body.spot,
    rate: 0.04,
    cache_hit: true,
    legs: body.legs.map(() => ({
      T_remaining: 0.08,
      forward: 771,
      carry: 0.01,
      iv: 0.135,
      vol_source: 'interpolated',
      model_price: 6.1,
    })),
    points: spots.map((spot, i) => ({
      spot,
      value: 0,
      profit: -100 + i,
      delta,
      gamma: delta / 100,
      vega: 1,
      theta: -2,
      rho: 0.5,
    })),
  }
}

const summary = () => screen.findByTestId('payoff-summary')
const curveCalls = (spy: { mock: { calls: unknown[][] } }) =>
  spy.mock.calls.map((c) => c[0] as OptionPositionCurveRequest)

beforeEach(() => {
  chartProps.mockClear()
  greekProps.mockClear()
  vi.spyOn(api, 'getOptionsArchive').mockResolvedValue(ARCHIVE as never)
})
afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

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

describe('PayoffPage before expiry (decision 55)', () => {
  const mockCurve = () =>
    vi
      .spyOn(api, 'getPositionCurve')
      .mockImplementation(async (body) => fakeCurve(body) as never)

  it('asks for the curve on the capture day under both rules and draws it over the payoff', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    const post = mockCurve()
    renderPage(<PayoffPage />)

    await screen.findByTestId('curve-summary')
    const bodies = curveCalls(post)
    expect(bodies.map((b) => b.vol_rule).sort()).toEqual(['sticky_moneyness', 'sticky_strike'])
    expect(bodies[0]).toMatchObject({
      ticker: 'SPY',
      day: '2026-10-20',
      as_of: '2026-10-20',
      spot: 768.5,
      legs: [
        { kind: 'call', side: 1, quantity: 1, strike: 770, premium: 6.2, expiry: '2026-11-20' },
        { kind: 'put', side: 1, quantity: 1, strike: 770, premium: 7.3, expiry: '2026-11-20' },
      ],
    })
    // The same window as the payoff at expiry: 15% past the outer breakevens.
    expect(bodies[0].low).toBeCloseTo(756.5 * 0.85)
    expect(bodies[0].high).toBeCloseTo(783.5 * 1.15)

    const props = chartProps.mock.calls.at(-1)?.[0] as {
      points: unknown[]
      before: { x: number; profit: number }[]
    }
    expect(props.points.length).toBeGreaterThan(100)
    expect(props.before).toEqual([
      { x: bodies[0].low, profit: -100 },
      { x: 768.5, profit: -99 },
      { x: bodies[0].high, profit: -98 },
    ])
    const s = within(screen.getByTestId('curve-summary'))
    expect(s.getByText('Delta').nextSibling).toHaveTextContent('30.00') // strike, day 20
    expect(within(screen.getByTestId('leg-vols')).getAllByText('13.5%')).toHaveLength(2)
  })

  it('moves the date with the slider, once it rests', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    const post = mockCurve()
    renderPage(<PayoffPage />)
    await screen.findByTestId('curve-summary')

    const slider = screen.getByLabelText(/^Date:/) as HTMLInputElement
    expect(slider.max).toBe('31') // 2026-10-20 to 2026-11-20
    fireEvent.change(slider, { target: { value: '10' } })
    expect(screen.getByText(/Date: 2026-10-30 \(21 days to 2026-11-20\)/)).toBeInTheDocument()
    await waitFor(() =>
      expect(curveCalls(post).some((b) => b.as_of === '2026-10-30')).toBe(true),
    )
    await waitFor(() =>
      expect(
        within(screen.getByTestId('curve-summary')).getByText('Delta').nextSibling,
      ).toHaveTextContent('40.00'),
    )
  })

  it('switches the volatility rule, keeping the other rule dashed beside it', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    mockCurve()
    renderPage(<PayoffPage />)
    await screen.findByTestId('curve-summary')
    await waitFor(() =>
      expect((greekProps.mock.calls.at(-1)?.[0] as { points: { other?: number }[] }).points[1])
        .toEqual({ x: 768.5, value: 30, other: 40 }),
    )

    await userEvent.selectOptions(screen.getByLabelText('Volatility as the spot moves'), 'sticky_moneyness')
    await waitFor(() => {
      const props = greekProps.mock.calls.at(-1)?.[0] as {
        points: { other?: number }[]
        ruleLabel: string
      }
      expect(props.ruleLabel).toBe('sticky moneyness')
      expect(props.points[1]).toEqual({ x: 768.5, value: 40, other: 30 })
    })
  })

  it('charts the Greek chosen', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    mockCurve()
    renderPage(<PayoffPage />)
    await screen.findByTestId('curve-summary')
    await userEvent.selectOptions(screen.getByLabelText('Greek'), 'theta')
    const props = greekProps.mock.calls.at(-1)?.[0] as { label: string; points: { value: number }[] }
    expect(props.label).toBe('Theta')
    expect(props.points.map((p) => p.value)).toEqual([-2, -2, -2])
  })

  it('builds a calendar from the far expiry and prices its far leg from that expiry', async () => {
    const get = vi
      .spyOn(api, 'getOptionQuotes')
      .mockImplementation(async (_t, _d, expiry) =>
        (expiry === '2026-12-18' ? FAR_QUOTES : QUOTES) as never,
      )
    const post = mockCurve()
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Structure'), 'call_calendar')

    await waitFor(() => expect(get).toHaveBeenCalledWith('SPY', '2026-10-20', '2026-12-18'))
    expect((screen.getByLabelText('Leg 2 expiry') as HTMLSelectElement).value).toBe('2026-12-18')
    // Sell the near 770 call at its bid, buy the far one at its ask.
    const s = within(await summary())
    expect(s.getByText('$830 paid')).toBeInTheDocument()
    expect(s.queryByText('Max gain')).not.toBeInTheDocument()
    expect(screen.getByText('Profit before expiry')).toBeInTheDocument()

    await waitFor(() =>
      expect(curveCalls(post).at(-1)?.legs).toEqual([
        { kind: 'call', side: -1, quantity: 1, strike: 770, premium: 6.0, expiry: '2026-11-20' },
        { kind: 'call', side: 1, quantity: 1, strike: 770, premium: 14.3, expiry: '2026-12-18' },
      ]),
    )
    expect((screen.getByLabelText(/^Date:/) as HTMLInputElement).max).toBe('59')
    const props = chartProps.mock.calls.at(-1)?.[0] as { points: unknown[]; breakevens: number[] }
    expect(props.points).toEqual([])
    expect(props.breakevens).toEqual([])
  })

  it('passes over a far expiry near a projected ex-date, and says so when chosen', async () => {
    const flagged = {
      ...QUOTES,
      expiries: [...QUOTES.expiries, '2027-01-15'],
      dividend_date_uncertain: ['2026-12-18'],
    }
    const get = vi
      .spyOn(api, 'getOptionQuotes')
      .mockImplementation(async (_t, _d, expiry) =>
        (expiry && expiry !== '2026-11-20'
          ? { ...FAR_QUOTES, ...flagged, expiry, quotes: FAR_QUOTES.quotes }
          : flagged) as never,
      )
    mockCurve()
    renderPage(<PayoffPage />)
    await summary()
    await userEvent.selectOptions(screen.getByLabelText('Structure'), 'call_calendar')

    await waitFor(() => expect(get).toHaveBeenCalledWith('SPY', '2026-10-20', '2027-01-15'))
    expect((screen.getByLabelText('Leg 2 expiry') as HTMLSelectElement).value).toBe('2027-01-15')
    expect(screen.queryByTestId('near-ex-date')).not.toBeInTheDocument()

    await userEvent.selectOptions(screen.getByLabelText('Leg 2 expiry'), '2026-12-18')
    expect(screen.getByTestId('near-ex-date')).toHaveTextContent(/^2026-12-18 expires within days/)
  })

  it("notes the page's own expiry when it is near a projected ex-date", async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue({
      ...QUOTES,
      dividend_date_uncertain: ['2026-11-20'],
    } as never)
    mockCurve()
    renderPage(<PayoffPage />)
    await summary()
    expect(screen.getByTestId('near-ex-date')).toHaveTextContent(/^2026-11-20 expires/)
  })

  it('offers no curve and no calendars on typed premiums', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    const post = mockCurve()
    renderPage(<PayoffPage />)
    await screen.findByTestId('curve-summary')
    const calls = post.mock.calls.length
    await userEvent.selectOptions(screen.getByLabelText('Underlying'), '')

    await userEvent.type(screen.getByLabelText('Leg 1 premium'), '3')
    await userEvent.type(screen.getByLabelText('Leg 2 premium'), '2')
    await summary()
    expect(post.mock.calls.length).toBe(calls)
    expect(screen.queryByLabelText(/^Date:/)).not.toBeInTheDocument()
    expect(screen.queryByTestId('curve-summary')).not.toBeInTheDocument()
    const ids = [...(screen.getByLabelText('Structure') as HTMLSelectElement).options].map(
      (o) => o.value,
    )
    expect(ids).not.toContain('call_calendar')
    expect(ids).toContain('iron_condor')
  })

  it('surfaces a refused curve', async () => {
    vi.spyOn(api, 'getOptionQuotes').mockResolvedValue(QUOTES as never)
    vi.spyOn(api, 'getPositionCurve').mockRejectedValue(
      new ApiError(422, 'Leg 1: no smile for expiry 2026-11-20 in this capture'),
    )
    renderPage(<PayoffPage />)
    expect(
      await screen.findByText('Leg 1: no smile for expiry 2026-11-20 in this capture'),
    ).toBeInTheDocument()
    expect(await summary()).toBeInTheDocument()
  })
})
