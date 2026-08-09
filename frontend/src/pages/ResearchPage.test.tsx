/**
 * ResearchPage.
 *
 * Two things here are regressions on how Streamlit rendered the same data:
 * absent metrics must not display as 0, and a news item must show a real
 * title, publisher and date rather than "[None](None)" from "Unknown" dated
 * 1970-01-01.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { ResearchPage } from './ResearchPage'

const PROFILE = {
  symbol: 'AAPL',
  long_name: 'Apple Inc.',
  short_name: 'Apple',
  sector: 'Technology',
  industry: 'Consumer Electronics',
  full_time_employees: 150000,
  business_summary: 'Designs and sells consumer electronics.',
  market_cap: 4.5728e12,
  trailing_pe: 38.2,
  forward_pe: 32.9,
  dividend_yield: 0.34,
  website: null,
  country: 'United States',
  currency: 'USD',
}

const FINANCIALS = {
  symbol: 'AAPL',
  quarterly: false,
  income_statement: [
    { line_item: 'Total Revenue', values: { '2025-12-31': 4.1e11 } },
  ],
  balance_sheet: [],
  cash_flow: [],
}

const NEWS = {
  source: 'market',
  symbols: ['SPY'],
  truncated_symbols: [],
  items: [
    {
      id: 'n1',
      symbol: 'SPY',
      title: 'Markets rally on rate hopes',
      url: 'https://example.com/a',
      publisher: 'Reuters',
      summary: 'Stocks rose.',
      published_at: '2026-08-09T16:00:00Z',
    },
  ],
}

beforeEach(() => {
  vi.spyOn(api, 'getProfile').mockResolvedValue(PROFILE as never)
  vi.spyOn(api, 'getFinancials').mockResolvedValue(FINANCIALS as never)
  vi.spyOn(api, 'getNews').mockResolvedValue(NEWS as never)
  vi.spyOn(api, 'listPortfolios').mockResolvedValue([] as never)
  vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
})

afterEach(() => vi.restoreAllMocks())

describe('ResearchPage — profile', () => {
  it('renders the company header and metrics', async () => {
    renderPage(<ResearchPage />)
    expect(await screen.findByText('Apple Inc.')).toBeInTheDocument()
    expect(screen.getByText(/Technology · Consumer Electronics/)).toBeInTheDocument()
    expect(screen.getByText('$4.57T')).toBeInTheDocument()
    expect(screen.getByText('38.20')).toBeInTheDocument()
  })

  it('shows an em dash for an absent metric, never 0', async () => {
    // THE regression. Streamlit did `info.get('trailingPE', 0)`, so a company
    // with no P/E displayed "0.00" — indistinguishable from a real zero.
    vi.spyOn(api, 'getProfile').mockResolvedValue({
      ...PROFILE,
      trailing_pe: null,
      market_cap: null,
    } as never)
    renderPage(<ResearchPage />)

    await screen.findByText('Apple Inc.')
    expect(screen.queryByText('0.00')).not.toBeInTheDocument()
    expect(screen.getAllByText('—').length).toBeGreaterThan(0)
  })

  it('looks up a different ticker on submit', async () => {
    const getProfile = vi.spyOn(api, 'getProfile')
    renderPage(<ResearchPage />)
    await screen.findByText('Apple Inc.')

    const input = screen.getByLabelText('Ticker')
    await userEvent.clear(input)
    await userEvent.type(input, 'msft')
    await userEvent.click(screen.getByRole('button', { name: 'Look up' }))

    await waitFor(() => expect(getProfile).toHaveBeenCalledWith('MSFT'))
  })

  it('marks a 503 as an upstream problem rather than a bad request', async () => {
    // A real ApiError, not a plain Error with fields bolted on: the retry
    // policy is `instanceof ApiError`, so a lookalike silently retries and the
    // test then waits out the backoff instead of asserting.
    vi.spyOn(api, 'getProfile').mockRejectedValue(
      new ApiError(503, "No profile available for 'NOSUCH'."),
    )
    renderPage(<ResearchPage />)
    expect(await screen.findByText(/upstream data provider/)).toBeInTheDocument()
  })
})

describe('ResearchPage — financials', () => {
  it('renders statement rows with their periods', async () => {
    renderPage(<ResearchPage />)
    expect(await screen.findByText('Total Revenue')).toBeInTheDocument()
    expect(screen.getByText('2025-12-31')).toBeInTheDocument()
    expect(screen.getByText('$410.00B')).toBeInTheDocument()
  })

  it('says so when a statement is not reported', async () => {
    renderPage(<ResearchPage />)
    // balance_sheet and cash_flow are empty in the fixture.
    await waitFor(() =>
      expect(screen.getAllByText('Not reported.').length).toBe(2),
    )
  })

  it('switches between annual and quarterly', async () => {
    const getFinancials = vi.spyOn(api, 'getFinancials')
    renderPage(<ResearchPage />)
    await screen.findByText('Total Revenue')

    await userEvent.click(screen.getByRole('button', { name: /Showing annual/ }))
    await waitFor(() =>
      expect(getFinancials).toHaveBeenCalledWith('AAPL', { quarterly: true }),
    )
  })
})

describe('ResearchPage — news', () => {
  it('renders a real title, publisher and date', async () => {
    // THE regression. yfinance 1.2.0 nests items under `content`; the Streamlit
    // widget read the flat keys, so every story rendered as "[None](None)"
    // from "Unknown", dated 1970-01-01.
    renderPage(<ResearchPage />)

    const link = await screen.findByText(/Markets rally on rate hopes/)
    expect(link.closest('a')).toHaveAttribute('href', 'https://example.com/a')
    expect(screen.getByText('Reuters')).toBeInTheDocument()
    expect(screen.queryByText(/1970/)).not.toBeInTheDocument()
  })

  it('defaults to the market feed', async () => {
    const getNews = vi.spyOn(api, 'getNews')
    renderPage(<ResearchPage />)
    await waitFor(() => expect(getNews).toHaveBeenCalledWith({}))
  })

  it('switches the feed to a watchlist', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([
      { name: 'MAG7', symbols: ['AAPL'], created_at: null },
    ] as never)
    const getNews = vi.spyOn(api, 'getNews')

    renderPage(<ResearchPage />)
    await userEvent.click(await screen.findByRole('button', { name: /MAG7/ }))

    await waitFor(() =>
      expect(getNews).toHaveBeenCalledWith({ watchlist: 'MAG7' }),
    )
  })

  it('switches the feed to a portfolio', async () => {
    vi.spyOn(api, 'listPortfolios').mockResolvedValue([
      { name: 'Growth', initial_cash: 1, created_at: null, metadata: null },
    ] as never)
    const getNews = vi.spyOn(api, 'getNews')

    renderPage(<ResearchPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'Growth' }))

    await waitFor(() =>
      expect(getNews).toHaveBeenCalledWith({ portfolio: 'Growth' }),
    )
  })

  it('reports symbols dropped by the ten-ticker cap', async () => {
    vi.spyOn(api, 'getNews').mockResolvedValue({
      ...NEWS,
      source: 'watchlist:Big',
      truncated_symbols: ['T11', 'T12'],
    } as never)
    renderPage(<ResearchPage />)

    // A long watchlist must not look fully covered.
    expect(await screen.findByText(/Not covered \(10-ticker limit\)/)).toBeInTheDocument()
    expect(screen.getByText(/T11, T12/)).toBeInTheDocument()
  })

  it('says so when there are no stories rather than rendering blank', async () => {
    vi.spyOn(api, 'getNews').mockResolvedValue({
      ...NEWS,
      items: [],
    } as never)
    renderPage(<ResearchPage />)
    expect(await screen.findByText('No stories found.')).toBeInTheDocument()
  })
})
