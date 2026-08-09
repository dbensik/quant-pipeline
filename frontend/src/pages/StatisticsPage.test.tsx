/**
 * StatisticsPage.
 *
 * The catalogue DESCRIBES each test — arity and input_kind — and the page is
 * driven by that rather than hardcoding six tests. These assert the
 * description is actually used, since a page that ignored it would look right
 * and 422 on every pair test.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { StatisticsPage } from './StatisticsPage'

const CATALOGUE = {
  count: 3,
  tests: [
    {
      id: 'adf',
      display_name: 'Augmented Dickey-Fuller',
      description: 'Tests whether a series is stationary.',
      arity: 'single',
      input_kind: 'price',
      min_symbols: 1,
      params: [],
      caveat: null,
    },
    {
      id: 'ols',
      display_name: 'OLS Regression (Alpha / Beta)',
      description: 'Regresses an asset on a benchmark.',
      arity: 'pair',
      input_kind: 'returns',
      min_symbols: 2,
      params: [],
      caveat: null,
    },
    {
      id: 'pca',
      display_name: 'Principal Component Analysis',
      description: 'Shared variance across a basket.',
      arity: 'multi',
      input_kind: 'returns',
      min_symbols: 2,
      params: [],
      caveat: null,
    },
  ],
}

const ADF_RESULT = {
  test_id: 'adf',
  display_name: 'Augmented Dickey-Fuller',
  symbols: ['AAPL'],
  input_kind: 'price',
  observations: 400,
  params: {},
  result: {
    'p-value': 0.7052,
    is_stationary: false,
    integration_order: 'I(1)',
    interpretation: 'The series is I(1).',
    'Critical Values': { '1%': -3.4, '5%': -2.9 },
  },
}

beforeEach(() => {
  vi.spyOn(api, 'listStatistics').mockResolvedValue(CATALOGUE as never)
})

afterEach(() => vi.restoreAllMocks())

describe('StatisticsPage — catalogue drives the form', () => {
  it('lists the tests with their input kind', async () => {
    renderPage(<StatisticsPage />)
    // The selected test's name appears twice — once in the picker, once in
    // the detail header — so this counts rather than expecting one.
    expect(
      (await screen.findAllByText('Augmented Dickey-Fuller')).length,
    ).toBeGreaterThan(0)
    // input_kind is surfaced because it changes what the test MEANS: PCA on
    // price levels measures shared trend, not co-movement.
    expect(screen.getAllByText('returns').length).toBeGreaterThan(0)
    expect(screen.getAllByText('price').length).toBeGreaterThan(0)
  })

  it('states how many symbols each test takes', async () => {
    renderPage(<StatisticsPage />)
    expect(await screen.findByText(/Takes one symbol/)).toBeInTheDocument()
    expect(screen.getByText(/Takes exactly two symbols, in order/)).toBeInTheDocument()
  })

  it('prefills two symbols when a pair test is chosen', async () => {
    renderPage(<StatisticsPage />)
    await userEvent.click(
      await screen.findByText('OLS Regression (Alpha / Beta)'),
    )
    // Otherwise switching from ADF leaves one symbol and 422s immediately.
    await waitFor(() =>
      expect(screen.getByLabelText('Symbols')).toHaveValue('AAPL, MSFT'),
    )
  })

  it('warns that order matters for a pair test', async () => {
    renderPage(<StatisticsPage />)
    await userEvent.click(await screen.findByText('OLS Regression (Alpha / Beta)'))
    // OLS treats the first symbol as the asset and the second as the
    // benchmark, so swapping them inverts alpha and beta.
    expect(
      await screen.findByText(/first is the asset, the second the benchmark/),
    ).toBeInTheDocument()
  })

  it('prefills several symbols for a multi test', async () => {
    renderPage(<StatisticsPage />)
    await userEvent.click(await screen.findByText('Principal Component Analysis'))
    await waitFor(() =>
      expect(screen.getByLabelText('Symbols')).toHaveValue('AAPL, MSFT, JPM, XOM'),
    )
  })
})

describe('StatisticsPage — running', () => {
  it('posts to the chosen test with the symbol list', async () => {
    const run = vi.spyOn(api, 'runStatistic').mockResolvedValue(ADF_RESULT as never)

    renderPage(<StatisticsPage />)
    await screen.findAllByText('Augmented Dickey-Fuller')
    await userEvent.click(screen.getByRole('button', { name: 'Run test' }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]).toBe('adf')
    expect(run.mock.calls[0][1].symbols).toEqual(['AAPL'])
  })

  it('renders scalar results as a table', async () => {
    vi.spyOn(api, 'runStatistic').mockResolvedValue(ADF_RESULT as never)
    renderPage(<StatisticsPage />)
    await userEvent.click(
      await screen.findByRole('button', { name: 'Run test' }),
    )

    expect(await screen.findByText('integration_order')).toBeInTheDocument()
    expect(screen.getByText('I(1)')).toBeInTheDocument()
    expect(screen.getByText('0.7052')).toBeInTheDocument()
  })

  it('puts nested results behind a disclosure rather than dropping them', async () => {
    vi.spyOn(api, 'runStatistic').mockResolvedValue(ADF_RESULT as never)
    renderPage(<StatisticsPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'Run test' }))

    expect(await screen.findByText('Critical Values')).toBeInTheDocument()
  })

  it('reports the observation count and input kind used', async () => {
    vi.spyOn(api, 'runStatistic').mockResolvedValue(ADF_RESULT as never)
    renderPage(<StatisticsPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'Run test' }))

    expect(
      await screen.findByText(/400 observations · computed on price/),
    ).toBeInTheDocument()
  })

  it('surfaces an API rejection', async () => {
    vi.spyOn(api, 'runStatistic').mockRejectedValue(
      new ApiError(422, "'ols' takes exactly 2 symbols; 1 given."),
    )
    renderPage(<StatisticsPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'Run test' }))

    expect(await screen.findByText(/takes exactly 2 symbols/)).toBeInTheDocument()
  })
})
