/**
 * ScreenersPage.
 *
 * The behaviour that matters is COMPOSITION: each step filters the survivors
 * of the last, and the per-step counts must show where a screen emptied. A
 * page that only rendered the final list would hide that.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { ScreenersPage } from './ScreenersPage'

const CATALOGUE = {
  count: 2,
  screeners: [
    {
      id: 'low_volatility',
      display_name: 'Low Volatility',
      description: 'Keep the least volatile names.',
      caveat: null,
      params: [
        {
          name: 'quantile',
          type: 'float',
          default: 0.25,
          label: 'Volatility quantile',
          description: '',
          minimum: 0.01,
          maximum: 1,
        },
      ],
    },
    {
      id: 'momentum',
      display_name: 'Momentum',
      description: 'Keep names clearing a return floor.',
      caveat: null,
      params: [
        {
          name: 'momentum_window',
          type: 'int',
          default: 126,
          label: 'Lookback window',
          description: '',
          minimum: 2,
          maximum: 500,
        },
      ],
    },
  ],
}

const RESULT = {
  requested: 7,
  with_data: 6,
  passed: ['AAPL', 'KO'],
  steps: [
    {
      screener_id: 'low_volatility',
      display_name: 'Low Volatility',
      params: { quantile: 0.25 },
      passed: 3,
    },
    {
      screener_id: 'momentum',
      display_name: 'Momentum',
      params: { momentum_window: 126 },
      passed: 2,
    },
  ],
}

beforeEach(() => {
  vi.spyOn(api, 'listScreeners').mockResolvedValue(CATALOGUE as never)
  vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
})

afterEach(() => vi.restoreAllMocks())

describe('ScreenersPage — building a screen', () => {
  it('offers the catalogue', async () => {
    renderPage(<ScreenersPage />)
    expect(
      await screen.findByRole('button', { name: '+ Low Volatility' }),
    ).toBeInTheDocument()
  })

  it('adds steps in order and sends them that way', async () => {
    const run = vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)

    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: '+ Momentum' }))
    await userEvent.click(screen.getByRole('button', { name: '+ Low Volatility' }))
    await userEvent.click(screen.getByRole('button', { name: /^Screen/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    // Order is the point: steps compose, so momentum-then-volatility is not
    // the same screen as volatility-then-momentum.
    expect(run.mock.calls[0][0].screeners.map((s) => s.screener_id)).toEqual([
      'momentum',
      'low_volatility',
    ])
  })

  it('lets the same screener appear twice', async () => {
    const run = vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)
    renderPage(<ScreenersPage />)

    const button = await screen.findByRole('button', { name: '+ Momentum' })
    await userEvent.click(button)
    await userEvent.click(button)
    await userEvent.click(screen.getByRole('button', { name: /^Screen/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0].screeners).toHaveLength(2)
  })

  it('removes a step', async () => {
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: '+ Momentum' }))
    expect(screen.getByText('1. Momentum')).toBeInTheDocument()

    await userEvent.click(screen.getByLabelText('Remove step 1'))
    expect(screen.queryByText('1. Momentum')).not.toBeInTheDocument()
  })

  it('parses the universe into symbols', async () => {
    const run = vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)
    renderPage(<ScreenersPage />)

    const input = await screen.findByLabelText('Symbols')
    await userEvent.clear(input)
    await userEvent.type(input, 'aapl, msft')
    await userEvent.click(screen.getByRole('button', { name: /^Screen/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0].symbols).toEqual(['AAPL', 'MSFT'])
  })

  it('fills the universe from a watchlist', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([
      { name: 'MAG7', symbols: ['AAPL', 'MSFT', 'NVDA'], created_at: null },
    ] as never)
    renderPage(<ScreenersPage />)

    await userEvent.click(await screen.findByRole('button', { name: /MAG7 \(3\)/ }))
    expect(screen.getByLabelText('Symbols')).toHaveValue('AAPL, MSFT, NVDA')
  })
})

describe('ScreenersPage — results', () => {
  it('shows the per-step counts, not just the survivors', async () => {
    vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Screen/ }))

    // Steps compose, so a screen ending empty must show WHERE it emptied.
    expect(await screen.findByText('Universe with data')).toBeInTheDocument()
    expect(screen.getByText(/1\. Low Volatility/)).toBeInTheDocument()
    expect(screen.getByText('3')).toBeInTheDocument()
    expect(screen.getByText(/2\. Momentum/)).toBeInTheDocument()
  })

  it('lists the survivors', async () => {
    vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Screen/ }))

    expect(await screen.findByText('Survivors (2)')).toBeInTheDocument()
    expect(screen.getByText('AAPL')).toBeInTheDocument()
    expect(screen.getByText('KO')).toBeInTheDocument()
  })

  it('points at the step counts when nothing passes', async () => {
    vi.spyOn(api, 'runScreen').mockResolvedValue({
      ...RESULT,
      passed: [],
      steps: [{ ...RESULT.steps[0], passed: 0 }],
    } as never)
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Screen/ }))

    expect(await screen.findByText(/Nothing passed/)).toBeInTheDocument()
    expect(screen.getByText(/where the universe emptied/)).toBeInTheDocument()
  })

  it('distinguishes symbols with no bars from symbols that failed a filter', async () => {
    // requested 7, with_data 6 — one symbol had no bars at all, which is not
    // the same as failing a screen.
    vi.spyOn(api, 'runScreen').mockResolvedValue(RESULT as never)
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Screen/ }))

    expect(await screen.findByText(/7 requested · 6 had bars · 2 passed/)).toBeInTheDocument()
  })

  it('surfaces an API rejection', async () => {
    vi.spyOn(api, 'runScreen').mockRejectedValue(
      new ApiError(422, '250 symbols requested; the limit is 200.'),
    )
    renderPage(<ScreenersPage />)
    await userEvent.click(await screen.findByRole('button', { name: /^Screen/ }))

    expect(await screen.findByText(/the limit is 200/)).toBeInTheDocument()
  })
})
