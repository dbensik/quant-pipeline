/**
 * DataPage.
 *
 * This page drives the write path that step 7d built. The Streamlit button it
 * replaces shelled out to cli/run_pipeline.py, which writes SQLite — a
 * database the API does not read.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { DataPage } from './DataPage'

const REPORT = {
  symbols: ['AAPL', 'HES'],
  written: 268,
  failed: [],
  started_at: '2026-08-09T19:00:00Z',
  finished_at: '2026-08-09T19:05:00Z',
  results: [
    {
      symbol: 'AAPL',
      fetched: 268,
      written: 268,
      skipped_empty: 0,
      error: null,
      first_bar: '2025-07-16T00:00:00Z',
      last_bar: '2026-08-07T00:00:00Z',
    },
    {
      symbol: 'HES',
      fetched: 0,
      written: 0,
      skipped_empty: 0,
      error: null,
      first_bar: null,
      last_bar: null,
    },
  ],
}

beforeEach(() => {
  vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
  vi.spyOn(api, 'ingestStatus').mockResolvedValue({
    running: false,
    started_at: null,
    completed: 0,
    total: 0,
    current_symbol: null,
  } as never)
})

afterEach(() => vi.restoreAllMocks())

describe('DataPage — ingest', () => {
  it('sends null symbols to refresh the whole registry', async () => {
    const run = vi.spyOn(api, 'runIngest').mockResolvedValue(REPORT as never)
    renderPage(<DataPage />)

    await userEvent.click(
      await screen.findByRole('button', { name: 'Ingest everything' }),
    )
    await waitFor(() => expect(run).toHaveBeenCalled())
    // null, not [] — an empty list would be a different request.
    expect(run.mock.calls[0][0]).toEqual({ symbols: null, full_backfill: false })
  })

  it('sends an explicit symbol list when given one', async () => {
    const run = vi.spyOn(api, 'runIngest').mockResolvedValue(REPORT as never)
    renderPage(<DataPage />)

    await userEvent.type(
      await screen.findByLabelText('Symbols (optional)'),
      'aapl, msft',
    )
    await userEvent.click(screen.getByRole('button', { name: /Ingest 2 symbols/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]?.symbols).toEqual(['AAPL', 'MSFT'])
  })

  it('passes the full-backfill flag', async () => {
    const run = vi.spyOn(api, 'runIngest').mockResolvedValue(REPORT as never)
    renderPage(<DataPage />)

    await userEvent.click(await screen.findByLabelText(/Full backfill/))
    await userEvent.click(screen.getByRole('button', { name: /Ingest everything/ }))

    await waitFor(() => expect(run).toHaveBeenCalled())
    expect(run.mock.calls[0][0]?.full_backfill).toBe(true)
  })

  it('summarises what was written', async () => {
    vi.spyOn(api, 'runIngest').mockResolvedValue(REPORT as never)
    renderPage(<DataPage />)
    await userEvent.click(
      await screen.findByRole('button', { name: 'Ingest everything' }),
    )

    expect(await screen.findByText('268 bars written')).toBeInTheDocument()
    expect(screen.getByText('2 symbols')).toBeInTheDocument()
  })

  it('does not present a zero-write symbol as a failure', async () => {
    // written: 0 with no error means already current OR delisted — the API
    // cannot yet tell them apart, so the UI must not claim either.
    vi.spyOn(api, 'runIngest').mockResolvedValue(REPORT as never)
    renderPage(<DataPage />)
    await userEvent.click(
      await screen.findByRole('button', { name: 'Ingest everything' }),
    )
    await userEvent.click(await screen.findByText('Per-symbol detail'))

    expect(
      screen.getByText(/up to date \(or no longer trading\)/),
    ).toBeInTheDocument()
  })

  it('shows failures distinctly', async () => {
    vi.spyOn(api, 'runIngest').mockResolvedValue({
      ...REPORT,
      failed: ['NOPE'],
      results: [
        { ...REPORT.results[0], symbol: 'NOPE', written: 0, error: 'Not in the asset registry — add it first.' },
      ],
    } as never)
    renderPage(<DataPage />)
    await userEvent.click(
      await screen.findByRole('button', { name: 'Ingest everything' }),
    )

    expect(await screen.findByText('1 failed')).toBeInTheDocument()
  })

  it('explains a 409 rather than showing a bare conflict', async () => {
    vi.spyOn(api, 'runIngest').mockRejectedValue(
      new ApiError(409, 'An ingest is already running.'),
    )
    renderPage(<DataPage />)
    await userEvent.click(
      await screen.findByRole('button', { name: 'Ingest everything' }),
    )

    expect(await screen.findByText(/already running/)).toBeInTheDocument()
    expect(screen.getByText(/same window twice/)).toBeInTheDocument()
  })
})

describe('DataPage — registering tickers', () => {
  it('registers a ticker with its asset class', async () => {
    const add = vi
      .spyOn(api, 'addAsset')
      .mockResolvedValue({ symbol: 'SOL-USD', asset_class: 'crypto', source: 'yfinance', created: true } as never)

    renderPage(<DataPage />)
    await userEvent.type(await screen.findByLabelText('Symbol'), 'sol-usd')
    await userEvent.selectOptions(screen.getByLabelText('Asset class'), 'crypto')
    await userEvent.click(screen.getByRole('button', { name: 'Register' }))

    await waitFor(() => expect(add).toHaveBeenCalled())
    expect(add.mock.calls[0][0]).toEqual({ symbol: 'sol-usd', asset_class: 'crypto' })
  })

  it('says when a ticker was already registered rather than erroring', async () => {
    vi.spyOn(api, 'addAsset').mockResolvedValue({
      symbol: 'AAPL',
      asset_class: 'equity',
      source: 'yfinance',
      created: false,
    } as never)

    renderPage(<DataPage />)
    await userEvent.type(await screen.findByLabelText('Symbol'), 'AAPL')
    await userEvent.click(screen.getByRole('button', { name: 'Register' }))

    expect(await screen.findByText(/was already registered/)).toBeInTheDocument()
  })
})

describe('DataPage — index constituents', () => {
  it('lists members without registering them', async () => {
    const universe = vi.spyOn(api, 'getUniverse').mockResolvedValue({
      source: 'dow_jones',
      symbols: ['AAPL', 'MSFT'],
      count: 2,
    } as never)
    const add = vi.spyOn(api, 'addAsset')

    renderPage(<DataPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'dow_jones' }))

    await waitFor(() => expect(universe).toHaveBeenCalledWith('dow_jones'))
    expect(await screen.findByText('2 members')).toBeInTheDocument()
    // Listing must not register anything — the caller decides.
    expect(add).not.toHaveBeenCalled()
  })

  it('can push the constituents into the ingest list', async () => {
    vi.spyOn(api, 'getUniverse').mockResolvedValue({
      source: 'dow_jones',
      symbols: ['AAPL', 'MSFT'],
      count: 2,
    } as never)

    renderPage(<DataPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'dow_jones' }))
    await userEvent.click(
      await screen.findByRole('button', { name: /Use as the ingest list/ }),
    )

    expect(screen.getByLabelText('Symbols (optional)')).toHaveValue('AAPL, MSFT')
  })

  it('surfaces an unreachable constituent list', async () => {
    vi.spyOn(api, 'getUniverse').mockRejectedValue(
      new ApiError(503, 'Could not fetch the sp500 universe.'),
    )
    renderPage(<DataPage />)
    await userEvent.click(await screen.findByRole('button', { name: 'sp500' }))

    expect(await screen.findByText(/Could not fetch the sp500/)).toBeInTheDocument()
  })
})
