/**
 * WatchlistsPage — the first page with WRITES.
 *
 * The behaviour worth testing here is not that a form renders, it is that a
 * save reaches the API with the right body and that the list refreshes
 * afterwards. A mutation without invalidation still "works" in the sense that
 * the request succeeds, and the UI silently keeps showing the old list — the
 * frontend twin of the expire_on_commit=False bug on the backend.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { WatchlistsPage } from './WatchlistsPage'

const MAG7 = {
  name: 'MAG7',
  symbols: ['AAPL', 'MSFT', 'NVDA'],
  created_at: '2026-08-09T00:00:00Z',
}
const CRYPTO = { name: 'Crypto', symbols: ['BTC-USD'], created_at: null }

beforeEach(() => {
  vi.spyOn(api, 'listAssets').mockResolvedValue({
    count: 2,
    assets: [
      { symbol: 'AAPL', asset_class: 'equity', source: 'yfinance' },
      { symbol: 'MSFT', asset_class: 'equity', source: 'yfinance' },
    ],
  } as never)
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('WatchlistsPage — reading', () => {
  it('lists saved watchlists with their sizes', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([CRYPTO, MAG7] as never)
    renderPage(<WatchlistsPage />)

    expect(await screen.findByText('MAG7')).toBeInTheDocument()
    expect(screen.getByText('Crypto')).toBeInTheDocument()
    // Sizes come from the payload, not from a second request per list.
    expect(screen.getByText('3')).toBeInTheDocument()
  })

  it('loads a watchlist into the editor when selected', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([MAG7] as never)
    renderPage(<WatchlistsPage />)

    await userEvent.click(await screen.findByText('MAG7'))

    expect(screen.getByLabelText('Name')).toHaveValue('MAG7')
    for (const symbol of MAG7.symbols) {
      expect(screen.getByLabelText(`Remove ${symbol}`)).toBeInTheDocument()
    }
  })

  it('surfaces a load failure instead of rendering an empty list', async () => {
    vi.spyOn(api, 'listWatchlists').mockRejectedValue(
      Object.assign(new Error('boom'), { detail: 'Database unreachable' }),
    )
    renderPage(<WatchlistsPage />)
    expect(await screen.findByText(/Database unreachable/)).toBeInTheDocument()
  })
})

describe('WatchlistsPage — writing', () => {
  it('sends the complete symbol list on save', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
    const save = vi
      .spyOn(api, 'saveWatchlist')
      .mockResolvedValue({ name: 'Energy', symbols: ['XOM'] } as never)

    renderPage(<WatchlistsPage />)

    await userEvent.type(await screen.findByLabelText('Name'), 'Energy')
    await userEvent.type(screen.getByLabelText('Add ticker'), 'xom')
    await userEvent.click(screen.getByRole('button', { name: 'Add' }))
    await userEvent.click(screen.getByRole('button', { name: /Save watchlist/ }))

    // Upper-cased client-side so the chip matches what the server stores.
    await waitFor(() => expect(save).toHaveBeenCalledWith('Energy', ['XOM']))
  })

  it('refreshes the list after a save', async () => {
    const list = vi
      .spyOn(api, 'listWatchlists')
      .mockResolvedValue([] as never)
    vi.spyOn(api, 'saveWatchlist').mockResolvedValue({
      name: 'Energy',
      symbols: ['XOM'],
    } as never)

    renderPage(<WatchlistsPage />)
    await screen.findByLabelText('Name')
    const before = list.mock.calls.length

    await userEvent.type(screen.getByLabelText('Name'), 'Energy')
    await userEvent.click(screen.getByRole('button', { name: /Save watchlist/ }))

    // THE assertion. Without invalidateQueries the save succeeds and the list
    // keeps showing pre-save state.
    await waitFor(() =>
      expect(list.mock.calls.length).toBeGreaterThan(before),
    )
  })

  it('refreshes the list after a delete', async () => {
    const list = vi
      .spyOn(api, 'listWatchlists')
      .mockResolvedValue([MAG7] as never)
    const remove = vi.spyOn(api, 'deleteWatchlist').mockResolvedValue(undefined)

    renderPage(<WatchlistsPage />)
    await screen.findByText('MAG7')
    const before = list.mock.calls.length

    await userEvent.click(screen.getByLabelText('Delete MAG7'))

    await waitFor(() => expect(remove).toHaveBeenCalledWith('MAG7'))
    await waitFor(() => expect(list.mock.calls.length).toBeGreaterThan(before))
  })

  it('does not duplicate a ticker already in the list', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
    renderPage(<WatchlistsPage />)

    const entry = await screen.findByLabelText('Add ticker')
    await userEvent.type(entry, 'AAPL')
    await userEvent.click(screen.getByRole('button', { name: 'Add' }))
    await userEvent.type(entry, 'aapl')
    await userEvent.click(screen.getByRole('button', { name: 'Add' }))

    expect(screen.getAllByLabelText('Remove AAPL')).toHaveLength(1)
  })

  it('removing a chip drops it from the payload', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([MAG7] as never)
    const save = vi.spyOn(api, 'saveWatchlist').mockResolvedValue(MAG7 as never)

    renderPage(<WatchlistsPage />)
    await userEvent.click(await screen.findByText('MAG7'))
    await userEvent.click(screen.getByLabelText('Remove MSFT'))
    await userEvent.click(screen.getByRole('button', { name: /Save watchlist/ }))

    // Saving REPLACES the list, so an omitted ticker is a deletion — which is
    // what makes removing one through this control possible at all.
    await waitFor(() =>
      expect(save).toHaveBeenCalledWith('MAG7', ['AAPL', 'NVDA']),
    )
  })

  it('will not save without a name', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
    const save = vi.spyOn(api, 'saveWatchlist')

    renderPage(<WatchlistsPage />)
    const button = await screen.findByRole('button', { name: /Save watchlist/ })
    expect(button).toBeDisabled()
    expect(save).not.toHaveBeenCalled()
  })

  it('shows the API message when a save is rejected', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
    vi.spyOn(api, 'saveWatchlist').mockRejectedValue(
      Object.assign(new Error('bad'), { detail: '501 symbols; the limit is 500.' }),
    )

    renderPage(<WatchlistsPage />)
    await userEvent.type(await screen.findByLabelText('Name'), 'Huge')
    await userEvent.click(screen.getByRole('button', { name: /Save watchlist/ }))

    expect(await screen.findByText(/the limit is 500/)).toBeInTheDocument()
  })

  it('flags a ticker that is not in the asset registry', async () => {
    vi.spyOn(api, 'listWatchlists').mockResolvedValue([] as never)
    renderPage(<WatchlistsPage />)

    await userEvent.type(await screen.findByLabelText('Add ticker'), 'NOSUCH')
    await userEvent.click(screen.getByRole('button', { name: 'Add' }))

    // Allowed — it may be registered later — but a typo should not sit silently.
    expect(
      screen.getByTitle('NOSUCH is not in the asset registry'),
    ).toBeInTheDocument()
  })
})
