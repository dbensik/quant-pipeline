/**
 * WatchlistMenu — saving the ticker a page shows to watchlists.
 *
 * The safety rule under test: this control adds or removes ONE ticker and
 * never re-saves a whole list (a stale copy would drop tickers saved elsewhere),
 * except to create a list that does not exist yet.
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { WatchlistMenu } from './WatchlistMenu'

const LISTS = [
  { name: 'Energy', symbols: ['XOM', 'CVX'], created_at: null },
  { name: 'MAG7', symbols: ['AAPL', 'MSFT'], created_at: null },
]

beforeEach(() => {
  vi.spyOn(api, 'listWatchlists').mockResolvedValue(LISTS as never)
})

afterEach(() => vi.restoreAllMocks())

async function openMenu(symbol = 'AAPL') {
  renderPage(<WatchlistMenu symbol={symbol} />)
  const button = await screen.findByRole('button', { name: /watchlist/i })
  await waitFor(() => expect(api.listWatchlists).toHaveBeenCalled())
  await userEvent.click(button)
  return screen.findByRole('dialog')
}

describe('WatchlistMenu', () => {
  it('says how many lists hold the ticker', async () => {
    renderPage(<WatchlistMenu symbol="AAPL" />)
    expect(await screen.findByRole('button', { name: 'In 1 watchlist' })).toBeInTheDocument()
  })

  it('offers to add a ticker no list holds', async () => {
    renderPage(<WatchlistMenu symbol="TSLA" />)
    expect(await screen.findByRole('button', { name: 'Add to watchlist' })).toBeInTheDocument()
  })

  it('ticks the lists that hold the ticker', async () => {
    await openMenu('AAPL')
    expect(await screen.findByRole('checkbox', { name: /MAG7/ })).toBeChecked()
    expect(screen.getByRole('checkbox', { name: /Energy/ })).not.toBeChecked()
  })

  it('ticking a list adds only this ticker, server side', async () => {
    const add = vi.spyOn(api, 'addToWatchlist').mockResolvedValue({} as never)
    const save = vi.spyOn(api, 'saveWatchlist')
    await openMenu('AAPL')
    await userEvent.click(await screen.findByRole('checkbox', { name: /Energy/ }))
    await waitFor(() => expect(add).toHaveBeenCalledWith('Energy', 'AAPL'))
    expect(save).not.toHaveBeenCalled()
  })

  it('unticking a list removes only this ticker', async () => {
    const remove = vi.spyOn(api, 'removeFromWatchlist').mockResolvedValue({} as never)
    await openMenu('AAPL')
    await userEvent.click(await screen.findByRole('checkbox', { name: /MAG7/ }))
    await waitFor(() => expect(remove).toHaveBeenCalledWith('MAG7', 'AAPL'))
  })

  it('creates a new list holding just this ticker', async () => {
    const save = vi.spyOn(api, 'saveWatchlist').mockResolvedValue({} as never)
    await openMenu('AAPL')
    await userEvent.type(screen.getByLabelText('New watchlist name'), 'Ideas')
    await userEvent.click(screen.getByRole('button', { name: 'Create' }))
    await waitFor(() => expect(save).toHaveBeenCalledWith('Ideas', ['AAPL']))
  })

  it('a name that already exists gets the ticker added, never overwritten', async () => {
    const add = vi.spyOn(api, 'addToWatchlist').mockResolvedValue({} as never)
    const save = vi.spyOn(api, 'saveWatchlist')
    await openMenu('AAPL')
    await userEvent.type(screen.getByLabelText('New watchlist name'), 'Energy')
    await userEvent.click(screen.getByRole('button', { name: 'Create' }))
    await waitFor(() => expect(add).toHaveBeenCalledWith('Energy', 'AAPL'))
    expect(save).not.toHaveBeenCalled()
  })

  it('renders nothing without a ticker', () => {
    const { container } = renderPage(<WatchlistMenu symbol={null} />)
    expect(container.querySelector('button')).toBeNull()
  })
})
