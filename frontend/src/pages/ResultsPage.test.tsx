/**
 * ResultsPage.
 *
 * DataFrames arrive in pandas' `orient="split"` layout, which is the contract
 * dashboard_app/api_client.py already assumed — so rendering it is the point.
 *
 * Phase 5 — React pages for the ported routers
 */

import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError, api } from '@/api/client'
import { renderPage } from '@/test/renderPage'
import { ResultsPage } from './ResultsPage'

const FILES = [
  {
    name: 'BNHPortWeightOptimization250709.json',
    size_bytes: 249543,
    modified_at: '2025-07-09T12:00:00Z',
  },
]

const PAYLOAD = {
  symbol: '7 Tickers',
  metric: 'Sharpe Ratio',
  strategy: 'Buy & Hold Weight Optimization',
  results: {
    index: ['0', '1'],
    columns: ['Sharpe Ratio', 'Total Return'],
    data: [
      [1.2345, 0.4],
      [0.9876, 0.2],
    ],
  },
}

beforeEach(() => {
  vi.spyOn(api, 'listResults').mockResolvedValue(FILES as never)
  vi.spyOn(api, 'loadResult').mockResolvedValue(PAYLOAD as never)
})

afterEach(() => vi.restoreAllMocks())

describe('ResultsPage', () => {
  it('lists saved files', async () => {
    renderPage(<ResultsPage />)
    expect(
      await screen.findByText('BNHPortWeightOptimization250709.json'),
    ).toBeInTheDocument()
  })

  it('prompts before anything is selected', async () => {
    renderPage(<ResultsPage />)
    expect(await screen.findByText(/Choose a file to view it/)).toBeInTheDocument()
  })

  it('loads a result and renders its scalar fields', async () => {
    renderPage(<ResultsPage />)
    await userEvent.click(
      await screen.findByText('BNHPortWeightOptimization250709.json'),
    )

    expect(await screen.findByText(/Buy & Hold Weight Optimization/)).toBeInTheDocument()
    expect(screen.getByText(/7 Tickers/)).toBeInTheDocument()
    // "Sharpe Ratio" appears both as a scalar field and as a frame column, so
    // this counts rather than expecting one.
    expect(screen.getAllByText(/Sharpe Ratio/).length).toBeGreaterThan(1)
  })

  it('renders a split-orient DataFrame as a table', async () => {
    renderPage(<ResultsPage />)
    await userEvent.click(
      await screen.findByText('BNHPortWeightOptimization250709.json'),
    )

    // {index, columns, data} is pandas' orient="split" — the layout
    // api_client.py's _deserialize_data already reconstructed.
    expect(await screen.findByText('1.2345')).toBeInTheDocument()
    expect(screen.getByText('0.9876')).toBeInTheDocument()
  })

  it('says how many rows were truncated rather than silently cutting', async () => {
    vi.spyOn(api, 'loadResult').mockResolvedValue({
      results: {
        index: Array.from({ length: 120 }, (_, i) => String(i)),
        columns: ['x'],
        data: Array.from({ length: 120 }, (_, i) => [i]),
      },
    } as never)

    renderPage(<ResultsPage />)
    await userEvent.click(
      await screen.findByText('BNHPortWeightOptimization250709.json'),
    )

    expect(await screen.findByText(/Showing 50 of 120 rows/)).toBeInTheDocument()
  })

  it('deletes a result and refreshes the list', async () => {
    const list = vi.spyOn(api, 'listResults')
    const remove = vi.spyOn(api, 'deleteResult').mockResolvedValue(undefined)

    renderPage(<ResultsPage />)
    await screen.findByText('BNHPortWeightOptimization250709.json')
    const before = list.mock.calls.length

    await userEvent.click(
      screen.getByLabelText('Delete BNHPortWeightOptimization250709.json'),
    )

    await waitFor(() =>
      expect(remove).toHaveBeenCalledWith('BNHPortWeightOptimization250709.json'),
    )
    await waitFor(() => expect(list.mock.calls.length).toBeGreaterThan(before))
  })

  it('says so when nothing is saved', async () => {
    vi.spyOn(api, 'listResults').mockResolvedValue([] as never)
    renderPage(<ResultsPage />)
    expect(await screen.findByText('Nothing saved yet.')).toBeInTheDocument()
  })

  it('surfaces a load failure', async () => {
    vi.spyOn(api, 'loadResult').mockRejectedValue(
      new ApiError(404, "No saved result named 'gone.json'."),
    )
    renderPage(<ResultsPage />)
    await userEvent.click(
      await screen.findByText('BNHPortWeightOptimization250709.json'),
    )

    expect(await screen.findByText(/No saved result named/)).toBeInTheDocument()
  })
})
