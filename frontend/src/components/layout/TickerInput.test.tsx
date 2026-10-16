import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { TickerInput } from './TickerInput'

describe('TickerInput', () => {
  it('commits on Enter, upper-cased', async () => {
    const onCommit = vi.fn()
    render(<TickerInput id="t" value="AAPL" onCommit={onCommit} suggestions={['AAPL', 'MSFT']} />)
    const box = screen.getByRole('combobox')
    await userEvent.clear(box)
    await userEvent.type(box, 'msft{Enter}')
    expect(onCommit).toHaveBeenCalledWith('MSFT')
  })

  it('never commits while typing — "AA" on the way to "AAPL" is not a ticker change', async () => {
    const onCommit = vi.fn()
    render(<TickerInput id="t" value="MSFT" onCommit={onCommit} suggestions={['AA', 'AAPL']} />)
    const box = screen.getByRole('combobox')
    await userEvent.clear(box)
    await userEvent.type(box, 'AA')
    expect(onCommit).not.toHaveBeenCalled()
  })

  it('without a button, leaving the field commits', async () => {
    const onCommit = vi.fn()
    render(<TickerInput id="t" value="AAPL" onCommit={onCommit} suggestions={[]} />)
    const box = screen.getByRole('combobox')
    await userEvent.clear(box)
    await userEvent.type(box, 'JPM')
    fireEvent.blur(box)
    expect(onCommit).toHaveBeenCalledWith('JPM')
  })

  it('offers the registry symbols as suggestions', () => {
    const { container } = render(
      <TickerInput id="t" value="AAPL" onCommit={vi.fn()} suggestions={['AAPL', 'MSFT']} />,
    )
    const options = [...container.querySelectorAll('datalist#t-suggestions option')]
    expect(options.map((o) => o.getAttribute('value'))).toEqual(['AAPL', 'MSFT'])
  })

  it('shows a ticker chosen elsewhere', () => {
    const { rerender } = render(<TickerInput id="t" value="AAPL" onCommit={vi.fn()} suggestions={[]} />)
    rerender(<TickerInput id="t" value="NVDA" onCommit={vi.fn()} suggestions={[]} />)
    expect(screen.getByRole('combobox')).toHaveValue('NVDA')
  })
})
