import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'

import { Caveat } from './Caveat'

const TEXT = 'Measured at its default parameters on 428 names: beat buy and hold on 11%.'

describe('Caveat', () => {
  it('starts collapsed, with the text on the page once', () => {
    const { container } = render(<Caveat text={TEXT} />)
    expect(container.querySelector('details')).not.toHaveAttribute('open')
    expect(screen.getAllByText(TEXT)).toHaveLength(1)
    expect(screen.getByText('Evidence and limits:')).toBeInTheDocument()
  })

  it('opens on a click', async () => {
    const { container } = render(<Caveat text={TEXT} />)
    await userEvent.click(screen.getByText(TEXT))
    expect(container.querySelector('details')).toHaveAttribute('open')
  })

  it('is not an alarm', () => {
    render(<Caveat text={TEXT} />)
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('renders nothing without text', () => {
    const { container } = render(<Caveat text={null} />)
    expect(container).toBeEmptyDOMElement()
  })
})
