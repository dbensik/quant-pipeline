import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it } from 'vitest'

import type { StrategySchema } from '@/api/client'
import { queryKeys } from '@/api/queries'
import { useAppStore } from '@/store/useAppStore'
import { StrategySelector } from './StrategySelector'

const MA_CROSSOVER: StrategySchema = {
  id: 'ma_crossover',
  display_name: 'Moving Average Crossover',
  description: 'Long when the short moving average is above the long one.',
  input_contract: 'single',
  caveat: null,
  params: [
    {
      name: 'short_window',
      type: 'int',
      default: 40,
      label: 'Short window',
      description: 'Must be smaller than the long window',
      minimum: 1,
      maximum: 500,
    },
    {
      name: 'long_window',
      type: 'int',
      default: 100,
      label: 'Long window',
      description: 'Must be larger than the short window',
      minimum: 2,
      maximum: 1000,
    },
  ],
}

const RANDOM_FOREST: StrategySchema = {
  id: 'ml_random_forest',
  display_name: 'Random Forest (ML)',
  description: 'Random-forest classifier over lagged returns.',
  input_contract: 'single',
  caveat: 'Known look-ahead bias: the model trains on the full history.',
  params: [],
}

const initialStore = useAppStore.getState()

/**
 * Seeds the strategy catalogue directly into the query cache. No network layer
 * is needed — the component reads it through useStrategies, and seeding is
 * both faster and less brittle than intercepting fetch.
 */
function renderWith(strategies: StrategySchema[]) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  queryClient.setQueryData(queryKeys.strategies('single'), {
    count: strategies.length,
    strategies,
  })

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<StrategySelector />, { wrapper })
}

beforeEach(() => {
  useAppStore.setState(initialStore, true)
})

describe('StrategySelector — the trigger shows a human name', () => {
  it('displays the display name, not the registry id', async () => {
    // THE regression. Radix renders the raw `value` unless SelectValue is given
    // explicit children, so the trigger read "ma_crossover" instead of "Moving
    // Average Crossover". The symbol picker hid this because there the value IS
    // the label.
    renderWith([MA_CROSSOVER])

    await waitFor(() => {
      expect(screen.getByText('Moving Average Crossover')).toBeInTheDocument()
    })
    expect(screen.queryByText('ma_crossover')).not.toBeInTheDocument()
  })
})

describe('StrategySelector — auto-selection', () => {
  it('selects the first strategy so the panel is usable without a click', async () => {
    renderWith([MA_CROSSOVER, RANDOM_FOREST])
    await waitFor(() => {
      expect(useAppStore.getState().strategyId).toBe('ma_crossover')
    })
  })

  it('seeds parameters from the schema', async () => {
    // Registry defaults, not hardcoded ones — this is what makes a new strategy
    // appear with working controls and no frontend change.
    renderWith([MA_CROSSOVER])
    await waitFor(() => {
      expect(useAppStore.getState().strategyParams).toEqual({
        short_window: 40,
        long_window: 100,
      })
    })
  })
})

describe('StrategySelector — parameter controls come from the schema', () => {
  it('renders a labelled control per declared parameter', async () => {
    renderWith([MA_CROSSOVER])
    await waitFor(() => {
      expect(screen.getByLabelText('Short window')).toBeInTheDocument()
    })
    expect(screen.getByLabelText('Long window')).toBeInTheDocument()
  })

  it('applies the declared type and bounds', async () => {
    renderWith([MA_CROSSOVER])
    const input = await screen.findByLabelText('Short window')
    expect(input).toHaveAttribute('type', 'number')
    expect(input).toHaveAttribute('min', '1')
    expect(input).toHaveAttribute('max', '500')
  })

  it('shows the declared default as the value', async () => {
    renderWith([MA_CROSSOVER])
    expect(await screen.findByLabelText('Short window')).toHaveValue(40)
  })

  it('says so when a strategy takes no parameters', async () => {
    renderWith([RANDOM_FOREST])
    expect(
      await screen.findByText(/takes no parameters/i),
    ).toBeInTheDocument()
  })
})

describe('StrategySelector — caveats', () => {
  it('surfaces a known limitation', async () => {
    // ml_random_forest has look-ahead bias. Hiding the caveat would present
    // un-achievable results as trustworthy.
    renderWith([RANDOM_FOREST])
    expect(await screen.findByText(/Known limitation/i)).toBeInTheDocument()
    expect(screen.getByText(/look-ahead bias/i)).toBeInTheDocument()
  })

  it('shows no warning for a sound strategy', async () => {
    renderWith([MA_CROSSOVER])
    await screen.findByLabelText('Short window')
    expect(screen.queryByText(/Known limitation/i)).not.toBeInTheDocument()
  })
})
