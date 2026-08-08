import { act, renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { useDebounced } from './useDebounced'

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('useDebounced', () => {
  it('returns the initial value immediately', () => {
    const { result } = renderHook(() => useDebounced('a', 400))
    expect(result.current).toBe('a')
  })

  it('withholds a new value until it settles', () => {
    const { result, rerender } = renderHook(({ value }) => useDebounced(value, 400), {
      initialProps: { value: 'a' },
    })

    rerender({ value: 'b' })
    expect(result.current).toBe('a')

    act(() => {
      vi.advanceTimersByTime(400)
    })
    expect(result.current).toBe('b')
  })

  it('emits only the final value of a rapid burst', () => {
    // THE regression this exists for. Typing "0.5" into a number field passes
    // through the intermediate value 0, which several strategies reject as
    // non-positive — the undebounced version fired a request per keystroke and
    // flashed a 422 the user never asked for.
    const { result, rerender } = renderHook(
      ({ value }) => useDebounced(value, 400),
      { initialProps: { value: { threshold: 1.5 } } },
    )

    for (const threshold of [0, 0.5, 0.75] as const) {
      rerender({ value: { threshold } })
      act(() => {
        vi.advanceTimersByTime(100) // shorter than the delay — nothing settles
      })
      expect(result.current).toEqual({ threshold: 1.5 })
    }

    act(() => {
      vi.advanceTimersByTime(400)
    })
    expect(result.current).toEqual({ threshold: 0.75 })
  })

  it('compares objects by value, not identity', () => {
    // Callers build a fresh params object each render. Comparing by reference
    // would restart the timer every render and never settle.
    const { result, rerender } = renderHook(
      ({ value }) => useDebounced(value, 400),
      { initialProps: { value: { window: 20 } } },
    )

    rerender({ value: { window: 20 } }) // equal, new object
    act(() => {
      vi.advanceTimersByTime(400)
    })
    expect(result.current).toEqual({ window: 20 })

    rerender({ value: { window: 30 } }) // genuinely different
    act(() => {
      vi.advanceTimersByTime(400)
    })
    expect(result.current).toEqual({ window: 30 })
  })
})
