/**
 * lib/useDebounced.ts
 * Delay a value until it stops changing.
 *
 * Phase 4 — React frontend
 */

import { useEffect, useState } from 'react'

/**
 * Returns `value` after it has been stable for `delayMs`.
 *
 * Used for the signal overlay's parameters. Typing "0.5" into a number field
 * passes through the intermediate value 0, which several strategies reject
 * ("threshold must be positive") — so an undebounced query fires a request per
 * keystroke and flashes a 422 the user never actually asked for. Debouncing
 * means only the value they settled on is ever requested.
 *
 * The value is compared by JSON, not identity, so callers can pass a freshly
 * built params object each render without defeating the debounce.
 */
export function useDebounced<T>(value: T, delayMs = 400): T {
  const [debounced, setDebounced] = useState(value)
  const serialised = JSON.stringify(value)

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs)
    return () => clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [serialised, delayMs])

  return debounced
}
