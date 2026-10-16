/**
 * Vitest setup — runs before every test file.
 *
 * Phase 4 — React frontend
 */

import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

import { useAppStore } from '@/store/useAppStore'

// The store's state as the app starts, restored after every test. Pages share
// one ticker through it (since 2026-10-15), so a test that looks up MSFT would
// otherwise hand MSFT to the next test that expects the AAPL default.
const initialAppState = useAppStore.getState()

// React Testing Library does not auto-clean without globals enabled, and a
// leaked DOM between tests makes getBy* queries match the previous test's
// output — a failure mode that looks like a passing test.
afterEach(() => {
  cleanup()
  useAppStore.setState(initialAppState, true)
})
