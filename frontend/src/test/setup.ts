/**
 * Vitest setup — runs before every test file.
 *
 * Phase 4 — React frontend
 */

import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'

// React Testing Library does not auto-clean without globals enabled, and a
// leaked DOM between tests makes getBy* queries match the previous test's
// output — a failure mode that looks like a passing test.
afterEach(() => {
  cleanup()
})
