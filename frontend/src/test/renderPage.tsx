/**
 * test/renderPage.tsx
 *
 * The wrapper every page test needs: a fresh QueryClient plus a router.
 *
 * Pages use NavLink/useNavigate through AppLayout and render inside <Routes>,
 * so rendering one bare throws "useRoutes() may be used only in the context of
 * a <Router>". Established once here rather than repeated per page — the same
 * reason routes.tsx declares the pages once.
 *
 * Retries are off: a test asserting an error state should not wait through
 * three attempts before the assertion can run.
 *
 * Phase 5 — React pages for the ported routers
 */

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import type { ReactElement, ReactNode } from 'react'
import { MemoryRouter } from 'react-router-dom'

export function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  })
}

export function renderPage(
  ui: ReactElement,
  options: { route?: string; client?: QueryClient } = {},
) {
  const client = options.client ?? makeQueryClient()
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[options.route ?? '/']}>
        {children}
      </MemoryRouter>
    </QueryClientProvider>
  )
  return { client, ...render(ui, { wrapper }) }
}
