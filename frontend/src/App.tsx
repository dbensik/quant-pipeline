/**
 * App.tsx
 * Root component: providers + router + layout.
 *
 * Phase 5 — React pages for the ported routers
 */

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'

import { AppLayout } from '@/components/layout/AppLayout'
import { routes } from '@/routes'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Market history is immutable once stored, so refetching because the
      // window regained focus is pure waste here.
      refetchOnWindowFocus: false,
      staleTime: 60_000,
    },
  },
})

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppLayout>
          <Routes>
            {routes.map(({ path, element: Element }) => (
              <Route key={path} path={path} element={<Element />} />
            ))}
            {/* Anything unrecognised goes home rather than rendering blank. */}
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </AppLayout>
      </BrowserRouter>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}
