/**
 * App.tsx
 * Root component: providers + layout + page.
 *
 * Phase 4 — React frontend
 */

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'

import { AppLayout } from '@/components/layout/AppLayout'
import { DashboardPage } from '@/pages/DashboardPage'

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
      <AppLayout>
        <DashboardPage />
      </AppLayout>
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
}
