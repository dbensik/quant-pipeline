/**
 * components/layout/AppLayout.tsx
 * App shell: header with API status, and a content slot.
 *
 * Phase 4 — React frontend
 */

import type { ReactNode } from 'react'

import { useHealth } from '@/api/queries'
import { API_BASE_URL } from '@/api/client'
import { Badge } from '@/components/ui/badge'

function ApiStatus() {
  const { data, isLoading, isError } = useHealth()

  if (isLoading) {
    return <Badge variant="secondary">Connecting…</Badge>
  }
  if (isError || data?.status !== 'ready') {
    return (
      <Badge variant="destructive" title={`No response from ${API_BASE_URL}`}>
        API offline
      </Badge>
    )
  }
  return (
    <Badge variant="secondary" title={API_BASE_URL}>
      API ready
    </Badge>
  )
}

export function AppLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
          <div>
            <h1 className="text-xl font-semibold tracking-tight">
              🚀 Quant Pipeline
            </h1>
            <p className="text-sm text-muted-foreground">
              Research dashboard — React frontend (Phase 4)
            </p>
          </div>
          <ApiStatus />
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
    </div>
  )
}
