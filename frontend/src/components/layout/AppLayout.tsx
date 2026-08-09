/**
 * components/layout/AppLayout.tsx
 * App shell: header with API status, primary navigation, and a content slot.
 *
 * Phase 5 — React pages for the ported routers
 */

import type { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'

import { useHealth } from '@/api/queries'
import { API_BASE_URL } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { routes } from '@/routes'
import { cn } from '@/lib/utils'

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

function Navigation() {
  return (
    <nav className="border-b bg-muted/30">
      <div className="mx-auto flex max-w-7xl gap-1 overflow-x-auto px-6">
        {routes.map(({ path, label, icon: Icon }) => (
          <NavLink
            key={path}
            to={path}
            // `end` only on the index route, or every path would match it and
            // "Chart & Backtest" would render active on every page.
            end={path === '/'}
            className={({ isActive }) =>
              cn(
                'flex shrink-0 items-center gap-2 border-b-2 px-3 py-2.5 text-sm transition-colors',
                isActive
                  ? 'border-primary font-medium text-foreground'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )
            }
          >
            <Icon className="h-4 w-4" />
            {label}
          </NavLink>
        ))}
      </div>
    </nav>
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
              Research dashboard
            </p>
          </div>
          <ApiStatus />
        </div>
      </header>

      <Navigation />

      <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
    </div>
  )
}
