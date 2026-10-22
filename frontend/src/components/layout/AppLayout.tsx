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
import { DataFreshnessBadge } from '@/components/layout/DataFreshnessBadge'
import { Badge } from '@/components/ui/badge'
import { routeGroups, routes } from '@/routes'
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

// Sections wrap onto a second line as whole units rather than scrolling
// sideways: fourteen pages do not fit one row at max-w-7xl. Every section has
// a left divider; the row is shifted left under a clip so the divider of
// whichever section opens a line is hidden.
function Navigation() {
  return (
    <nav className="border-b bg-muted/30">
      <div className="mx-auto max-w-7xl px-6">
        <div className="overflow-hidden">
          <div className="-ml-3 flex flex-wrap">
            {routeGroups.map((group) => (
              <div
                key={group}
                role="group"
                aria-label={group}
                className="flex items-center border-l pl-3 pr-3"
              >
                <span className="pr-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground/70">
                  {group}
                </span>
                {routes
                  .filter((r) => r.group === group)
                  .map(({ path, label, icon: Icon }) => (
                    <NavLink
                      key={path}
                      to={path}
                      // `end` only on the index route, or every path would match it and
                      // "Chart & Backtest" would render active on every page.
                      end={path === '/'}
                      className={({ isActive }) =>
                        cn(
                          'flex shrink-0 items-center gap-1.5 border-b-2 px-2.5 py-2.5 text-sm transition-colors',
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
            ))}
          </div>
        </div>
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
          <div className="flex items-center gap-2">
            <DataFreshnessBadge />
            <ApiStatus />
          </div>
        </div>
      </header>

      <Navigation />

      <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
    </div>
  )
}
