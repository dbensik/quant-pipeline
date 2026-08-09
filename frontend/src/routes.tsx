/**
 * routes.tsx
 *
 * One declaration of the app's pages, shared by the router and the navigation
 * bar. Defining them twice is how a nav link and its route drift apart.
 *
 * Phase 5 — React pages for the ported routers
 */

import type { ComponentType } from 'react'
import { LineChart, Star } from 'lucide-react'

import { DashboardPage } from '@/pages/DashboardPage'
import { WatchlistsPage } from '@/pages/WatchlistsPage'

export interface RouteDef {
  path: string
  label: string
  icon: ComponentType<{ className?: string }>
  element: ComponentType
  /** Shown in the page header under the title. */
  blurb: string
}

export const routes: RouteDef[] = [
  {
    path: '/',
    label: 'Chart & Backtest',
    icon: LineChart,
    element: DashboardPage,
    blurb: 'Price history, signal overlay and a single-symbol backtest.',
  },
  {
    path: '/watchlists',
    label: 'Watchlists',
    icon: Star,
    element: WatchlistsPage,
    blurb: 'Named lists of tickers.',
  },
]
