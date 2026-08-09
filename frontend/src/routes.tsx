/**
 * routes.tsx
 *
 * One declaration of the app's pages, shared by the router and the navigation
 * bar. Defining them twice is how a nav link and its route drift apart.
 *
 * Phase 5 — React pages for the ported routers
 */

import type { ComponentType } from 'react'
import {
  BarChart3,
  Briefcase,
  LineChart,
  Newspaper,
  SlidersHorizontal,
  Star,
} from 'lucide-react'

import { ComparePage } from '@/pages/ComparePage'
import { DashboardPage } from '@/pages/DashboardPage'
import { OptimizePage } from '@/pages/OptimizePage'
import { PortfoliosPage } from '@/pages/PortfoliosPage'
import { ResearchPage } from '@/pages/ResearchPage'
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
    path: '/compare',
    label: 'Compare',
    icon: BarChart3,
    element: ComparePage,
    blurb: 'Run several strategies on one symbol and rank them.',
  },
  {
    path: '/optimize',
    label: 'Optimize',
    icon: SlidersHorizontal,
    element: OptimizePage,
    blurb: 'Grid-search strategy parameters, or Monte Carlo portfolio weights.',
  },
  {
    path: '/portfolios',
    label: 'Portfolios',
    icon: Briefcase,
    element: PortfoliosPage,
    blurb: 'Trade log, derived positions and P&L, rebalancing previews.',
  },
  {
    path: '/research',
    label: 'Research',
    icon: Newspaper,
    element: ResearchPage,
    blurb: 'Company profile, financial statements and news.',
  },
  {
    path: '/watchlists',
    label: 'Watchlists',
    icon: Star,
    element: WatchlistsPage,
    blurb: 'Named lists of tickers.',
  },
]
