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
  Calculator,
  ChartCandlestick,
  Briefcase,
  Database,
  Filter,
  LineChart,
  Newspaper,
  PieChart,
  Save,
  Sigma,
  SlidersHorizontal,
  Star,
  TrendingUp,
} from 'lucide-react'

import { AllocationPage } from '@/pages/AllocationPage'
import { ComparePage } from '@/pages/ComparePage'
import { DataPage } from '@/pages/DataPage'
import { DashboardPage } from '@/pages/DashboardPage'
import { OptimizePage } from '@/pages/OptimizePage'
import { OptionsPage } from '@/pages/OptionsPage'
import { PayoffPage } from '@/pages/PayoffPage'
import { ValuationPage } from '@/pages/ValuationPage'
import { ScreenersPage } from '@/pages/ScreenersPage'
import { StatisticsPage } from '@/pages/StatisticsPage'
import { PortfoliosPage } from '@/pages/PortfoliosPage'
import { ResearchPage } from '@/pages/ResearchPage'
import { ResultsPage } from '@/pages/ResultsPage'
import { WatchlistsPage } from '@/pages/WatchlistsPage'

/**
 * The navigation's sections, in the order work flows through them: get the
 * data, find and study names, test strategies on them, then options, then
 * trade and keep what was learned. `routes` is listed in the same order.
 */
export const routeGroups = ['Data', 'Research', 'Strategies', 'Options', 'Track'] as const

export type RouteGroup = (typeof routeGroups)[number]

export interface RouteDef {
  path: string
  group: RouteGroup
  label: string
  icon: ComponentType<{ className?: string }>
  element: ComponentType
  /** Shown in the page header under the title. */
  blurb: string
}

export const routes: RouteDef[] = [
  {
    path: '/data',
    group: 'Data',
    label: 'Data',
    icon: Database,
    element: DataPage,
    blurb: 'Ingest price bars, register tickers, browse index constituents.',
  },
  {
    path: '/watchlists',
    group: 'Data',
    label: 'Watchlists',
    icon: Star,
    element: WatchlistsPage,
    blurb: 'Named lists of tickers.',
  },
  {
    path: '/screeners',
    group: 'Research',
    label: 'Screeners',
    icon: Filter,
    element: ScreenersPage,
    blurb: 'Filter the universe by momentum, volatility or fundamentals.',
  },
  {
    path: '/research',
    group: 'Research',
    label: 'Research',
    icon: Newspaper,
    element: ResearchPage,
    blurb: 'Company profile, financial statements and news.',
  },
  {
    path: '/valuation',
    group: 'Research',
    label: 'Valuation',
    icon: Calculator,
    element: ValuationPage,
    blurb: 'Discounted cash flow from SEC filings, as of any date, with every input and its source.',
  },
  {
    path: '/statistics',
    group: 'Research',
    label: 'Statistics',
    icon: Sigma,
    element: StatisticsPage,
    blurb: 'Stationarity, cointegration, alpha/beta and PCA.',
  },
  {
    path: '/',
    group: 'Strategies',
    label: 'Chart & Backtest',
    icon: LineChart,
    element: DashboardPage,
    blurb: 'Price history, signal overlay and a single-symbol backtest.',
  },
  {
    path: '/compare',
    group: 'Strategies',
    label: 'Compare',
    icon: BarChart3,
    element: ComparePage,
    blurb: 'Run several strategies on one symbol and rank them.',
  },
  {
    path: '/optimize',
    group: 'Strategies',
    label: 'Optimize',
    icon: SlidersHorizontal,
    element: OptimizePage,
    blurb: 'Grid-search strategy parameters, or Monte Carlo portfolio weights.',
  },
  {
    path: '/allocation',
    group: 'Strategies',
    label: 'Allocation',
    icon: PieChart,
    element: AllocationPage,
    blurb: 'A multi-asset allocation rule over a basket, beside the same basket equal-weighted.',
  },
  {
    path: '/options',
    group: 'Options',
    label: 'Options',
    icon: ChartCandlestick,
    element: OptionsPage,
    blurb: 'Implied-volatility smiles and term structure from the archived option chains, and a pricer.',
  },
  {
    path: '/payoffs',
    group: 'Options',
    label: 'Payoffs',
    icon: TrendingUp,
    element: PayoffPage,
    blurb: 'Multi-leg option positions and their profit at expiry, priced from the archived quotes.',
  },
  {
    path: '/portfolios',
    group: 'Track',
    label: 'Portfolios',
    icon: Briefcase,
    element: PortfoliosPage,
    blurb: 'Trade log, derived positions and P&L, rebalancing previews.',
  },
  {
    path: '/results',
    group: 'Track',
    label: 'Results',
    icon: Save,
    element: ResultsPage,
    blurb: 'Saved analysis results.',
  },
]
