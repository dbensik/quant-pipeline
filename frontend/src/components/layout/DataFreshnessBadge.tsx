/**
 * components/layout/DataFreshnessBadge.tsx
 *
 * The age of the newest stored bar, in the header of every page.
 *
 * Price data ended 2025-07-15 and every backtest ran on it for a year with
 * nothing on screen saying so. Then the 06:00 maintenance job aborted four
 * mornings running (2026-08-28..31) and the dashboard looked identical. This
 * badge is the fix for both: staleness is visible instead of silent.
 *
 * It links to the Data page, where the per-class and per-asset breakdown
 * lives; the badge itself stays to one line.
 */

import { Link } from 'react-router-dom'

import { useDataFreshness } from '@/api/queries'
import { Badge } from '@/components/ui/badge'
import { summariseDataAge } from '@/lib/dataAge'

export function DataFreshnessBadge() {
  const { data, error, isLoading } = useDataFreshness()

  if (isLoading) {
    return <Badge variant="secondary">Checking data age…</Badge>
  }

  const summary = summariseDataAge(data, error)
  const variant = summary.tone === 'ok' ? 'secondary' : 'destructive'

  return (
    <Badge
      variant={variant}
      title={summary.detail}
      data-tone={summary.tone}
      render={<Link to="/data" aria-label={`Data freshness: ${summary.label}`} />}
    >
      {summary.label}
    </Badge>
  )
}
