/**
 * lib/dataAge.ts
 *
 * Pure: turn the freshness response into what the header badge shows.
 *
 * Kept out of the component so the thresholds are testable without a DOM,
 * and so the two places that render data age (header badge, Data page card)
 * cannot disagree about what "stale" means.
 *
 * The thresholds are about the WHOLE store's newest bar, which is what the
 * badge leads with. A Monday shows Friday's equity bar (3 days) and that is
 * healthy; crypto trades daily so the store-wide newest bar is usually
 * yesterday. Beyond `maxAgeDays` — the server's own threshold, echoed back in
 * the response so both sides use one number — an ingest has been missed.
 */

import type { DataFreshnessResponse } from '@/api/client'

export type DataAgeTone = 'ok' | 'stale' | 'unknown'

export interface DataAgeSummary {
  tone: DataAgeTone
  /** Short text for the badge, e.g. "Bars to 2026-09-13 · 1d". */
  label: string
  /** Longer text for the tooltip. */
  detail: string
}

/** `YYYY-MM-DD` from an ISO timestamp, without a timezone shift. */
export function isoDate(ts: string | null | undefined): string {
  return ts ? ts.slice(0, 10) : '—'
}

export function summariseDataAge(
  data: DataFreshnessResponse | undefined,
  error: unknown,
): DataAgeSummary {
  if (error || !data) {
    // Not "loading" styling: an error here is the failure mode this badge
    // exists for, and a neutral grey would read as "fine, just slow".
    return {
      tone: 'unknown',
      label: 'Data age unknown',
      detail: 'Could not read the newest stored bar from the API.',
    }
  }
  if (data.newest_bar == null || data.age_days == null) {
    return {
      tone: 'stale',
      label: 'No bars stored',
      detail: `${data.assets} registered assets, none with price data.`,
    }
  }

  const age = data.age_days
  const storeStale = age > data.max_age_days
  const label = `Bars to ${isoDate(data.newest_bar)} · ${age}d`
  const staleNote =
    data.stale > 0
      ? `${data.stale} of ${data.assets} assets have no bar in the last ${data.max_age_days} days.`
      : `All ${data.assets} assets have a bar in the last ${data.max_age_days} days.`

  if (storeStale) {
    return {
      tone: 'stale',
      label,
      detail: `Newest stored bar is ${age} days old — the 06:00 ingest has probably not run. ${staleNote}`,
    }
  }
  return {
    // Some assets stale while the store as a whole is current is a warning
    // about those names, not about the job — still 'ok' for the badge tone,
    // and the count is in the label so it is not hidden in a tooltip.
    tone: 'ok',
    label: data.stale > 0 ? `${label} · ${data.stale} stale` : label,
    detail: staleNote,
  }
}
