/**
 * components/layout/DateRangePresets.tsx
 * 1Y / 5Y / 10Y / Max buttons for the shared date range.
 */

import { Button } from '@/components/ui/button'
import { PRESETS, matchingPreset, presetRange, todayIso } from '@/lib/dateRange'
import { useAppStore } from '@/store/useAppStore'

export function DateRangePresets() {
  const { startDate, endDate, setDateRange } = useAppStore()
  const today = todayIso()
  const active = matchingPreset(startDate, endDate, today)
  return (
    <div className="flex flex-wrap gap-1.5" role="group" aria-label="Date range presets">
      {PRESETS.map((key) => (
        <Button
          key={key}
          type="button"
          size="sm"
          variant={active === key ? 'secondary' : 'outline'}
          aria-pressed={active === key}
          onClick={() => {
            const r = presetRange(key, today)
            setDateRange(r.start, r.end)
          }}
        >
          {key}
        </Button>
      ))}
    </div>
  )
}
