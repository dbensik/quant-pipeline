/**
 * components/backtest/BacktestProgress.tsx
 * Live progress for a websocket-driven backtest.
 *
 * Phase 4 — React frontend
 */

import type { WsProgress } from '@/api/ws'

const STAGES: Array<{ key: WsProgress['stage']; label: string }> = [
  { key: 'fetching', label: 'Loading history' },
  { key: 'running', label: 'Running strategy' },
  { key: 'summarising', label: 'Computing metrics' },
]

export function BacktestProgress({ progress }: { progress: WsProgress | null }) {
  // Before the first progress message the socket is still connecting. Showing
  // 0% rather than nothing means the bar never appears to jump from absent.
  const pct = progress?.pct ?? 0
  const currentIndex = progress
    ? STAGES.findIndex((stage) => stage.key === progress.stage)
    : -1

  return (
    <div className="space-y-3" role="status" aria-live="polite">
      <div className="flex items-baseline justify-between text-sm">
        <span className="font-medium">
          {progress?.detail ?? 'Connecting…'}
        </span>
        <span className="font-mono text-xs text-muted-foreground">{pct}%</span>
      </div>

      <div
        className="h-2 w-full overflow-hidden rounded-full bg-secondary"
        role="progressbar"
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div
          className="h-full rounded-full bg-primary transition-all duration-300 ease-out"
          style={{ width: `${pct}%` }}
        />
      </div>

      <ol className="flex gap-4 text-xs">
        {STAGES.map((stage, index) => {
          const done = currentIndex > index
          const active = currentIndex === index
          return (
            <li
              key={stage.key}
              className={
                done
                  ? 'text-muted-foreground line-through'
                  : active
                    ? 'font-medium text-foreground'
                    : 'text-muted-foreground'
              }
            >
              {done ? '✓ ' : active ? '● ' : '○ '}
              {stage.label}
            </li>
          )
        })}
      </ol>
    </div>
  )
}
