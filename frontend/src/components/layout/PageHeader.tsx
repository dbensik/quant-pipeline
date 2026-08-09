/**
 * components/layout/PageHeader.tsx
 * Title + one-line description, shared by every page.
 *
 * Phase 5 — React pages for the ported routers
 */

import type { ReactNode } from 'react'

export function PageHeader({
  title,
  blurb,
  actions,
}: {
  title: string
  blurb?: string
  /** Buttons aligned to the right of the title. */
  actions?: ReactNode
}) {
  return (
    <div className="mb-6 flex items-start justify-between gap-4">
      <div>
        <h2 className="text-2xl font-semibold tracking-tight">{title}</h2>
        {blurb ? (
          <p className="mt-1 text-sm text-muted-foreground">{blurb}</p>
        ) : null}
      </div>
      {actions ? <div className="flex shrink-0 gap-2">{actions}</div> : null}
    </div>
  )
}
