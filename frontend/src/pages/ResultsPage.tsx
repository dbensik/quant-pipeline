/**
 * pages/ResultsPage.tsx
 * Saved analysis results.
 *
 * Replaces the Streamlit load/save controls. Results are stored as JSON, not
 * pickle — unpickling a caller-named file over HTTP is arbitrary code
 * execution, and a pickle is unreadable here anyway.
 *
 * DataFrames arrive in pandas' `orient="split"` layout ({index, columns,
 * data}), which is rendered as a table.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useState } from 'react'
import { Trash2 } from 'lucide-react'

import type { ApiError } from '@/api/client'
import { useDeleteResult, useResult, useResults } from '@/api/queries'
import { PageHeader } from '@/components/layout/PageHeader'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'

interface SplitFrame {
  index: (string | number)[]
  columns: string[]
  data: (number | string | null)[][]
}

/** pandas `orient="split"` — the layout the API writes and api_client read. */
function isSplitFrame(value: unknown): value is SplitFrame {
  return (
    typeof value === 'object' &&
    value !== null &&
    'index' in value &&
    'columns' in value &&
    'data' in value
  )
}

function FrameTable({ frame, limit = 50 }: { frame: SplitFrame; limit?: number }) {
  const rows = frame.data.slice(0, limit)
  return (
    <div>
      <div className="max-h-96 overflow-auto rounded border">
        <table className="w-full text-xs">
          <thead className="sticky top-0 border-b bg-background text-left uppercase text-muted-foreground">
            <tr>
              <th className="px-2 py-1.5" />
              {frame.columns.map((column) => (
                <th key={column} className="px-2 py-1.5 text-right">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className="border-b last:border-0">
                <td className="px-2 py-1 text-muted-foreground">
                  {String(frame.index[i]).slice(0, 19)}
                </td>
                {row.map((cell, j) => (
                  <td key={j} className="px-2 py-1 text-right tabular-nums">
                    {typeof cell === 'number' ? cell.toFixed(4) : String(cell ?? '—')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {frame.data.length > limit ? (
        // Said rather than silently truncated.
        <p className="mt-1 text-xs text-muted-foreground">
          Showing {limit} of {frame.data.length} rows.
        </p>
      ) : null}
    </div>
  )
}

function Payload({ value, name }: { value: unknown; name?: string }) {
  if (isSplitFrame(value)) {
    return (
      <div className="space-y-1">
        {name ? <h4 className="text-sm font-medium">{name}</h4> : null}
        <FrameTable frame={value} />
      </div>
    )
  }
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    return (
      <div className="space-y-3">
        {Object.entries(value as Record<string, unknown>).map(([key, child]) => (
          <Payload key={key} value={child} name={key} />
        ))}
      </div>
    )
  }
  return (
    <p className="text-sm">
      {name ? <span className="text-muted-foreground">{name}: </span> : null}
      {String(value)}
    </p>
  )
}

export function ResultsPage() {
  const { data: results, isLoading } = useResults()
  const [selected, setSelected] = useState<string | null>(null)
  const payload = useResult(selected)
  const remove = useDeleteResult()

  return (
    <div>
      <PageHeader
        title="Saved results"
        blurb="Stored as JSON. Legacy pickles are converted by scripts/convert_pickled_results.py."
      />

      <div className="grid gap-6 lg:grid-cols-[300px_1fr]">
        <Card className="h-fit">
          <CardHeader>
            <CardTitle className="text-base">Files</CardTitle>
            <CardDescription>{results?.length ?? 0} saved</CardDescription>
          </CardHeader>
          <CardContent className="space-y-1">
            {isLoading ? <Skeleton className="h-9 w-full" /> : null}
            {results?.map((file) => (
              <div key={file.name} className="flex items-center gap-1">
                <Button
                  variant={selected === file.name ? 'secondary' : 'ghost'}
                  className="flex-1 justify-start"
                  onClick={() => setSelected(file.name)}
                >
                  <span className="truncate text-xs">{file.name}</span>
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={`Delete ${file.name}`}
                  onClick={() =>
                    remove.mutate(file.name, {
                      onSuccess: () =>
                        setSelected((c) => (c === file.name ? null : c)),
                    })
                  }
                >
                  <Trash2 className="h-4 w-4 text-muted-foreground" />
                </Button>
              </div>
            ))}
            {results?.length === 0 ? (
              <p className="px-3 py-2 text-sm text-muted-foreground">
                Nothing saved yet.
              </p>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {selected ?? 'Select a result'}
            </CardTitle>
            {selected && results ? (
              <CardDescription>
                {(
                  (results.find((r) => r.name === selected)?.size_bytes ?? 0) / 1024
                ).toFixed(0)}{' '}
                KB · saved{' '}
                {new Date(
                  results.find((r) => r.name === selected)?.modified_at ?? '',
                ).toLocaleString()}
              </CardDescription>
            ) : null}
          </CardHeader>
          <CardContent>
            {!selected ? (
              <p className="text-sm text-muted-foreground">
                Choose a file to view it.
              </p>
            ) : payload.isLoading ? (
              <Skeleton className="h-40 w-full" />
            ) : payload.error ? (
              <Alert variant="destructive">
                <AlertDescription>
                  {(payload.error as ApiError).detail}
                </AlertDescription>
              </Alert>
            ) : (
              <Payload value={payload.data} />
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
