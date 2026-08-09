/**
 * pages/WatchlistsPage.tsx
 * Named lists of tickers — create, edit, delete.
 *
 * Ported from the Streamlit sidebar's watchlist CRUD, which read and wrote
 * `watchlists.json`. Those now live in the database behind /api/v1/watchlists.
 *
 * Phase 5 — React pages for the ported routers
 */

import { useEffect, useState } from 'react'
import { Plus, Trash2, X } from 'lucide-react'

import { ApiError } from '@/api/client'
import type { WatchlistOut } from '@/api/client'
import {
  useAssets,
  useDeleteWatchlist,
  useSaveWatchlist,
  useWatchlists,
} from '@/api/queries'
import { PageHeader } from '@/components/layout/PageHeader'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'

const NEW = '__new__'

export function WatchlistsPage() {
  const { data: watchlists, isLoading, isError, error } = useWatchlists()
  const { data: assets } = useAssets()
  const save = useSaveWatchlist()
  const remove = useDeleteWatchlist()

  const [selected, setSelected] = useState<string>(NEW)
  const [name, setName] = useState('')
  const [symbols, setSymbols] = useState<string[]>([])
  const [entry, setEntry] = useState('')

  // Load the chosen list into the editor. Keyed on `selected` alone: adding
  // `watchlists` would reset the form under the user every time the query
  // refetched, discarding edits in progress.
  useEffect(() => {
    if (selected === NEW) {
      setName('')
      setSymbols([])
      return
    }
    const found = watchlists?.find((w) => w.name === selected)
    if (found) {
      setName(found.name)
      setSymbols([...found.symbols])
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected])

  const known = new Set((assets?.assets ?? []).map((a) => a.symbol))

  function addSymbol(raw: string) {
    const symbol = raw.toUpperCase().trim()
    if (!symbol) return
    // De-duplicated here as well as server-side, so the chip does not flicker
    // in and out on save.
    setSymbols((current) =>
      current.includes(symbol) ? current : [...current, symbol],
    )
    setEntry('')
  }

  function onSave() {
    if (!name.trim()) return
    save.mutate(
      { name: name.trim(), symbols },
      { onSuccess: (saved: WatchlistOut) => setSelected(saved.name) },
    )
  }

  function onDelete(target: string) {
    remove.mutate(target, {
      onSuccess: () => {
        if (selected === target) setSelected(NEW)
      },
    })
  }

  const mutationError = (save.error ?? remove.error) as ApiError | null

  return (
    <div>
      <PageHeader
        title="Watchlists"
        blurb="Named lists of tickers. Saving replaces the whole list."
      />

      {isError ? (
        <Alert variant="destructive" className="mb-6">
          <AlertDescription>{(error as ApiError)?.detail}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-6 md:grid-cols-[280px_1fr]">
        {/* -- list ---------------------------------------------------- */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Your lists</CardTitle>
            <CardDescription>
              {watchlists ? `${watchlists.length} saved` : '…'}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-1">
            <Button
              variant={selected === NEW ? 'secondary' : 'ghost'}
              className="w-full justify-start"
              onClick={() => setSelected(NEW)}
            >
              <Plus className="mr-2 h-4 w-4" />
              New watchlist
            </Button>

            {isLoading ? (
              <>
                <Skeleton className="h-9 w-full" />
                <Skeleton className="h-9 w-full" />
              </>
            ) : null}

            {watchlists?.map((list) => (
              <div key={list.name} className="flex items-center gap-1">
                <Button
                  variant={selected === list.name ? 'secondary' : 'ghost'}
                  className="flex-1 justify-between"
                  onClick={() => setSelected(list.name)}
                >
                  <span className="truncate">{list.name}</span>
                  <Badge variant="outline">{list.symbols.length}</Badge>
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={`Delete ${list.name}`}
                  onClick={() => onDelete(list.name)}
                  disabled={remove.isPending}
                >
                  <Trash2 className="h-4 w-4 text-muted-foreground" />
                </Button>
              </div>
            ))}

            {watchlists?.length === 0 ? (
              <p className="px-3 py-2 text-sm text-muted-foreground">
                No watchlists yet.
              </p>
            ) : null}
          </CardContent>
        </Card>

        {/* -- editor -------------------------------------------------- */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">
              {selected === NEW ? 'New watchlist' : `Editing “${selected}”`}
            </CardTitle>
            <CardDescription>
              Saving replaces the whole list — removing a chip and saving
              removes that ticker.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="watchlist-name">Name</Label>
              <Input
                id="watchlist-name"
                value={name}
                placeholder="e.g. MAG7"
                onChange={(e) => setName(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="watchlist-symbol">Add ticker</Label>
              <div className="flex gap-2">
                <Input
                  id="watchlist-symbol"
                  value={entry}
                  placeholder="AAPL"
                  list="known-symbols"
                  onChange={(e) => setEntry(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault()
                      addSymbol(entry)
                    }
                  }}
                />
                <Button type="button" onClick={() => addSymbol(entry)}>
                  Add
                </Button>
              </div>
              <datalist id="known-symbols">
                {(assets?.assets ?? []).slice(0, 2000).map((a) => (
                  <option key={a.symbol} value={a.symbol} />
                ))}
              </datalist>
            </div>

            <div className="space-y-2">
              <Label>Tickers ({symbols.length})</Label>
              <div className="flex min-h-[44px] flex-wrap gap-2 rounded-md border p-2">
                {symbols.length === 0 ? (
                  <span className="px-1 py-0.5 text-sm text-muted-foreground">
                    None yet.
                  </span>
                ) : null}
                {symbols.map((symbol) => (
                  <Badge
                    key={symbol}
                    variant={known.has(symbol) ? 'secondary' : 'outline'}
                    className="gap-1"
                    // A ticker with no bars stored is still allowed — it may be
                    // registered later — but it is worth flagging rather than
                    // letting a typo sit silently in a list.
                    title={
                      known.has(symbol)
                        ? undefined
                        : `${symbol} is not in the asset registry`
                    }
                  >
                    {known.has(symbol) ? null : '⚠ '}
                    {symbol}
                    <button
                      type="button"
                      aria-label={`Remove ${symbol}`}
                      onClick={() =>
                        setSymbols((c) => c.filter((s) => s !== symbol))
                      }
                    >
                      <X className="h-3 w-3" />
                    </button>
                  </Badge>
                ))}
              </div>
            </div>

            {mutationError ? (
              <Alert variant="destructive">
                <AlertDescription>{mutationError.detail}</AlertDescription>
              </Alert>
            ) : null}

            <div className="flex items-center gap-3">
              <Button onClick={onSave} disabled={!name.trim() || save.isPending}>
                {save.isPending ? 'Saving…' : 'Save watchlist'}
              </Button>
              {save.isSuccess ? (
                <span className="text-sm text-muted-foreground">Saved.</span>
              ) : null}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
