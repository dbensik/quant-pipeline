/**
 * components/watchlists/WatchlistMenu.tsx
 * Save the ticker a page is showing to watchlists, without leaving the page.
 *
 * Each list is a checkbox: ticked means the list holds the ticker, and toggling
 * adds or removes that ONE ticker server side (POST/DELETE .../symbols). It
 * never re-saves a whole list from this page's copy — that copy may be stale,
 * and a PUT would drop tickers saved elsewhere since.
 *
 * A new list is created holding just this ticker. A name that already exists
 * gets the ticker added instead: a PUT there would replace its contents.
 */

import { Star } from 'lucide-react'
import { useState } from 'react'

import type { ApiError } from '@/api/client'
import { useSaveWatchlist, useToggleWatchlistSymbol, useWatchlists } from '@/api/queries'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

export function WatchlistMenu({ symbol }: { symbol: string | null | undefined }) {
  const ticker = (symbol ?? '').toUpperCase().trim()
  const [open, setOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const lists = useWatchlists()
  const toggle = useToggleWatchlistSymbol()
  const create = useSaveWatchlist()

  if (!ticker) return null

  const all = lists.data ?? []
  const holding = all.filter((w) => w.symbols.includes(ticker)).map((w) => w.name)
  const error = (toggle.error ?? create.error) as ApiError | null

  function createList() {
    const name = newName.trim()
    if (!name) return
    const onSuccess = () => setNewName('')
    if (all.some((w) => w.name === name)) {
      toggle.mutate({ name, symbol: ticker, add: true }, { onSuccess })
    } else {
      create.mutate({ name, symbols: [ticker] }, { onSuccess })
    }
  }

  return (
    <div className="relative inline-block">
      <Button
        type="button"
        size="sm"
        variant={holding.length ? 'secondary' : 'outline'}
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
      >
        <Star className={`mr-1 h-4 w-4 ${holding.length ? 'fill-current' : ''}`} />
        {holding.length ? `In ${holding.length} watchlist${holding.length > 1 ? 's' : ''}` : 'Add to watchlist'}
      </Button>

      {open ? (
        <div
          role="dialog"
          aria-label={`Watchlists for ${ticker}`}
          className="absolute right-0 z-20 mt-2 w-64 space-y-3 rounded-lg border bg-background p-3 shadow-md"
        >
          <p className="text-xs font-medium text-muted-foreground">Watchlists holding {ticker}</p>

          {lists.isLoading ? <p className="text-sm text-muted-foreground">Loading…</p> : null}
          {!lists.isLoading && all.length === 0 ? (
            <p className="text-sm text-muted-foreground">No watchlists yet.</p>
          ) : null}

          <div className="max-h-56 space-y-1 overflow-y-auto">
            {all.map((w) => {
              const checked = w.symbols.includes(ticker)
              return (
                <label key={w.name} className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={toggle.isPending}
                    onChange={() => toggle.mutate({ name: w.name, symbol: ticker, add: !checked })}
                  />
                  <span className="flex-1 truncate">{w.name}</span>
                  <span className="text-xs text-muted-foreground">{w.symbols.length}</span>
                </label>
              )
            })}
          </div>

          <form
            className="flex gap-2 border-t pt-3"
            onSubmit={(e) => {
              e.preventDefault()
              createList()
            }}
          >
            <Input
              aria-label="New watchlist name"
              placeholder="New watchlist"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
            />
            <Button type="submit" size="sm" disabled={!newName.trim() || create.isPending}>
              Create
            </Button>
          </form>

          {error ? <p className="text-xs text-destructive">{error.detail}</p> : null}
        </div>
      ) : null}
    </div>
  )
}
