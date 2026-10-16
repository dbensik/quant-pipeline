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
 *
 * The panel is rendered in a portal at the button's position: inside a Card
 * (overflow-hidden) an absolutely positioned panel was clipped after its first
 * row — seen in the browser 2026-10-15. It closes on an outside click, Escape,
 * scroll or resize, so a fixed panel never drifts away from its button.
 */

import { Star } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import type { ApiError } from "@/api/client";
import {
  useSaveWatchlist,
  useToggleWatchlistSymbol,
  useWatchlists,
} from "@/api/queries";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function WatchlistMenu({
  symbol,
}: {
  symbol: string | null | undefined;
}) {
  const ticker = (symbol ?? "").toUpperCase().trim();
  const [open, setOpen] = useState(false);
  const [anchor, setAnchor] = useState<{ top: number; right: number } | null>(
    null,
  );
  const buttonRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const [newName, setNewName] = useState("");
  const lists = useWatchlists();
  const toggle = useToggleWatchlistSymbol();
  const create = useSaveWatchlist();

  useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    const onPointer = (e: PointerEvent) => {
      const target = e.target as Node;
      if (
        panelRef.current?.contains(target) ||
        buttonRef.current?.contains(target)
      )
        return;
      close();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  }, [open]);

  if (!ticker) return null;

  function toggleOpen() {
    if (!open && buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      setAnchor({
        top: rect.bottom + 8,
        right: window.innerWidth - rect.right,
      });
    }
    setOpen((o) => !o);
  }

  const all = lists.data ?? [];
  const holding = all
    .filter((w) => w.symbols.includes(ticker))
    .map((w) => w.name);
  const error = (toggle.error ?? create.error) as ApiError | null;

  function createList() {
    const name = newName.trim();
    if (!name) return;
    const onSuccess = () => setNewName("");
    if (all.some((w) => w.name === name)) {
      toggle.mutate({ name, symbol: ticker, add: true }, { onSuccess });
    } else {
      create.mutate({ name, symbols: [ticker] }, { onSuccess });
    }
  }

  return (
    <>
      <Button
        ref={buttonRef}
        type="button"
        size="sm"
        variant={holding.length ? "secondary" : "outline"}
        aria-expanded={open}
        onClick={toggleOpen}
      >
        <Star
          className={`mr-1 h-4 w-4 ${holding.length ? "fill-current" : ""}`}
        />
        {holding.length
          ? `In ${holding.length} watchlist${holding.length > 1 ? "s" : ""}`
          : "Add to watchlist"}
      </Button>

      {open
        ? createPortal(
            <div
              ref={panelRef}
              role="dialog"
              aria-label={`Watchlists for ${ticker}`}
              style={
                anchor
                  ? { position: "fixed", top: anchor.top, right: anchor.right }
                  : undefined
              }
              className="z-50 w-64 space-y-3 rounded-lg border bg-background p-3 shadow-md"
            >
              <p className="text-xs font-medium text-muted-foreground">
                Watchlists holding {ticker}
              </p>

              {lists.isLoading ? (
                <p className="text-sm text-muted-foreground">Loading…</p>
              ) : null}
              {!lists.isLoading && all.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No watchlists yet.
                </p>
              ) : null}

              <div className="max-h-56 space-y-1 overflow-y-auto">
                {all.map((w) => {
                  const checked = w.symbols.includes(ticker);
                  return (
                    <label
                      key={w.name}
                      className="flex items-center gap-2 text-sm"
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={toggle.isPending}
                        onChange={() =>
                          toggle.mutate({
                            name: w.name,
                            symbol: ticker,
                            add: !checked,
                          })
                        }
                      />
                      <span className="flex-1 truncate">{w.name}</span>
                      <span className="text-xs text-muted-foreground">
                        {w.symbols.length}
                      </span>
                    </label>
                  );
                })}
              </div>

              <form
                className="flex gap-2 border-t pt-3"
                onSubmit={(e) => {
                  e.preventDefault();
                  createList();
                }}
              >
                <Input
                  aria-label="New watchlist name"
                  placeholder="New watchlist"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                />
                <Button
                  type="submit"
                  size="sm"
                  disabled={!newName.trim() || create.isPending}
                >
                  Create
                </Button>
              </form>

              {error ? (
                <p className="text-xs text-destructive">{error.detail}</p>
              ) : null}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
