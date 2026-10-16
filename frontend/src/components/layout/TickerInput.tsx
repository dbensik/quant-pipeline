/**
 * components/layout/TickerInput.tsx
 * A searchable ticker box: free text with the registry's symbols as suggestions.
 *
 * Commits on Enter, on leaving the field, or on the optional button — never
 * while typing, or "AA" on the way to "AAPL" would load another company.
 * Suggestions use a native <datalist>: searchable, keyboard-accessible, and no
 * new component library.
 *
 * Pages bind it to the SHARED ticker (useAppStore.selectedSymbol), so a ticker
 * chosen on Research is the one Valuation, Compare and Chart & Backtest open
 * with (2026-10-15). Before, each page kept its own and showed AAPL.
 */

import { useState } from 'react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'

export function TickerInput({
  id,
  value,
  onCommit,
  suggestions,
  buttonLabel,
}: {
  id: string
  value: string
  onCommit: (symbol: string) => void
  suggestions: string[]
  /** Shown as a submit button beside the field; omit to commit on Enter/blur only. */
  buttonLabel?: string
}) {
  const [text, setText] = useState(value)
  const [shown, setShown] = useState(value)
  // A ticker chosen elsewhere replaces what the field shows (React's
  // adjust-state-during-render pattern; an effect would render twice).
  if (shown !== value) {
    setShown(value)
    setText(value)
  }

  function commit() {
    const symbol = text.toUpperCase().trim()
    if (symbol && symbol !== value) onCommit(symbol)
    else setText(value)
  }

  const listId = `${id}-suggestions`
  return (
    <form
      className="flex gap-2"
      onSubmit={(e) => {
        e.preventDefault()
        commit()
      }}
    >
      <Input
        id={id}
        list={listId}
        autoComplete="off"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onBlur={buttonLabel ? undefined : commit}
      />
      <datalist id={listId}>
        {suggestions.map((s) => (
          <option key={s} value={s} />
        ))}
      </datalist>
      {buttonLabel ? (
        <Button type="submit" className="shrink-0">
          {buttonLabel}
        </Button>
      ) : null}
    </form>
  )
}
