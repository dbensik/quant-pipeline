/**
 * components/layout/Caveat.tsx
 * A strategy's caveat, collapsed to one line with the full text a click away.
 *
 * Since decisions 25-36 every strategy carries what was measured about it
 * (beat buy and hold on N% of names, ...) beside any real limitation. Shown as
 * red alerts — "Results are not trustworthy", "Known limitation" — that read
 * as an error on every run; shown as amber paragraphs they crowded the page.
 * Neutral and collapsed, the evidence stays one click from every result.
 *
 * ONE copy of the text, in the <summary>: cut to a line while closed, wrapped
 * in full when open. A preview plus a body would put it in the DOM twice and a
 * screen reader would read it twice. It is always present, so it stays
 * searchable while collapsed.
 */

export function Caveat({
  text,
  label = 'Evidence and limits',
  className = '',
}: {
  text: string | null | undefined
  label?: string
  className?: string
}) {
  if (!text) return null
  return (
    <details className={`group rounded-md border bg-muted/40 px-3 py-2 text-xs ${className}`}>
      <summary className="flex cursor-pointer list-none items-baseline gap-1.5 [&::-webkit-details-marker]:hidden">
        <span className="shrink-0 font-medium">{label}:</span>
        <span className="truncate leading-relaxed text-muted-foreground group-open:whitespace-normal">
          {text}
        </span>
      </summary>
    </details>
  )
}
