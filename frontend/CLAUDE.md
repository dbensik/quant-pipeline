# frontend/

Frontend specifics: charts render nothing under jsdom (Recharts measures a 0x0
parent; Plotly needs canvas APIs jsdom lacks), so never assert on chart output —
mock the chart child and assert on its props. Chart computation lives in pure
modules beside the components (`chartRows.ts`, `candlestickData.ts`) for the
same reason.

Plotly is loaded lazily and must stay that way — it is ~1.2 MB, larger than the
rest of the bundle. `npm run build` must show a separate CandlestickChart chunk
with no "plotly" in the main chunk.
