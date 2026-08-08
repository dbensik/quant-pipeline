/**
 * Types for plotly.js-finance-dist-min.
 *
 * DefinitelyTyped publishes @types/plotly.js but nothing for the per-domain
 * dist bundles. The finance dist is the same library with a reduced trace set,
 * so its module surface is identical — pointing at the published types is
 * accurate, and far better than letting the import fall back to `any`, which
 * would silently un-type every Plotly call in CandlestickChart.
 *
 * Declared as a default-exported value (not `export =` of a type-only import)
 * so it survives `verbatimModuleSyntax`, which the app has enabled.
 *
 * Phase 4 — React frontend
 */

declare module 'plotly.js-finance-dist-min' {
  const Plotly: typeof import('plotly.js')
  export default Plotly
}
