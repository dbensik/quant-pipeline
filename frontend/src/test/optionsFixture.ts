/**
 * A small OptionsSurfaceResponse for the Options page and its row modules:
 * three expiries, one flagged near an ex-date, one with an un-interpolable
 * ATM vol (null), a cone, and smile points carrying Yahoo's IV beside ours.
 */

import type { OptionsSurfaceResponse, OptionSVIFit, OptionSVIResponse } from '@/api/client'

const point = (strike: number, isCall: boolean, k: number, iv: number, vendor: number | null) => ({
  strike, is_call: isCall, log_moneyness: k, forward_delta: isCall ? 0.3 : -0.3, iv, vendor_iv: vendor,
  bid: 1, ask: 1.02, premium: isCall ? 0 : 0.05,
})

export function surfaceFixture(overrides: Partial<OptionsSurfaceResponse> = {}): OptionsSurfaceResponse {
  return {
    ticker: 'SPY',
    day: '2026-10-08',
    captured_at: '2026-10-08T19:45:00Z',
    rate: { series: '^IRX', observation_date: '2026-10-08', quoted_pct: 4.043, continuous: 0.0412, stale: false },
    dividends: [{ ex_date: '2026-12-18', amount: 1.889, projected: true }],
    quality: { rows: 5404, priced: 4489, zero_bid: 514, locked: 110, no_iv: 265 },
    expiries: [
      { expiry: '2026-10-30', T: 0.06, forward: 780.0, raw_parity_forward: 779.9, forward_passes: 3, forward_converged: true, implied_spot: 778.1, dividends_before_expiry: 0, dividend_date_uncertain: false },
      { expiry: '2026-12-18', T: 0.19, forward: 782.0, raw_parity_forward: 782.7, forward_passes: 4, forward_converged: true, implied_spot: 777.9, dividends_before_expiry: 1, dividend_date_uncertain: true },
      { expiry: '2027-01-15', T: 0.27, forward: 784.8, raw_parity_forward: 784.1, forward_passes: 4, forward_converged: false, implied_spot: 778.6, dividends_before_expiry: 1, dividend_date_uncertain: false },
    ],
    term_structure: [
      { expiry: '2026-10-30', T: 0.06, forward: 780.0, atm_iv: 0.12, put25_iv: 0.137, call25_iv: 0.108, skew_25d: 0.029, points: 4, dividend_date_uncertain: false },
      { expiry: '2026-12-18', T: 0.19, forward: 782.0, atm_iv: null, put25_iv: null, call25_iv: 0.119, skew_25d: null, points: 4, dividend_date_uncertain: true },
      { expiry: '2027-01-15', T: 0.27, forward: 784.8, atm_iv: 0.137, put25_iv: 0.167, call25_iv: 0.121, skew_25d: 0.046, points: 4, dividend_date_uncertain: false },
    ],
    smiles: {
      '2026-10-30': [point(760, false, -0.026, 0.14, 0.15), point(800, true, 0.025, 0.11, null), point(740, false, -0.053, 0.16, 0.19), point(820, true, 0.05, 0.105, 0.12)],
      '2026-12-18': [point(760, false, -0.029, 0.15, 0.16), point(800, true, 0.023, 0.12, 0.13)],
      '2027-01-15': [point(760, false, -0.032, 0.16, 0.18), point(800, true, 0.019, 0.125, 0.14)],
    },
    compute_seconds: 21.9,
    computed_at: '2026-10-09T11:40:00-04:00',
    cache_hit: true,
    cone: {
      symbol: 'SPY', estimator: 'yang_zhang', as_of: '2026-10-08', bars: 2958,
      windows: [
        { window: 10, min: 0.04, p25: 0.09, median: 0.118, p75: 0.174, max: 1.17, current: 0.103 },
        { window: 20, min: 0.044, p25: 0.096, median: 0.124, p75: 0.18, max: 0.93, current: 0.112 },
        { window: 60, min: 0.058, p25: 0.107, median: 0.136, p75: 0.187, max: 0.64, current: null },
        { window: 120, min: 0.064, p25: 0.116, median: 0.144, p75: 0.191, max: 0.48, current: 0.124 },
      ],
    },
    cone_reason: null,
    ...overrides,
  } as OptionsSurfaceResponse
}

const curve = (lo: number, hi: number, n = 9) =>
  Array.from({ length: n }, (_, i) => {
    const k = lo + ((hi - lo) * i) / (n - 1)
    return { log_moneyness: k, iv: 0.12 + 0.3 * k * k }
  })

const fit = (expiry: string, T: number, extra: Partial<OptionSVIFit> = {}): OptionSVIFit =>
  ({
    expiry, T, days: T * 365, points: 9,
    params: { a: 0.0012, b: 0.0431, rho: -0.4512, m: 0.0123, sigma: 0.0811 },
    rmse_vol_pts: 0.31, within_spread_share: 0.45, atm_iv: 0.121, min_iv: 0.11,
    left_wing_slope: -0.0625, right_wing_slope: 0.0237, bottom_quoted: true, wings_within_lee: true,
    k_range: { low: -0.053, high: 0.05 }, curve: curve(-0.15, 0.15),
    butterfly: { verdict: 'none', checked: { low: -0.153, high: 0.15 }, fit: [], min_g: 0.4, quotes: [] },
    ...extra,
  }) as OptionSVIFit

/** SVI for surfaceFixture(): 2026-12-18 is skipped (2 points). */
export function sviFixture(overrides: Partial<OptionSVIResponse> = {}): OptionSVIResponse {
  return {
    ticker: 'SPY',
    day: '2026-10-08',
    captured_at: '2026-10-08T19:45:00Z',
    parameters: { a: 'Level.', b: 'Wings.', rho: 'Tilt.', m: 'Shift.', sigma: 'Rounding.' },
    min_points: 4,
    min_days: 2,
    butterfly_margin: 0.1,
    fits: [
      fit('2026-10-30', 0.06, {
        butterfly: {
          verdict: 'both', checked: { low: -0.153, high: 0.15 },
          fit: [{ low: -0.12, high: -0.08 }], min_g: -0.02, quotes: [{ low: -0.053, high: 0.025 }],
        },
      }),
      fit('2027-01-15', 0.27, { bottom_quoted: false, params: { a: -0.2, b: 5.3, rho: 0.97, m: 0.93, sigma: 0.21 } }),
    ],
    skipped: [{ expiry: '2026-12-18', reason: '2 smile points, fewer than 4' }],
    calendar: [
      {
        near: '2026-10-30', far: '2027-01-15', verdict: 'fit_only',
        shared: { low: -0.026, high: 0.019 }, fit: [{ low: 0.01, high: 0.019 }], quotes: [],
        mid_crossing: [{ low: -0.02, high: -0.01 }],
      },
    ],
    butterfly_summary: { none: 1, both: 1, fit_only: 0, quotes_only: 0 },
    calendar_summary: { none: 0, both: 0, fit_only: 1, quotes_only: 0 },
    fit_seconds: 1.2,
    surface_cache_hit: true,
    ...overrides,
  } as OptionSVIResponse
}
