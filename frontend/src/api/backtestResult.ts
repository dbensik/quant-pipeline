/**
 * api/backtestResult.ts
 *
 * One view model for a completed backtest, whichever transport produced it.
 *
 * The two responses are deliberately NOT identical: REST returns the full
 * trade log as an array, the websocket returns only a count (the log can run
 * to thousands of rows and the UI shows only how many). Normalising here keeps
 * that difference in one place instead of forcing every component to branch on
 * where the result came from.
 *
 * Phase 4 — React frontend
 */

import type { BacktestResponse } from './client'
import type { WsResult } from './ws'

export interface BacktestResult {
  symbol: string
  strategyId: string
  strategyName: string
  bars: number
  tradeCount: number
  /** Strategy parameters only — initial capital is separate, see below. */
  params: Record<string, unknown>
  /**
   * Starting capital. A top-level field rather than part of `params`, because
   * it is not a strategy parameter — reading it out of `params` yields
   * undefined and silently draws the break-even line at a default.
   */
  initialCapital: number
  seed: number | null
  metrics: Record<string, unknown>
  caveat: string | null
  equityCurve: Array<{ time: string; total: number }>
  /** Which transport produced this, so the UI can say so. */
  via: 'rest' | 'websocket'
}

export function fromRest(response: BacktestResponse): BacktestResult {
  return {
    symbol: response.symbol,
    strategyId: response.strategy_id,
    strategyName: response.strategy_name,
    bars: response.bars,
    tradeCount: response.trades?.length ?? 0,
    params: response.params,
    initialCapital: response.initial_capital,
    seed: response.seed ?? null,
    metrics: response.metrics,
    caveat: response.caveat ?? null,
    equityCurve: (response.equity_curve ?? []).map((point) => ({
      time: point.time,
      total: point.total,
    })),
    via: 'rest',
  }
}

export function fromSocket(message: WsResult): BacktestResult {
  return {
    symbol: message.symbol,
    strategyId: message.strategy_id,
    strategyName: message.strategy_name,
    bars: message.bars,
    tradeCount: message.trades,
    params: message.params,
    initialCapital: message.initial_capital,
    seed: message.seed,
    metrics: message.metrics,
    caveat: message.caveat,
    equityCurve: message.equity_curve ?? [],
    via: 'websocket',
  }
}
