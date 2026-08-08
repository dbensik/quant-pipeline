/**
 * api/useBacktestSocket.ts
 *
 * Runs a backtest over the websocket and exposes its progress as React state.
 *
 * This is the counterpart to useRunBacktest (the REST mutation): same result,
 * but the stages arrive as they happen instead of the UI sitting blank until
 * the run finishes.
 *
 * Not a TanStack Query mutation, deliberately — Query models one request and
 * one response, whereas this yields a stream of intermediate messages before
 * the result. Wrapping it in useMutation would either hide the progress or
 * abuse onProgress-style callbacks to smuggle it out.
 *
 * Phase 4 — React frontend
 */

import { useCallback, useEffect, useRef, useState } from 'react'

import type { BacktestInput } from './client'
import type { BacktestResult } from './backtestResult'
import { fromSocket } from './backtestResult'
import { runBacktestOverSocket } from './ws'
import type { WsProgress } from './ws'

export interface BacktestSocketState {
  isRunning: boolean
  progress: WsProgress | null
  result: BacktestResult | null
  error: string | null
}

const IDLE: BacktestSocketState = {
  isRunning: false,
  progress: null,
  result: null,
  error: null,
}

export function useBacktestSocket() {
  const [state, setState] = useState<BacktestSocketState>(IDLE)

  // Guards against setting state after unmount, and against a stale run
  // overwriting a newer one if the user fires two backtests quickly.
  const mounted = useRef(true)
  const runId = useRef(0)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  const run = useCallback((request: BacktestInput) => {
    const id = ++runId.current
    const isCurrent = () => mounted.current && runId.current === id

    setState({ ...IDLE, isRunning: true })

    runBacktestOverSocket(request, (message) => {
      if (!isCurrent()) return
      if (message.type === 'progress') {
        setState((previous) => ({ ...previous, progress: message }))
      }
    })
      .then((message) => {
        if (!isCurrent()) return
        setState({
          isRunning: false,
          progress: null,
          result: fromSocket(message),
          error: null,
        })
      })
      .catch((cause: unknown) => {
        if (!isCurrent()) return
        setState({
          isRunning: false,
          progress: null,
          result: null,
          error: cause instanceof Error ? cause.message : String(cause),
        })
      })
  }, [])

  const reset = useCallback(() => {
    // Bump the id so an in-flight run cannot resurrect its result after reset.
    runId.current += 1
    setState(IDLE)
  }, [])

  return { ...state, run, reset }
}
