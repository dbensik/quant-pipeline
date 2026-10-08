/**
 * api/useSimulationSocket.ts
 *
 * Runs a Monte Carlo simulation over the websocket and exposes its progress
 * as React state. The twin of useBacktestSocket — same reasons for not being
 * a TanStack mutation (a stream of messages, not one response).
 *
 * Phase 4 of research/monte-carlo-plan-2026-10-07.md
 */

import { useCallback, useEffect, useRef, useState } from 'react'

import type { SimulationInput, SimulationResponse } from './client'
import { runSimulationOverSocket } from './ws'
import type { WsProgress } from './ws'

export interface SimulationSocketState {
  isRunning: boolean
  progress: WsProgress | null
  result: SimulationResponse | null
  error: string | null
}

const IDLE: SimulationSocketState = {
  isRunning: false,
  progress: null,
  result: null,
  error: null,
}

export function useSimulationSocket() {
  const [state, setState] = useState<SimulationSocketState>(IDLE)
  const mounted = useRef(true)
  const runId = useRef(0)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  const run = useCallback((request: SimulationInput) => {
    const id = ++runId.current
    const isCurrent = () => mounted.current && runId.current === id

    setState({ ...IDLE, isRunning: true })

    runSimulationOverSocket(request, (message) => {
      if (!isCurrent()) return
      if (message.type === 'progress') {
        setState((previous) => ({ ...previous, progress: message }))
      }
    })
      .then((message) => {
        if (!isCurrent()) return
        // Strip the envelope's `type`; what remains is the REST response shape.
        const { type: _type, ...result } = message
        void _type
        setState({ isRunning: false, progress: null, result, error: null })
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
    runId.current += 1
    setState(IDLE)
  }, [])

  return { ...state, run, reset }
}
