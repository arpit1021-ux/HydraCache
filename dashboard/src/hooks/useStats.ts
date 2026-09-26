import { useEffect, useState, useCallback, useRef } from 'react'

export interface Stats {
  keys: number
  hits: number
  misses: number
  hit_rate: number
  // Global histogram of real recorded command latencies, bucketed at
  // <1ms, 1-5ms, 5-10ms, 10-25ms, 25-50ms, 50-100ms, 100-250ms, >=250ms —
  // see internal/metrics.LatencyHistogramBounds, the single source of
  // truth for these boundaries. Optional because older/incompatible
  // backends won't send it.
  latency_histogram?: number[]
}

export function useStats(pollInterval = 2000) {
  const [stats, setStats] = useState<Stats | null>(null)
  const [connected, setConnected] = useState(false)
  const mountedRef = useRef(true)

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch('/api/stats')
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data: Stats = await res.json()
      if (mountedRef.current) {
        setStats(data)
        setConnected(true)
      }
    } catch {
      if (mountedRef.current) {
        setConnected(false)
        // Shown only while disconnected — App.tsx's persistent "Demo
        // Data" banner makes that state visible whenever this fallback
        // is in use.
        setStats(
          (prev) =>
            prev ?? {
              keys: 284593,
              hits: 1847293,
              misses: 291847,
              hit_rate: 86.4,
              latency_histogram: [1240, 3580, 2890, 1420, 680, 320, 85, 12],
            }
        )
      }
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true
    fetchData()
    const interval = setInterval(fetchData, pollInterval)
    return () => {
      mountedRef.current = false
      clearInterval(interval)
    }
  }, [fetchData, pollInterval])

  return { stats, connected }
}
