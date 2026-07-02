import { useEffect, useState, useCallback, useRef } from 'react'

export interface Stats {
  keys: number
  hits: number
  misses: number
  hit_rate: number
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
        setStats(
          (prev) =>
            prev ?? {
              keys: 284593,
              hits: 1847293,
              misses: 291847,
              hit_rate: 86.4,
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
