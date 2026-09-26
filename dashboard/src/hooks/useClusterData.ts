import { useEffect, useState, useCallback, useRef } from 'react'

export interface ClusterNode {
  id: string
  address: string
  role: 'peer' | 'leader' | 'replica'
  health: 'alive' | 'suspect' | 'dead' | 'left'
  replication_lag: number
  last_seen: string
  joined_at: string
}

export interface ClusterData {
  nodes: ClusterNode[]
  epoch: number
}

// demoClusterData is shown only while disconnected from a real backend —
// App.tsx renders a persistent "Demo Data — Disconnected" banner whenever
// that's the case, so this is never presented as live telemetry.
function demoClusterData(): ClusterData {
  const now = Date.now()
  const seenSecondsAgo = (s: number) => new Date(now - s * 1000).toISOString()
  return {
    nodes: [
      { id: 'node-0', address: '127.0.0.1:8380', role: 'leader', health: 'alive', replication_lag: 0, last_seen: seenSecondsAgo(1), joined_at: seenSecondsAgo(86400) },
      { id: 'node-1', address: '127.0.0.1:8381', role: 'replica', health: 'alive', replication_lag: 5, last_seen: seenSecondsAgo(1), joined_at: seenSecondsAgo(86400) },
      { id: 'node-2', address: '127.0.0.1:8382', role: 'replica', health: 'alive', replication_lag: 8, last_seen: seenSecondsAgo(2), joined_at: seenSecondsAgo(86000) },
      { id: 'node-3', address: '127.0.0.1:8383', role: 'replica', health: 'suspect', replication_lag: 45, last_seen: seenSecondsAgo(14), joined_at: seenSecondsAgo(85000) },
      { id: 'node-4', address: '127.0.0.1:8384', role: 'replica', health: 'alive', replication_lag: 3, last_seen: seenSecondsAgo(1), joined_at: seenSecondsAgo(84000) },
      { id: 'node-5', address: '127.0.0.1:8385', role: 'replica', health: 'alive', replication_lag: 7, last_seen: seenSecondsAgo(2), joined_at: seenSecondsAgo(83000) },
    ],
    epoch: Math.floor(now / 1000),
  }
}

export function useClusterData(pollInterval = 3000) {
  const [clusterData, setClusterData] = useState<ClusterData | null>(null)
  const [connected, setConnected] = useState(false)
  const mountedRef = useRef(true)

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch('/api/cluster')
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data: ClusterData = await res.json()
      if (mountedRef.current) {
        setClusterData(data)
        setConnected(true)
      }
    } catch {
      if (mountedRef.current) {
        setConnected(false)
        setClusterData((prev) => prev ?? demoClusterData())
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

  return { clusterData, connected }
}
