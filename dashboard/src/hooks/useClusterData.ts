import { useEffect, useState, useCallback, useRef } from 'react'

export interface ClusterNode {
  id: string
  address: string
  role: 'leader' | 'follower'
  status: 'healthy' | 'degraded' | 'down'
  cpu_load: number
  memory_used_mb: number
  memory_total_mb: number
  replication_lag_ms: number
}

export interface ClusterData {
  nodes: ClusterNode[]
  epoch: number
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
        setClusterData((prev) =>
          prev ?? {
            nodes: [
              {
                id: 'node-0',
                address: '127.0.0.1:8380',
                role: 'leader',
                status: 'healthy',
                cpu_load: 35,
                memory_used_mb: 2048,
                memory_total_mb: 8192,
                replication_lag_ms: 0,
              },
              {
                id: 'node-1',
                address: '127.0.0.1:8381',
                role: 'follower',
                status: 'healthy',
                cpu_load: 42,
                memory_used_mb: 1856,
                memory_total_mb: 8192,
                replication_lag_ms: 5,
              },
              {
                id: 'node-2',
                address: '127.0.0.1:8382',
                role: 'follower',
                status: 'healthy',
                cpu_load: 28,
                memory_used_mb: 1536,
                memory_total_mb: 8192,
                replication_lag_ms: 8,
              },
              {
                id: 'node-3',
                address: '127.0.0.1:8383',
                role: 'follower',
                status: 'degraded',
                cpu_load: 78,
                memory_used_mb: 6144,
                memory_total_mb: 8192,
                replication_lag_ms: 45,
              },
              {
                id: 'node-4',
                address: '127.0.0.1:8384',
                role: 'follower',
                status: 'healthy',
                cpu_load: 22,
                memory_used_mb: 1280,
                memory_total_mb: 8192,
                replication_lag_ms: 3,
              },
              {
                id: 'node-5',
                address: '127.0.0.1:8385',
                role: 'follower',
                status: 'healthy',
                cpu_load: 31,
                memory_used_mb: 1600,
                memory_total_mb: 8192,
                replication_lag_ms: 7,
              },
            ],
            epoch: Math.floor(Date.now() / 1000),
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

  return { clusterData, connected }
}
