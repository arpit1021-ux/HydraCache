import { useEffect, useState, useCallback, useRef } from 'react'

export interface ClusterNode {
  id: string
  address: string
  role: 'peer' | 'leader' | 'replica'
  health: 'alive' | 'suspect' | 'dead' | 'left'
  load: number
  memory_mb: number
  replication_lag: number
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
                health: 'alive',
                load: 35,
                memory_mb: 2048,
                replication_lag: 0,
              },
              {
                id: 'node-1',
                address: '127.0.0.1:8381',
                role: 'replica',
                health: 'alive',
                load: 42,
                memory_mb: 1856,
                replication_lag: 5,
              },
              {
                id: 'node-2',
                address: '127.0.0.1:8382',
                role: 'replica',
                health: 'alive',
                load: 28,
                memory_mb: 1536,
                replication_lag: 8,
              },
              {
                id: 'node-3',
                address: '127.0.0.1:8383',
                role: 'replica',
                health: 'suspect',
                load: 78,
                memory_mb: 6144,
                replication_lag: 45,
              },
              {
                id: 'node-4',
                address: '127.0.0.1:8384',
                role: 'replica',
                health: 'alive',
                load: 22,
                memory_mb: 1280,
                replication_lag: 3,
              },
              {
                id: 'node-5',
                address: '127.0.0.1:8385',
                role: 'replica',
                health: 'alive',
                load: 31,
                memory_mb: 1600,
                replication_lag: 7,
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
