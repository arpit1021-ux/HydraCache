import { useMemo } from 'react'
import type { ClusterNode } from '../hooks/useClusterData'

interface RequestFlowProps {
  nodes: ClusterNode[]
}

export default function RequestFlow({ nodes }: RequestFlowProps) {
  const connections = useMemo(() => {
    if (nodes.length < 2) return []
    const conns: { from: number; to: number; delay: number }[] = []
    for (let i = 0; i < nodes.length; i++) {
      const target = (i + 1) % nodes.length
      conns.push({ from: i, to: target, delay: Math.random() * 2 })
    }
    return conns
  }, [nodes])

  const nodePositions = useMemo(() => {
    const size = 200
    const center = size / 2
    const radius = 70
    return nodes.map((_, i) => {
      const angle = (i / nodes.length) * Math.PI * 2 - Math.PI / 2
      return {
        x: center + radius * Math.cos(angle),
        y: center + radius * Math.sin(angle),
      }
    })
  }, [nodes])

  if (nodes.length < 2) return null

  return (
    <div className="relative">
      <svg width="200" height="200" viewBox="0 0 200 200" className="mx-auto">
        {connections.map((conn, i) => {
          const from = nodePositions[conn.from]
          const to = nodePositions[conn.to]
          return (
            <g key={i}>
              <line
                x1={from.x}
                y1={from.y}
                x2={to.x}
                y2={to.y}
                stroke="#1e293b"
                strokeWidth={1.5}
              />
              <circle
                r={3}
                fill="#06b6d4"
                style={{
                  offsetPath: `path("M ${from.x} ${from.y} L ${to.x} ${to.y}")`,
                  animationDelay: `${conn.delay}s`,
                }}
                className="flow-dot"
              />
            </g>
          )
        })}
        {nodePositions.map((pos, i) => (
          <g key={i}>
            <circle
              cx={pos.x}
              cy={pos.y}
              r={10}
              fill="#0f172a"
              stroke={
                nodes[i].status === 'healthy'
                  ? '#10b981'
                  : nodes[i].status === 'degraded'
                  ? '#f59e0b'
                  : '#ef4444'
              }
              strokeWidth={2}
            />
            <text
              x={pos.x}
              y={pos.y + 1}
              textAnchor="middle"
              dominantBaseline="middle"
              fill="#94a3b8"
              fontSize={6}
              fontFamily="monospace"
            >
              {nodes[i].id.replace('node-', 'N')}
            </text>
          </g>
        ))}
      </svg>
      <div className="flex items-center justify-center gap-3 mt-2">
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-cyan-500" />
          <span className="text-[10px] text-gray-500">Request flow</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-emerald-500" />
          <span className="text-[10px] text-gray-500">Healthy</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full bg-amber-500" />
          <span className="text-[10px] text-gray-500">Degraded</span>
        </div>
      </div>
    </div>
  )
}
