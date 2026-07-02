import { useMemo } from 'react'
import type { ClusterNode } from '../hooks/useClusterData'

interface HashRingVisualProps {
  nodes: ClusterNode[]
  size?: number
}

const NODE_COLORS = [
  '#06b6d4', '#8b5cf6', '#f59e0b', '#ef4444',
  '#10b981', '#ec4899', '#6366f1', '#14b8a6',
  '#f97316', '#a855f7', '#3b82f6', '#22c55e',
]

function hashString(str: string): number {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    hash = (hash * 31 + str.charCodeAt(i)) % 2147483647
  }
  return hash / 2147483647
}

export default function HashRingVisual({ nodes, size = 320 }: HashRingVisualProps) {
  const center = size / 2
  const radius = size / 2 - 20
  const innerRadius = radius - 16

  const segments = useMemo(() => {
    if (nodes.length === 0) return []

    const virtualNodes: { angle: number; nodeIndex: number }[] = []
    nodes.forEach((node, i) => {
      const numVnodes = 8
      for (let v = 0; v < numVnodes; v++) {
        const hash = hashString(`${node.id}-vnode-${v}`)
        virtualNodes.push({ angle: hash * Math.PI * 2, nodeIndex: i })
      }
    })

    virtualNodes.sort((a, b) => a.angle - b.angle)

    return virtualNodes.map((vn, i) => {
      const nextVn = virtualNodes[(i + 1) % virtualNodes.length]
      let startAngle = vn.angle
      let endAngle = nextVn.angle
      if (endAngle <= startAngle) {
        endAngle += Math.PI * 2
      }
      return {
        startAngle,
        endAngle,
        color: NODE_COLORS[vn.nodeIndex % NODE_COLORS.length],
        nodeId: nodes[vn.nodeIndex].id,
      }
    })
  }, [nodes])

  const toXY = (angle: number, r: number) => ({
    x: center + r * Math.cos(angle - Math.PI / 2),
    y: center + r * Math.sin(angle - Math.PI / 2),
  })

  return (
    <svg
      width={size}
      height={size}
      viewBox={`0 0 ${size} ${size}`}
      className="drop-shadow-lg"
    >
      <circle
        cx={center}
        cy={center}
        r={radius}
        fill="none"
        stroke="#1e293b"
        strokeWidth={2}
      />
      <circle
        cx={center}
        cy={center}
        r={innerRadius}
        fill="none"
        stroke="#1e293b"
        strokeWidth={1}
        strokeDasharray="4 4"
      />

      {segments.map((seg, i) => {
        const p1o = toXY(seg.startAngle, radius)
        const p2o = toXY(seg.endAngle, radius)
        const p1i = toXY(seg.startAngle, innerRadius)
        const p2i = toXY(seg.endAngle, innerRadius)
        const largeArc = seg.endAngle - seg.startAngle > Math.PI ? 1 : 0

        const d = [
          `M ${p1i.x} ${p1i.y}`,
          `L ${p1o.x} ${p1o.y}`,
          `A ${radius} ${radius} 0 ${largeArc} 1 ${p2o.x} ${p2o.y}`,
          `L ${p2i.x} ${p2i.y}`,
          `A ${innerRadius} ${innerRadius} 0 ${largeArc} 0 ${p1i.x} ${p1i.y}`,
          'Z',
        ].join(' ')

        return (
          <path
            key={i}
            d={d}
            fill={seg.color}
            opacity={0.7}
            stroke="#0f172a"
            strokeWidth={1}
          />
        )
      })}

      {nodes.map((node, i) => {
        const angle = (i / nodes.length) * Math.PI * 2 - Math.PI / 2
        const labelRadius = radius + 14
        const pos = toXY(angle, labelRadius)
        return (
          <g key={node.id}>
            <circle
              cx={toXY(angle, radius).x}
              cy={toXY(angle, radius).y}
              r={5}
              fill={NODE_COLORS[i % NODE_COLORS.length]}
              stroke="#0f172a"
              strokeWidth={2}
            />
            <text
              x={pos.x}
              y={pos.y}
              textAnchor="middle"
              dominantBaseline="middle"
              fill="#94a3b8"
              fontSize={10}
              fontFamily="monospace"
            >
              {node.id}
            </text>
          </g>
        )
      })}

      <text
        x={center}
        y={center - 8}
        textAnchor="middle"
        fill="#64748b"
        fontSize={11}
        fontWeight="600"
      >
        HydraCache
      </text>
      <text
        x={center}
        y={center + 8}
        textAnchor="middle"
        fill="#475569"
        fontSize={9}
      >
        {nodes.length} nodes
      </text>
    </svg>
  )
}
