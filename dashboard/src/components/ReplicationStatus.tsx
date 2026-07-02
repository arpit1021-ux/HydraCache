import type { ClusterNode } from '../hooks/useClusterData'

interface ReplicationStatusProps {
  nodes: ClusterNode[]
}

const LAG_COLORS = ['#10b981', '#f59e0b', '#ef4444']

function lagColor(lag: number): string {
  if (lag < 10) return LAG_COLORS[0]
  if (lag < 50) return LAG_COLORS[1]
  return LAG_COLORS[2]
}

export default function ReplicationStatus({ nodes }: ReplicationStatusProps) {
  const maxLag = Math.max(...nodes.map((n) => n.replication_lag_ms), 1)

  return (
    <div className="space-y-3">
      {nodes.map((node) => (
        <div key={node.id} className="flex items-center gap-3">
          <span className="text-xs text-gray-400 w-16 font-mono shrink-0">{node.id}</span>
          <div className="flex-1 h-5 bg-gray-800 rounded-full overflow-hidden relative">
            <div
              className="h-full rounded-full transition-all duration-700"
              style={{
                width: `${(node.replication_lag_ms / maxLag) * 100}%`,
                backgroundColor: lagColor(node.replication_lag_ms),
              }}
            />
          </div>
          <span className="text-xs text-gray-400 w-20 text-right font-mono shrink-0">
            {node.replication_lag_ms}ms
          </span>
        </div>
      ))}
    </div>
  )
}
