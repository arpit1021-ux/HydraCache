import { Crown } from 'lucide-react'
import type { ClusterNode } from '../hooks/useClusterData'

interface LeaderBadgeProps {
  node: ClusterNode
}

export default function LeaderBadge({ node }: LeaderBadgeProps) {
  return (
    <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-amber-500/10 border border-amber-500/20">
      <Crown size={14} className="text-amber-400" />
      <span className="text-xs font-medium text-amber-300">
        Leader: {node.id}
      </span>
      <span className="text-xs text-amber-500/60">{node.address}</span>
    </div>
  )
}
