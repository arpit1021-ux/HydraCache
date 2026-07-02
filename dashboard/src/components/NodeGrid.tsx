import type { ClusterNode } from '../hooks/useClusterData'
import NodeCard from './NodeCard'

interface NodeGridProps {
  nodes: ClusterNode[]
}

export default function NodeGrid({ nodes }: NodeGridProps) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
      {nodes.map((node) => (
        <NodeCard key={node.id} node={node} />
      ))}
    </div>
  )
}
