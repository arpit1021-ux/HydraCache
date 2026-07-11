import type { ClusterData } from '../hooks/useClusterData'
import NodeGrid from '../components/NodeGrid'

interface NodesProps {
  clusterData: ClusterData | null
}

export default function Nodes({ clusterData }: NodesProps) {
  const nodes = clusterData?.nodes ?? []

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-lg font-bold text-white mb-1">Cluster Nodes</h2>
        <p className="text-sm text-gray-500">
          {nodes.length} nodes in cluster &middot; Epoch {clusterData?.epoch ?? '—'}
        </p>
      </div>

      {nodes.length === 0 ? (
        <div className="text-center py-20 text-gray-600">
          <p className="text-sm">No nodes available. Check backend connection.</p>
        </div>
      ) : (
        <NodeGrid nodes={nodes} />
      )}

      <div className="bg-gray-900/50 border border-gray-800 rounded-xl overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-800">
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
            Detailed Node Table
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-gray-500 uppercase tracking-wider border-b border-gray-800">
                <th className="text-left px-5 py-3 font-medium">Node</th>
                <th className="text-left px-5 py-3 font-medium">Address</th>
                <th className="text-left px-5 py-3 font-medium">Role</th>
                <th className="text-left px-5 py-3 font-medium">Status</th>
                <th className="text-right px-5 py-3 font-medium">CPU</th>
                <th className="text-right px-5 py-3 font-medium">Memory</th>
                <th className="text-right px-5 py-3 font-medium">Replication Lag</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((node) => (
                <tr
                  key={node.id}
                  className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors"
                >
                  <td className="px-5 py-3 font-semibold text-white font-mono">{node.id}</td>
                  <td className="px-5 py-3 text-gray-400 font-mono text-xs">{node.address}</td>
                  <td className="px-5 py-3">
                    <span
                      className={`text-[10px] uppercase font-bold tracking-wider px-2 py-0.5 rounded ${
                        node.role === 'leader'
                          ? 'bg-amber-500/20 text-amber-300'
                          : 'bg-gray-700 text-gray-400'
                      }`}
                    >
                      {node.role}
                    </span>
                  </td>
                  <td className="px-5 py-3">
                    <div className="flex items-center gap-1.5">
                      <div
                        className={`w-2 h-2 rounded-full ${
                          node.health === 'alive'
                            ? 'bg-emerald-500'
                            : node.health === 'suspect'
                            ? 'bg-amber-500'
                            : 'bg-red-500'
                        }`}
                      />
                      <span className="text-gray-400 capitalize">{node.health}</span>
                    </div>
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-gray-300">{node.load}%</td>
                  <td className="px-5 py-3 text-right font-mono text-gray-300">
                    {node.memory_mb} MB
                  </td>
                  <td className="px-5 py-3 text-right font-mono text-gray-300">
                    {node.replication_lag}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
