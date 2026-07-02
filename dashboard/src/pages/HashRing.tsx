import type { ClusterData } from '../hooks/useClusterData'
import HashRingVisual from '../components/HashRingVisual'

interface HashRingProps {
  clusterData: ClusterData | null
}

export default function HashRing({ clusterData }: HashRingProps) {
  const nodes = clusterData?.nodes ?? []

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-lg font-bold text-white mb-1">Hash Ring</h2>
        <p className="text-sm text-gray-500">
          Consistent hashing ring with {nodes.length} nodes &middot; 8 virtual nodes per node
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-8 flex items-center justify-center">
          {nodes.length > 0 ? (
            <HashRingVisual nodes={nodes} size={380} />
          ) : (
            <div className="text-gray-600 text-sm">No nodes available</div>
          )}
        </div>

        <div className="space-y-6">
          <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3 uppercase tracking-wider">
              Ring Distribution
            </h3>
            <div className="space-y-3">
              {nodes.map((node, i) => {
                const colors = [
                  'bg-cyan-500',
                  'bg-purple-500',
                  'bg-amber-500',
                  'bg-red-500',
                  'bg-emerald-500',
                  'bg-pink-500',
                ]
                const share = ((1 / nodes.length) * 100).toFixed(1)
                return (
                  <div key={node.id} className="flex items-center gap-3">
                    <div className={`w-3 h-3 rounded-full ${colors[i % colors.length]}`} />
                    <span className="text-xs font-mono text-gray-400 w-16">{node.id}</span>
                    <div className="flex-1 h-2 bg-gray-800 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${colors[i % colors.length]}`}
                        style={{ width: `${share}%`, opacity: 0.7 }}
                      />
                    </div>
                    <span className="text-xs text-gray-500 font-mono w-14 text-right">
                      {share}%
                    </span>
                  </div>
                )
              })}
            </div>
          </div>

          <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-400 mb-3 uppercase tracking-wider">
              Ring Configuration
            </h3>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Virtual Nodes</span>
                <span className="text-gray-300 font-mono">8 per node</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Hash Function</span>
                <span className="text-gray-300 font-mono">FNV-1a</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Key Range</span>
                <span className="text-gray-300 font-mono">0x0 - 0xFFFFFFFF</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Total Partitions</span>
                <span className="text-gray-300 font-mono">{nodes.length * 8}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
