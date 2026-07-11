import type { ClusterData } from '../hooks/useClusterData'
import type { Stats } from '../hooks/useStats'
import MetricsChart from '../components/MetricsChart'
import LatencyHistogram from '../components/LatencyHistogram'
import ReplicationStatus from '../components/ReplicationStatus'

interface MetricsProps {
  clusterData: ClusterData | null
  stats: Stats | null
}

export default function Metrics({ clusterData, stats }: MetricsProps) {
  const nodes = clusterData?.nodes ?? []

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-lg font-bold text-white mb-1">Metrics Overview</h2>
        <p className="text-sm text-gray-500">
          Real-time performance metrics for the HydraCache cluster
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Total Keys</div>
          <div className="text-2xl font-bold text-white">{stats?.keys?.toLocaleString() ?? '—'}</div>
        </div>
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Hit Rate</div>
          <div className="text-2xl font-bold text-emerald-400">{stats ? `${stats.hit_rate.toFixed(1)}%` : '—'}</div>
        </div>
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Total Requests</div>
          <div className="text-2xl font-bold text-cyan-400">
            {stats ? (stats.hits + stats.misses).toLocaleString() : '—'}
          </div>
        </div>
      </div>

      <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
        <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
          Request Rate & Hit Rate Over Time
        </h3>
        <MetricsChart stats={stats} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
            Latency Distribution
          </h3>
          <LatencyHistogram />
        </div>
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
            Replication Lag Per Node
          </h3>
          <ReplicationStatus nodes={nodes} />
        </div>
      </div>

      <div className="bg-gray-900/50 border border-gray-800 rounded-xl overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-800">
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
            Node Performance Summary
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-gray-500 uppercase tracking-wider border-b border-gray-800">
                <th className="text-left px-5 py-3 font-medium">Node</th>
                <th className="text-right px-5 py-3 font-medium">CPU Load</th>
                <th className="text-right px-5 py-3 font-medium">Memory</th>
                <th className="text-right px-5 py-3 font-medium">Replication Lag</th>
              </tr>
            </thead>
            <tbody>
              {nodes.map((node) => {
                return (
                  <tr
                    key={node.id}
                    className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors"
                  >
                    <td className="px-5 py-3 font-mono text-white">{node.id}</td>
                    <td className="px-5 py-3 text-right font-mono text-gray-300">{node.load}%</td>
                    <td className="px-5 py-3 text-right font-mono text-gray-300">
                      {node.memory_mb} MB
                    </td>
                    <td className="px-5 py-3 text-right font-mono text-gray-300">
                      {node.replication_lag}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
