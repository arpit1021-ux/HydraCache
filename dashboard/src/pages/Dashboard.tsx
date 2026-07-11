import { Database, Zap, Activity, AlertTriangle } from 'lucide-react'
import type { ClusterData } from '../hooks/useClusterData'
import type { Stats } from '../hooks/useStats'
import NodeGrid from '../components/NodeGrid'
import MetricsChart from '../components/MetricsChart'
import LatencyHistogram from '../components/LatencyHistogram'
import ReplicationStatus from '../components/ReplicationStatus'
import ClusterLogs from '../components/ClusterLogs'
import RequestFlow from '../components/RequestFlow'

interface DashboardProps {
  clusterData: ClusterData | null
  stats: Stats | null
}

function StatCard({
  icon: Icon,
  label,
  value,
  color,
}: {
  icon: typeof Database
  label: string
  value: string | number
  color: string
}) {
  return (
    <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-4 flex items-center gap-4">
      <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${color}`}>
        <Icon size={20} />
      </div>
      <div>
        <div className="text-xs text-gray-500 uppercase tracking-wider">{label}</div>
        <div className="text-lg font-bold text-white">{value}</div>
      </div>
    </div>
  )
}

export default function Dashboard({ clusterData, stats }: DashboardProps) {
  const nodes = clusterData?.nodes ?? []
  const healthyCount = nodes.filter((n) => n.health === 'alive').length
  const suspectCount = nodes.filter((n) => n.health === 'suspect').length
  const downCount = nodes.filter((n) => n.health === 'dead' || n.health === 'left').length

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          icon={Database}
          label="Total Keys"
          value={stats?.keys?.toLocaleString() ?? '—'}
          color="bg-cyan-500/10 text-cyan-400"
        />
        <StatCard
          icon={Zap}
          label="Hit Rate"
          value={stats ? `${stats.hit_rate.toFixed(1)}%` : '—'}
          color="bg-emerald-500/10 text-emerald-400"
        />
        <StatCard
          icon={Activity}
          label="Cluster Nodes"
          value={`${healthyCount}/${nodes.length}`}
          color="bg-purple-500/10 text-purple-400"
        />
        <StatCard
          icon={AlertTriangle}
          label="Issues"
          value={suspectCount + downCount}
          color={
            suspectCount + downCount > 0
              ? 'bg-amber-500/10 text-amber-400'
              : 'bg-gray-500/10 text-gray-400'
          }
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
            Request Rate & Hit Rate
          </h3>
          <MetricsChart stats={stats} />
        </div>
        <div className="bg-gray-900/50 border border-gray-800 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
            Request Flow
          </h3>
          <RequestFlow nodes={nodes} />
        </div>
      </div>

      <div>
        <h3 className="text-sm font-semibold text-gray-400 mb-4 uppercase tracking-wider">
          Cluster Nodes
        </h3>
        <NodeGrid nodes={nodes} />
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
            Replication Lag
          </h3>
          <ReplicationStatus nodes={nodes} />
        </div>
      </div>

      <ClusterLogs />
    </div>
  )
}
