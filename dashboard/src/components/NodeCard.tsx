import { Server, Cpu, HardDrive, Crown } from 'lucide-react'
import type { ClusterNode } from '../hooks/useClusterData'

interface NodeCardProps {
  node: ClusterNode
}

function healthColor(status: string): string {
  switch (status) {
    case 'healthy':
      return 'border-emerald-500/30 bg-emerald-500/5'
    case 'degraded':
      return 'border-amber-500/30 bg-amber-500/5'
    case 'down':
      return 'border-red-500/30 bg-red-500/5'
    default:
      return 'border-gray-700 bg-gray-800/50'
  }
}

function statusDot(status: string): string {
  switch (status) {
    case 'healthy':
      return 'bg-emerald-500'
    case 'degraded':
      return 'bg-amber-500'
    case 'down':
      return 'bg-red-500'
    default:
      return 'bg-gray-500'
  }
}

export default function NodeCard({ node }: NodeCardProps) {
  const isLeader = node.role === 'leader'

  return (
    <div
      className={`relative rounded-xl border p-4 transition-all hover:scale-[1.01] ${healthColor(
        node.status
      )}`}
    >
      {isLeader && (
        <div className="absolute -top-2 -right-2 w-6 h-6 rounded-full bg-amber-500 flex items-center justify-center">
          <Crown size={12} className="text-white" />
        </div>
      )}
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-lg bg-gray-800 flex items-center justify-center">
            <Server size={16} className="text-gray-400" />
          </div>
          <div>
            <div className="text-sm font-semibold text-white">{node.id}</div>
            <div className="text-xs text-gray-500 font-mono">{node.address}</div>
          </div>
        </div>
        <div className="flex items-center gap-1.5">
          <div className={`w-2 h-2 rounded-full ${statusDot(node.status)}`} />
          <span className="text-xs text-gray-400 capitalize">{node.status}</span>
        </div>
      </div>

      <div className="flex items-center gap-1 mb-3">
        <span
          className={`text-[10px] uppercase font-bold tracking-wider px-1.5 py-0.5 rounded ${
            isLeader
              ? 'bg-amber-500/20 text-amber-300'
              : 'bg-gray-700 text-gray-400'
          }`}
        >
          {node.role}
        </span>
      </div>

      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <Cpu size={12} className="text-gray-500" />
          <div className="flex-1">
            <div className="flex justify-between text-[11px] mb-0.5">
              <span className="text-gray-400">CPU</span>
              <span className="text-gray-300">{node.cpu_load}%</span>
            </div>
            <div className="h-1 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${
                  node.cpu_load > 80
                    ? 'bg-red-500'
                    : node.cpu_load > 60
                    ? 'bg-amber-500'
                    : 'bg-emerald-500'
                }`}
                style={{ width: `${Math.min(node.cpu_load, 100)}%` }}
              />
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <HardDrive size={12} className="text-gray-500" />
          <div className="flex-1">
            <div className="flex justify-between text-[11px] mb-0.5">
              <span className="text-gray-400">Memory</span>
              <span className="text-gray-300">
                {node.memory_used_mb} / {node.memory_total_mb} MB
              </span>
            </div>
            <div className="h-1 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${
                  node.memory_used_mb / node.memory_total_mb > 0.8
                    ? 'bg-red-500'
                    : node.memory_used_mb / node.memory_total_mb > 0.6
                    ? 'bg-amber-500'
                    : 'bg-cyan-500'
                }`}
                style={{
                  width: `${
                    Math.min((node.memory_used_mb / node.memory_total_mb) * 100, 100)
                  }%`,
                }}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
