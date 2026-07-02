import { Wifi, WifiOff } from 'lucide-react'
import type { ClusterData } from '../hooks/useClusterData'
import LeaderBadge from './LeaderBadge'

interface HeaderProps {
  connected: boolean
  clusterData: ClusterData | null
}

export default function Header({ connected, clusterData }: HeaderProps) {
  const leaderNode = clusterData?.nodes.find((n) => n.role === 'leader')

  return (
    <header className="h-14 bg-gray-900/80 backdrop-blur border-b border-gray-800 flex items-center justify-between px-6 shrink-0">
      <div className="flex items-center gap-4">
        <h1 className="text-sm font-semibold text-gray-300 tracking-wide uppercase">
          HydraCache Cluster Monitor
        </h1>
        {leaderNode && <LeaderBadge node={leaderNode} />}
      </div>
      <div className="flex items-center gap-3">
        <div className="text-xs text-gray-500 hidden sm:block">
          Epoch: {clusterData?.epoch ?? '—'}
        </div>
        <div
          className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
            connected
              ? 'bg-emerald-500/10 text-emerald-400'
              : 'bg-red-500/10 text-red-400'
          }`}
        >
          {connected ? (
            <>
              <span className="relative flex h-2 w-2">
                <span className="animate-pulse-ring absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
              </span>
              <Wifi size={14} />
              Connected
            </>
          ) : (
            <>
              <WifiOff size={14} />
              Disconnected
            </>
          )}
        </div>
      </div>
    </header>
  )
}
