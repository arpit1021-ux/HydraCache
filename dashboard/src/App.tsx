import { useState } from 'react'
import Sidebar from './components/Sidebar'
import Header from './components/Header'
import Dashboard from './pages/Dashboard'
import Nodes from './pages/Nodes'
import HashRing from './pages/HashRing'
import Metrics from './pages/Metrics'
import { useClusterData } from './hooks/useClusterData'
import { useStats } from './hooks/useStats'

export type Page = 'dashboard' | 'nodes' | 'hashring' | 'metrics'

export default function App() {
  const [currentPage, setCurrentPage] = useState<Page>('dashboard')
  const { clusterData, connected: clusterConnected } = useClusterData()
  const { stats, connected: statsConnected } = useStats()

  const connected = clusterConnected && statsConnected

  const renderPage = () => {
    switch (currentPage) {
      case 'dashboard':
        return <Dashboard clusterData={clusterData} stats={stats} />
      case 'nodes':
        return <Nodes clusterData={clusterData} />
      case 'hashring':
        return <HashRing clusterData={clusterData} />
      case 'metrics':
        return <Metrics clusterData={clusterData} stats={stats} />
    }
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar currentPage={currentPage} onNavigate={setCurrentPage} />
      <div className="flex flex-col flex-1 overflow-hidden">
        <Header connected={connected} clusterData={clusterData} />
        {!connected && (
          <div className="bg-amber-500/10 border-b border-amber-500/20 px-6 py-2 text-center shrink-0">
            <span className="text-xs font-semibold text-amber-400 uppercase tracking-wider">
              Demo Data — Disconnected from HydraCache backend
            </span>
          </div>
        )}
        <main className="flex-1 overflow-y-auto p-6">
          {renderPage()}
        </main>
      </div>
    </div>
  )
}
