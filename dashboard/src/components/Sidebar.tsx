import { LayoutDashboard, Server, Circle, BarChart3 } from 'lucide-react'
import type { Page } from '../App'

interface SidebarProps {
  currentPage: Page
  onNavigate: (page: Page) => void
}

const navItems: { id: Page; label: string; icon: typeof LayoutDashboard }[] = [
  { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { id: 'nodes', label: 'Nodes', icon: Server },
  { id: 'hashring', label: 'Hash Ring', icon: Circle },
  { id: 'metrics', label: 'Metrics', icon: BarChart3 },
]

export default function Sidebar({ currentPage, onNavigate }: SidebarProps) {
  return (
    <aside className="w-16 lg:w-56 bg-gray-900 border-r border-gray-800 flex flex-col shrink-0">
      <div className="h-14 flex items-center justify-center lg:justify-start lg:px-5 border-b border-gray-800">
        <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-white font-bold text-sm">
          H
        </div>
        <span className="hidden lg:block ml-3 font-semibold text-white text-sm tracking-wide">
          HydraCache
        </span>
      </div>

      <nav className="flex-1 py-3 px-2">
        {navItems.map(({ id, label, icon: Icon }) => {
          const active = currentPage === id
          return (
            <button
              key={id}
              onClick={() => onNavigate(id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg mb-1 text-sm font-medium transition-colors ${
                active
                  ? 'bg-cyan-500/10 text-cyan-400'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800'
              }`}
            >
              <Icon size={20} />
              <span className="hidden lg:block">{label}</span>
            </button>
          )
        })}
      </nav>

      <div className="px-3 py-4 border-t border-gray-800">
        <div className="hidden lg:block text-xs text-gray-500 text-center">
          v1.0.0
        </div>
      </div>
    </aside>
  )
}
