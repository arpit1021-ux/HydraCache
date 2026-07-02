import { useEffect, useState } from 'react'

interface LogEntry {
  id: number
  timestamp: string
  level: 'info' | 'warn' | 'error'
  message: string
}

const LOG_TEMPLATES = [
  { level: 'info' as const, msg: 'Node {node} joined cluster' },
  { level: 'info' as const, msg: 'Replication sync completed for {node}' },
  { level: 'info' as const, msg: 'Health check passed for {node}' },
  { level: 'info' as const, msg: 'Epoch incremented to {epoch}' },
  { level: 'info' as const, msg: 'Key eviction completed: {count} keys removed' },
  { level: 'info' as const, msg: 'Gossip protocol heartbeat from {node}' },
  { level: 'warn' as const, msg: 'High memory usage on {node}: {pct}%' },
  { level: 'warn' as const, msg: 'Replication lag increasing on {node}' },
  { level: 'warn' as const, msg: 'Connection pool near capacity on {node}' },
  { level: 'error' as const, msg: 'Failed to reach {node}: connection timeout' },
  { level: 'error' as const, msg: 'Split-brain detection triggered on {node}' },
]

const NODES = ['node-0', 'node-1', 'node-2', 'node-3', 'node-4', 'node-5']

function randomTemplate(): LogEntry {
  const tmpl = LOG_TEMPLATES[Math.floor(Math.random() * LOG_TEMPLATES.length)]
  const node = NODES[Math.floor(Math.random() * NODES.length)]
  const now = new Date()
  const timestamp = now.toLocaleTimeString('en-US', { hour12: false }) + '.' + String(now.getMilliseconds()).padStart(3, '0')

  let msg = tmpl.msg
    .replace('{node}', node)
    .replace('{epoch}', String(Math.floor(Date.now() / 1000)))
    .replace('{count}', String(Math.floor(Math.random() * 500)))
    .replace('{pct}', String(70 + Math.floor(Math.random() * 25)))

  return {
    id: Date.now() + Math.random(),
    timestamp,
    level: tmpl.level,
    message: msg,
  }
}

export default function ClusterLogs() {
  const [logs, setLogs] = useState<LogEntry[]>(() =>
    Array.from({ length: 15 }, randomTemplate)
  )

  useEffect(() => {
    const interval = setInterval(() => {
      setLogs((prev) => {
        const next = [...prev, randomTemplate()]
        if (next.length > 50) next.shift()
        return next
      })
    }, 1500)
    return () => clearInterval(interval)
  }, [])

  const levelStyles = {
    info: 'text-cyan-400',
    warn: 'text-amber-400',
    error: 'text-red-400',
  }

  return (
    <div className="bg-gray-900/50 rounded-xl border border-gray-800 overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-2">
        <div className="w-2 h-2 rounded-full bg-cyan-500 animate-pulse" />
        <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
          Cluster Logs
        </span>
      </div>
      <div className="h-64 overflow-y-auto font-mono text-[11px] leading-relaxed p-3 space-y-0.5">
        {logs.map((log) => (
          <div key={log.id} className="flex gap-2 animate-fade-in">
            <span className="text-gray-600 shrink-0">{log.timestamp}</span>
            <span className={`${levelStyles[log.level]} shrink-0 uppercase font-bold w-12`}>
              {log.level}
            </span>
            <span className="text-gray-300">{log.message}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
