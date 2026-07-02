import { useEffect, useRef, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import type { Stats } from '../hooks/useStats'

interface MetricsChartProps {
  stats: Stats | null
}

interface DataPoint {
  time: string
  requests: number
  hitRate: number
}

export default function MetricsChart({ stats }: MetricsChartProps) {
  const [data, setData] = useState<DataPoint[]>([])
  const prevHits = useRef(stats?.hits ?? 0)
  const prevMisses = useRef(stats?.misses ?? 0)

  useEffect(() => {
    if (!stats) return

    const now = new Date()
    const timeStr = now.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })

    const hitsDelta = Math.max(0, stats.hits - prevHits.current)
    const missesDelta = Math.max(0, stats.misses - prevMisses.current)
    const totalRequests = hitsDelta + missesDelta
    const requestsPerSec = totalRequests / 2

    prevHits.current = stats.hits
    prevMisses.current = stats.misses

    setData((prev) => {
      const next = [...prev, { time: timeStr, requests: requestsPerSec, hitRate: stats.hit_rate }]
      if (next.length > 60) next.shift()
      return next
    })
  }, [stats])

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="time"
            stroke="#475569"
            tick={{ fill: '#64748b', fontSize: 10 }}
            tickLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="left"
            stroke="#475569"
            tick={{ fill: '#64748b', fontSize: 10 }}
            tickLine={false}
            label={{ value: 'req/s', angle: -90, position: 'insideLeft', fill: '#64748b', fontSize: 10 }}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            stroke="#475569"
            tick={{ fill: '#64748b', fontSize: 10 }}
            tickLine={false}
            domain={[0, 100]}
            label={{ value: '%', angle: 90, position: 'insideRight', fill: '#64748b', fontSize: 10 }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1e293b',
              border: '1px solid #334155',
              borderRadius: '8px',
              color: '#e2e8f0',
              fontSize: 12,
            }}
          />
          <Legend
            wrapperStyle={{ fontSize: 12, color: '#94a3b8' }}
          />
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="requests"
            stroke="#06b6d4"
            strokeWidth={2}
            dot={false}
            name="Requests/sec"
          />
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="hitRate"
            stroke="#10b981"
            strokeWidth={2}
            dot={false}
            name="Hit Rate %"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
