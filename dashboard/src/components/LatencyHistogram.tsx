import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import type { Stats } from '../hooks/useStats'

interface LatencyHistogramProps {
  stats: Stats | null
}

// Must match internal/metrics.LatencyHistogramBounds exactly — that's the
// single source of truth for bucket boundaries; this is just the label
// for each index the backend sends.
const BUCKET_LABELS = [
  '<1ms',
  '1-5ms',
  '5-10ms',
  '10-25ms',
  '25-50ms',
  '50-100ms',
  '100-250ms',
  '>250ms',
]

export default function LatencyHistogram({ stats }: LatencyHistogramProps) {
  const counts = stats?.latency_histogram

  if (!counts) {
    return (
      <div className="h-64 flex items-center justify-center text-sm text-gray-600">
        No latency data yet
      </div>
    )
  }

  const data = BUCKET_LABELS.map((range, i) => ({ range, count: counts[i] ?? 0 }))

  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="range"
            stroke="#475569"
            tick={{ fill: '#64748b', fontSize: 10 }}
            tickLine={false}
          />
          <YAxis
            stroke="#475569"
            tick={{ fill: '#64748b', fontSize: 10 }}
            tickLine={false}
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
          <Bar
            dataKey="count"
            fill="#8b5cf6"
            radius={[4, 4, 0, 0]}
            maxBarSize={48}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
