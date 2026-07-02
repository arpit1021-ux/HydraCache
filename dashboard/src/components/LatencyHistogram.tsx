import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'

const latencyData = [
  { range: '<1ms', count: 1240 },
  { range: '1-5ms', count: 3580 },
  { range: '5-10ms', count: 2890 },
  { range: '10-25ms', count: 1420 },
  { range: '25-50ms', count: 680 },
  { range: '50-100ms', count: 320 },
  { range: '100-250ms', count: 85 },
  { range: '>250ms', count: 12 },
]

export default function LatencyHistogram() {
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={latencyData}>
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
