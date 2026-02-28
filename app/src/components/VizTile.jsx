import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts'
import { cn } from '../lib/utils'

const COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6']

function AnalysisTile({ data, onTriggerAnalysis, heartbeatCapabilities }) {
  if (!data || data.length === 0) {
    return (
      <div className="text-center py-8">
        <p className="text-slate-400 mb-4">No analysis data yet</p>
        <div className="flex flex-wrap gap-2 justify-center">
          {heartbeatCapabilities.map((cap) => (
            <button
              key={cap}
              onClick={() => onTriggerAnalysis(cap)}
              className="px-3 py-1.5 text-xs bg-medops-500/20 text-medops-400 rounded-lg hover:bg-medops-500/30 transition-colors"
            >
              Run {cap.replace(/_/g, ' ')}
            </button>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {data.map((analysis, idx) => (
        <div key={analysis.id || idx} className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
          <div className="flex items-start justify-between mb-2">
            <div className="flex items-center gap-2">
              <span className={cn(
                "px-2 py-0.5 rounded text-xs font-medium",
                analysis.analysis_type === 'cost_spike' ? "bg-red-500/20 text-red-400" :
                analysis.analysis_type === 'los_analysis' ? "bg-blue-500/20 text-blue-400" :
                "bg-amber-500/20 text-amber-400"
              )}>
                {analysis.analysis_type?.replace(/_/g, ' ')}
              </span>
              <span className="text-xs text-slate-500">
                {analysis.agent_mode}
              </span>
            </div>
            <span className="text-xs text-slate-500">
              {analysis.created_at ? new Date(analysis.created_at).toLocaleString() : ''}
            </span>
          </div>
          <p className="text-sm text-slate-300 line-clamp-3">{analysis.insights}</p>
          {analysis.recommendations && (
            <p className="text-xs text-medops-400 mt-2 line-clamp-2">
              Recommendation: {analysis.recommendations}
            </p>
          )}
        </div>
      ))}
      
      <div className="flex gap-2 pt-2">
        {heartbeatCapabilities.slice(0, 3).map((cap) => (
          <button
            key={cap}
            onClick={() => onTriggerAnalysis(cap)}
            className="flex-1 px-2 py-1.5 text-xs bg-slate-700/50 text-slate-300 rounded-lg hover:bg-slate-700 transition-colors"
          >
            {cap.replace(/_/g, ' ')}
          </button>
        ))}
      </div>
    </div>
  )
}

function MetricsTile({ summary, equipmentStats, strategyStats }) {
  const hospitalChartData = equipmentStats.map(e => ({
    name: e.hospital || e.equipment,
    encounters: parseInt(e.encounter_count || e.batch_count) || 0,
    readmissions: parseInt(e.readmission_count || e.fault_count) || 0,
  }))

  const departmentChartData = strategyStats.map(s => ({
    name: s.department || s.control_strategy,
    value: parseInt(s.encounter_count || s.batch_count) || 0,
  }))

  return (
    <div className="space-y-6">
      {/* Summary Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="text-center p-3 bg-slate-800/30 rounded-xl">
          <div className="text-2xl font-display font-bold text-white">
            {summary.total_encounters || '0'}
          </div>
          <div className="text-xs text-slate-400">Total Encounters</div>
        </div>
        <div className="text-center p-3 bg-slate-800/30 rounded-xl">
          <div className="text-2xl font-display font-bold text-red-400">
            {summary.readmissions || '0'}
          </div>
          <div className="text-xs text-slate-400">Readmissions</div>
        </div>
        <div className="text-center p-3 bg-slate-800/30 rounded-xl">
          <div className="text-2xl font-display font-bold text-medops-400">
            {summary.readmission_rate || '0'}%
          </div>
          <div className="text-xs text-slate-400">Readmit Rate</div>
        </div>
        <div className="text-center p-3 bg-slate-800/30 rounded-xl">
          <div className="text-2xl font-display font-bold text-blue-400">
            {summary.hospital_count || '0'}
          </div>
          <div className="text-xs text-slate-400">Hospitals</div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-2 gap-4">
        {/* Hospitals Bar Chart */}
        <div className="bg-slate-800/30 rounded-xl p-4">
          <h4 className="text-sm font-medium text-slate-300 mb-3">By Hospital</h4>
          {hospitalChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={150}>
              <BarChart data={hospitalChartData} layout="vertical">
                <XAxis type="number" stroke="#64748b" fontSize={10} />
                <YAxis dataKey="name" type="category" stroke="#64748b" fontSize={10} width={60} />
                <Tooltip 
                  contentStyle={{ background: '#1e293b', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#94a3b8' }}
                />
                <Bar dataKey="encounters" fill="#22c55e" radius={[0, 4, 4, 0]} />
                <Bar dataKey="readmissions" fill="#ef4444" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[150px] flex items-center justify-center text-slate-500 text-sm">
              No data
            </div>
          )}
        </div>

        {/* Strategy Pie Chart */}
        <div className="bg-slate-800/30 rounded-xl p-4">
          <h4 className="text-sm font-medium text-slate-300 mb-3">By Department</h4>
          {departmentChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={150}>
              <PieChart>
                <Pie
                  data={departmentChartData}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  innerRadius={30}
                  outerRadius={55}
                  paddingAngle={2}
                >
                  {departmentChartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip 
                  contentStyle={{ background: '#1e293b', border: 'none', borderRadius: '8px' }}
                />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[150px] flex items-center justify-center text-slate-500 text-sm">
              No data
            </div>
          )}
          <div className="flex flex-wrap gap-2 mt-2">
            {departmentChartData.map((s, i) => (
              <div key={s.name} className="flex items-center gap-1 text-xs">
                <div className="w-2 h-2 rounded-full" style={{ background: COLORS[i % COLORS.length] }} />
                <span className="text-slate-400 truncate max-w-[80px]">{s.name}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

export default function VizTile({ title, type, ...props }) {
  return (
    <div className="glass-card p-6">
      <h3 className="text-lg font-display font-medium text-white mb-4 flex items-center gap-2">
        {type === 'analysis' ? (
          <svg className="w-5 h-5 text-medops-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
        ) : (
          <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 8v8m-4-5v5m-4-2v2m-2 4h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
        )}
        {title}
      </h3>
      
      {type === 'analysis' ? (
        <AnalysisTile {...props} />
      ) : (
        <MetricsTile {...props} />
      )}
    </div>
  )
}

