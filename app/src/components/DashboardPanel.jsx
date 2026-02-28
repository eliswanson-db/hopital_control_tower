import { useState, useEffect, useCallback } from 'react'
import { cn } from '../lib/utils'

function StatCard({ title, value, subtitle, trend, color = 'teal' }) {
  const colorClasses = {
    teal: 'from-teal-500/20 to-teal-600/10 border-teal-500/30',
    red: 'from-red-500/20 to-red-600/10 border-red-500/30',
    green: 'from-green-500/20 to-green-600/10 border-green-500/30',
    blue: 'from-blue-500/20 to-blue-600/10 border-blue-500/30',
  }
  return (
    <div className={cn("p-4 rounded-xl bg-gradient-to-br border", colorClasses[color])}>
      <p className="text-xs text-slate-400 uppercase tracking-wide mb-1">{title}</p>
      <p className="text-2xl font-semibold text-warm-white">{value}</p>
      {subtitle && <p className="text-xs text-slate-400 mt-1">{subtitle}</p>}
      {trend && (
        <p className={cn("text-xs mt-1", trend > 0 ? "text-red-400" : "text-green-400")}>
          {trend > 0 ? '\u2191' : '\u2193'} {Math.abs(trend)}% vs last week
        </p>
      )}
    </div>
  )
}

function TimelineBar({ data }) {
  if (!data || data.length === 0) {
    return <div className="h-24 flex items-center justify-center text-slate-500 text-sm">No timeline data</div>
  }
  const maxEnc = Math.max(...data.map(d => d.encounters), 1)
  return (
    <div className="flex items-end gap-1 h-24">
      {data.map((day, i) => (
        <div key={i} className="flex-1 flex flex-col items-center gap-1">
          <div className="w-full flex flex-col-reverse">
            <div className="w-full bg-teal-500/60 rounded-t"
              style={{ height: `${Math.max(4, (day.encounters - day.readmissions) / maxEnc * 60)}px` }} />
            {day.readmissions > 0 && (
              <div className="w-full bg-red-500/60"
                style={{ height: `${Math.max(2, day.readmissions / maxEnc * 60)}px` }} />
            )}
          </div>
          <span className="text-[10px] text-slate-500 truncate w-full text-center">
            {i % 5 === 0 ? new Date(day.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : ''}
          </span>
        </div>
      ))}
    </div>
  )
}

function ReadmissionItem({ enc }) {
  const losHigh = parseFloat(enc.los_days) > 7
  const costHigh = enc.total_drug_cost > 500
  return (
    <div className="py-2.5 border-b border-slate-700/30 last:border-0">
      <div className="flex items-center justify-between mb-1">
        <p className="text-sm text-warm-white font-medium">{enc.encounter_id}</p>
        <span className={cn("text-xs px-2 py-0.5 rounded",
          losHigh ? "bg-red-500/20 text-red-400" : "bg-slate-500/20 text-slate-400")}>
          LOS: {enc.los_days}d
        </span>
      </div>
      <div className="flex items-center justify-between text-xs text-slate-400">
        <span>{enc.hospital} / {enc.department}</span>
        {enc.total_drug_cost > 0 && (
          <span className={cn(costHigh ? "text-amber-400" : "text-slate-400")}>
            ${enc.total_drug_cost.toLocaleString()}
          </span>
        )}
      </div>
      {enc.payer && (
        <p className="text-[10px] text-slate-500 mt-0.5">{enc.payer} -- discharged {enc.discharge_date || 'N/A'}</p>
      )}
    </div>
  )
}

const TYPE_LABELS = {
  cost_monitoring: 'Drug Cost Alert',
  los_analysis: 'Length of Stay Analysis',
  ed_performance: 'ED Wait Time Alert',
  staffing_analysis: 'Staffing Optimization',
  compliance_monitoring: 'Compliance Check',
  next_best_action_report: 'Next Best Action',
  readiness_check: 'Readiness Check',
}

function RecommendationItem({ rec }) {
  const typeColors = {
    cost_monitoring: 'bg-red-500/20 text-red-400',
    los_analysis: 'bg-blue-500/20 text-blue-400',
    ed_performance: 'bg-amber-500/20 text-amber-400',
    staffing_analysis: 'bg-green-500/20 text-green-400',
    compliance_monitoring: 'bg-purple-500/20 text-purple-400',
    next_best_action_report: 'bg-teal-500/20 text-teal-400',
    readiness_check: 'bg-slate-500/20 text-slate-400',
  }
  const label = TYPE_LABELS[rec.type] || rec.type?.replace(/_/g, ' ')
  return (
    <div className="py-3 border-b border-slate-700/30 last:border-0">
      <div className="flex items-center justify-between mb-1">
        <span className={cn("text-[10px] px-2 py-0.5 rounded",
          typeColors[rec.type] || 'bg-slate-500/20 text-slate-400')}>
          {label}
        </span>
        {rec.timestamp && (
          <span className="text-[10px] text-slate-500">
            {new Date(rec.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </span>
        )}
      </div>
      <p className="text-sm text-warm-white leading-snug">{rec.action}</p>
    </div>
  )
}

function AlertTile({ alert, onClick }) {
  const severityColors = {
    high: 'border-red-500/40 bg-red-500/10 hover:bg-red-500/20',
    medium: 'border-amber-500/40 bg-amber-500/10 hover:bg-amber-500/20',
    low: 'border-blue-500/40 bg-blue-500/10 hover:bg-blue-500/20',
  }
  return (
    <button onClick={onClick}
      className={cn("w-full text-left p-3 rounded-lg border transition-all",
        severityColors[alert.severity] || severityColors.medium)}>
      <p className="text-xs font-medium text-warm-white">{alert.title}</p>
      <p className="text-[10px] text-slate-400 mt-0.5">{alert.detail}</p>
    </button>
  )
}

function EdWaitTile({ data }) {
  if (!data || !data.levels?.length) return null
  const maxWait = Math.max(...data.levels.map(l => l.avg_wait), 1)
  return (
    <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
      <div className="flex items-center justify-between mb-3">
        <h4 className="text-xs font-medium text-slate-400 uppercase">ED Wait by Acuity</h4>
        {data.total_breaches > 0 && (
          <span className="text-[10px] px-2 py-0.5 bg-red-500/20 text-red-400 rounded">
            {data.total_breaches} breaches
          </span>
        )}
      </div>
      <div className="space-y-2">
        {data.levels.map(l => (
          <div key={l.acuity} className="flex items-center gap-2">
            <span className="text-[10px] text-slate-400 w-6">L{l.acuity}</span>
            <div className="flex-1 bg-slate-700/30 rounded-full h-3 overflow-hidden">
              <div className={cn("h-full rounded-full",
                l.breaches > 0 ? "bg-red-500/70" : "bg-teal-500/60")}
                style={{ width: `${Math.max(8, l.avg_wait / maxWait * 100)}%` }} />
            </div>
            <span className="text-[10px] text-slate-300 w-12 text-right">{l.avg_wait}m</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function DrugCostTile({ data }) {
  if (!data || !data.categories?.length) return null
  const topSpend = Math.max(...data.categories.map(c => c.spend), 1)
  return (
    <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
      <h4 className="text-xs font-medium text-slate-400 uppercase mb-1">Drug Costs (30d)</h4>
      <p className="text-lg font-semibold text-warm-white mb-3">${data.total_spend.toLocaleString()}</p>
      <div className="space-y-2">
        {data.categories.map(c => (
          <div key={c.category} className="flex items-center gap-2">
            <span className="text-[10px] text-slate-400 w-20 truncate">{c.category}</span>
            <div className="flex-1 bg-slate-700/30 rounded-full h-2.5 overflow-hidden">
              <div className="h-full rounded-full bg-amber-500/60"
                style={{ width: `${c.spend / topSpend * 100}%` }} />
            </div>
            <span className="text-[10px] text-slate-300 w-14 text-right">${(c.spend / 1000).toFixed(0)}k</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function StaffingTile({ data }) {
  if (!data || !data.departments?.length) return null
  return (
    <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
      <div className="flex items-center justify-between mb-3">
        <h4 className="text-xs font-medium text-slate-400 uppercase">Contract Labor Mix</h4>
        <span className={cn("text-xs font-medium",
          data.overall_contract_pct > 25 ? "text-red-400" : "text-green-400")}>
          {data.overall_contract_pct}% overall
        </span>
      </div>
      <div className="space-y-2">
        {data.departments.slice(0, 4).map(d => (
          <div key={d.department} className="flex items-center gap-2">
            <span className="text-[10px] text-slate-400 w-20 truncate">{d.department}</span>
            <div className="flex-1 bg-slate-700/30 rounded-full h-2.5 overflow-hidden">
              <div className={cn("h-full rounded-full",
                d.contract_pct > 25 ? "bg-red-500/60" : "bg-green-500/60")}
                style={{ width: `${Math.min(100, d.contract_pct)}%` }} />
            </div>
            <span className="text-[10px] text-slate-300 w-10 text-right">{d.contract_pct}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function DashboardPanel({ onQuerySelect }) {
  const [summary, setSummary] = useState(null)
  const [timeline, setTimeline] = useState([])
  const [readmissions, setReadmissions] = useState([])
  const [recommendations, setRecommendations] = useState([])
  const [alerts, setAlerts] = useState([])
  const [edData, setEdData] = useState(null)
  const [drugData, setDrugData] = useState(null)
  const [staffingData, setStaffingData] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchData = useCallback(async () => {
    try {
      const [summaryRes, timelineRes, readmitRes, recsRes, alertsRes, edRes, drugRes, staffRes] = await Promise.all([
        fetch('/api/encounters/summary'),
        fetch('/api/encounters/timeline?days=30'),
        fetch('/api/encounters/readmissions?limit=5'),
        fetch('/api/recommendations/latest?limit=3'),
        fetch('/api/alerts/active'),
        fetch('/api/ed/summary'),
        fetch('/api/drugs/summary'),
        fetch('/api/staffing/summary'),
      ])
      const [summaryData, timelineData, readmitData, recsData, alertsData, edD, drugD, staffD] = await Promise.all([
        summaryRes.json(), timelineRes.json(), readmitRes.json(), recsRes.json(), alertsRes.json(),
        edRes.json(), drugRes.json(), staffRes.json(),
      ])
      setSummary(summaryData.summary)
      setTimeline(timelineData.timeline || [])
      setReadmissions(readmitData.readmissions || [])
      setRecommendations(recsData.recommendations || [])
      setAlerts(alertsData.alerts || [])
      setEdData(edD)
      setDrugData(drugD)
      setStaffingData(staffD)
    } catch (err) {
      console.error('Dashboard fetch error:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 60000)
    return () => clearInterval(interval)
  }, [fetchData])

  if (loading) {
    return (
      <div className="w-96 flex-shrink-0 bg-slate-900/30 border-l border-slate-700/30 p-4">
        <div className="animate-pulse space-y-4">
          <div className="h-20 bg-slate-800/50 rounded-xl" />
          <div className="h-20 bg-slate-800/50 rounded-xl" />
          <div className="h-32 bg-slate-800/50 rounded-xl" />
        </div>
      </div>
    )
  }

  return (
    <div className="w-96 flex-shrink-0 bg-slate-900/30 border-l border-slate-700/30 overflow-y-auto custom-scrollbar">
      <div className="p-4 space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-warm-white">Dashboard</h3>
          <button onClick={fetchData} className="text-xs text-slate-400 hover:text-white transition-colors">Refresh</button>
        </div>

        <div className="grid grid-cols-3 gap-2">
          <StatCard title="Encounters"
            value={summary?.total_encounters || 0}
            trend={summary?.trends?.enc_trend}
            color="blue" />
          <StatCard title="Avg LOS"
            value={`${summary?.avg_los || 0}d`}
            trend={summary?.trends?.los_trend}
            color={parseFloat(summary?.avg_los) > 5 ? 'red' : parseFloat(summary?.avg_los) > 4 ? 'teal' : 'green'} />
          <StatCard title="Readmit Rate"
            value={`${summary?.readmission_rate || 0}%`}
            subtitle={`${summary?.readmissions || 0} total`}
            trend={summary?.trends?.readmit_trend}
            color={parseFloat(summary?.readmission_rate) > 10 ? 'red' : 'green'} />
        </div>

        {alerts.length > 0 && (
          <div className="space-y-2">
            <h4 className="text-xs font-medium text-slate-400 uppercase">Active Alerts</h4>
            {alerts.map((a, i) => (
              <AlertTile key={i} alert={a} onClick={() => onQuerySelect?.(
                `Investigate the ${a.title.toLowerCase()} issue: ${a.detail}`
              )} />
            ))}
          </div>
        )}

        <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
          <h4 className="text-xs font-medium text-slate-400 uppercase mb-3">30-Day Encounter Volume</h4>
          <TimelineBar data={timeline} />
          <div className="flex items-center gap-4 mt-2 text-[10px] text-slate-500">
            <span className="flex items-center gap-1"><div className="w-2 h-2 bg-teal-500/60 rounded" /> Encounters</span>
            <span className="flex items-center gap-1"><div className="w-2 h-2 bg-red-500/60 rounded" /> Readmissions</span>
          </div>
        </div>

        <EdWaitTile data={edData} />
        <DrugCostTile data={drugData} />
        <StaffingTile data={staffingData} />

        <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
          <h4 className="text-xs font-medium text-slate-400 uppercase mb-2">Recent Readmissions</h4>
          {readmissions.length > 0 ? (
            readmissions.map((enc, i) => <ReadmissionItem key={i} enc={enc} />)
          ) : (
            <p className="text-sm text-slate-500 py-2">No recent readmissions</p>
          )}
        </div>

        <div className="bg-slate-800/30 rounded-xl p-4 border border-slate-700/30">
          <h4 className="text-xs font-medium text-slate-400 uppercase mb-2">Recommended Actions</h4>
          {recommendations.length > 0 ? (
            recommendations.map((rec, i) => <RecommendationItem key={i} rec={rec} />)
          ) : (
            <p className="text-sm text-slate-500 py-2">No recommendations yet</p>
          )}
        </div>
      </div>
    </div>
  )
}
