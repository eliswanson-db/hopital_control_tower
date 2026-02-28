import { useState, useEffect, useCallback } from 'react'
import ContextCard from './ContextCard'
import { cn } from '../lib/utils'

function TimelineItem({ action }) {
  const getTypeColor = (type) => {
    const colors = {
      realtime_monitoring: 'bg-blue-500/20 text-blue-400',
      root_cause_analysis: 'bg-red-500/20 text-red-400',
      next_best_action_report: 'bg-amber-500/20 text-amber-400',
      equipment_health: 'bg-green-500/20 text-green-400',
      compliance_monitoring: 'bg-purple-500/20 text-purple-400',
      strategy_optimization: 'bg-teal-500/20 text-teal-400',
      learning_reflection: 'bg-slate-500/20 text-slate-400',
    }
    return colors[type] || 'bg-slate-500/20 text-slate-400'
  }

  const formatTime = (timestamp) => {
    const date = new Date(timestamp)
    const now = new Date()
    const diffMs = now - date
    const diffMins = Math.floor(diffMs / 60000)
    
    if (diffMins < 1) return 'Just now'
    if (diffMins < 60) return `${diffMins}m ago`
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h ago`
    return date.toLocaleDateString()
  }

  return (
    <div className="flex gap-3 items-start">
      <div className="flex-shrink-0 w-2 h-2 rounded-full bg-amber-500 mt-2" />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className={cn(
            "text-[10px] px-2 py-0.5 rounded uppercase font-medium",
            getTypeColor(action.analysis_type)
          )}>
            {action.analysis_type?.replace(/_/g, ' ')}
          </span>
          <span className="text-xs text-slate-500">
            {formatTime(action.created_at)}
          </span>
        </div>
        <p className="text-sm text-warm-white leading-snug line-clamp-2">
          {action.insights}
        </p>
      </div>
    </div>
  )
}

export default function AutonomousPanel({ autonomousStatus, alerts, onDismissAlert }) {
  const [recentActions, setRecentActions] = useState([])
  const [loading, setLoading] = useState(true)

  const fetchRecentActions = useCallback(async () => {
    try {
      const res = await fetch('/api/analysis/latest?limit=5')
      if (res.ok) {
        const data = await res.json()
        setRecentActions(data.analyses || [])
      }
    } catch (err) {
      console.error('Failed to fetch recent actions:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchRecentActions()
    const interval = setInterval(fetchRecentActions, 30000)
    return () => clearInterval(interval)
  }, [fetchRecentActions])

  const getStatusColor = () => {
    if (!autonomousStatus) return 'text-slate-500'
    return autonomousStatus.is_running ? 'text-living-green' : 'text-slate-500'
  }

  const getStatusText = () => {
    if (!autonomousStatus) return 'Unknown'
    if (autonomousStatus.is_paused) return 'Paused'
    if (autonomousStatus.is_running) return 'Active'
    return 'Stopped'
  }

  return (
    <div className="w-80 flex-shrink-0 bg-slate-900/30 border-r border-slate-700/30 overflow-y-auto custom-scrollbar">
      <div className="p-4 space-y-4">
        {/* Header */}
        <div>
          <h3 className="text-sm font-semibold text-warm-white mb-1">Autonomous Agent</h3>
          <div className="flex items-center gap-2">
            <div className={cn(
              "w-2 h-2 rounded-full",
              autonomousStatus?.is_running ? "bg-living-green animate-pulse" : "bg-slate-500"
            )} />
            <span className={cn("text-xs font-medium", getStatusColor())}>
              {getStatusText()}
            </span>
          </div>
          {autonomousStatus?.last_execution && (
            <p className="text-xs text-slate-500 mt-1">
              Last run: {new Date(autonomousStatus.last_execution).toLocaleTimeString()}
            </p>
          )}
        </div>

        {/* Active Alerts */}
        {alerts && alerts.length > 0 && (
          <div>
            <h4 className="text-xs font-medium text-slate-400 uppercase mb-2">Active Alerts</h4>
            <div className="space-y-2">
              {alerts.map(alert => (
                <ContextCard 
                  key={alert.id}
                  type="alert"
                  severity={alert.severity}
                  title={alert.title}
                  content={alert.content}
                  actions={alert.actions}
                  onDismiss={() => onDismissAlert(alert.id)}
                  compact
                />
              ))}
            </div>
          </div>
        )}

        {/* Recent Actions Timeline */}
        <div>
          <h4 className="text-xs font-medium text-slate-400 uppercase mb-3">Recent Actions</h4>
          {loading ? (
            <div className="space-y-3">
              {[1, 2, 3].map(i => (
                <div key={i} className="animate-pulse space-y-2">
                  <div className="h-3 bg-slate-800/50 rounded w-3/4" />
                  <div className="h-2 bg-slate-800/50 rounded w-full" />
                </div>
              ))}
            </div>
          ) : recentActions.length > 0 ? (
            <div className="space-y-4">
              {recentActions.map((action, i) => (
                <TimelineItem key={action.id || i} action={action} />
              ))}
            </div>
          ) : (
            <p className="text-sm text-slate-500">No recent actions</p>
          )}
        </div>

        {/* Execution Stats */}
        {autonomousStatus && (
          <div className="bg-slate-800/30 rounded-xl p-3 border border-slate-700/30">
            <h4 className="text-xs font-medium text-slate-400 uppercase mb-2">Statistics</h4>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <p className="text-xs text-slate-500">Total Runs</p>
                <p className="text-lg font-semibold text-warm-white">
                  {autonomousStatus.execution_count || 0}
                </p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Interval</p>
                <p className="text-lg font-semibold text-warm-white">
                  {Math.floor((autonomousStatus.interval_seconds || 60) / 60)}m
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Capabilities */}
        {autonomousStatus?.capabilities && (
          <div>
            <h4 className="text-xs font-medium text-slate-400 uppercase mb-2">Capabilities</h4>
            <div className="space-y-1">
              {autonomousStatus.capabilities
                .filter(c => c.enabled)
                .sort((a, b) => b.weight - a.weight)
                .map(cap => (
                  <div key={cap.id} className="flex items-center justify-between text-xs">
                    <span className="text-slate-300">{cap.name}</span>
                    <span className="text-slate-500">{cap.weight}%</span>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
