import { cn } from '../lib/utils'

function AlertCard({ severity, title, content, actions, onDismiss }) {
  const severityStyles = {
    critical: 'bg-soft-red/10 border-soft-red/30 text-soft-red',
    warning: 'bg-amber-500/10 border-amber-500/30 text-amber-400',
    info: 'bg-teal-500/10 border-teal-500/30 text-teal-400',
  }

  return (
    <div className={cn(
      "max-w-4xl rounded-xl border p-4",
      severityStyles[severity] || severityStyles.warning
    )}>
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <div className="mt-0.5">
            {severity === 'critical' ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            )}
          </div>
          <div>
            <h4 className="font-medium">{title}</h4>
            <p className="mt-1 text-sm opacity-90">{content}</p>
          </div>
        </div>
        {onDismiss && (
          <button onClick={onDismiss} className="opacity-60 hover:opacity-100 transition-opacity">
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>
      {actions && actions.length > 0 && (
        <div className="mt-3 flex gap-2">
          {actions.map((action, i) => (
            <button
              key={i}
              onClick={action.onClick}
              className="px-3 py-1.5 text-sm bg-white/10 rounded-lg hover:bg-white/20 transition-colors"
            >
              {action.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

function BatchCard({ batchId, equipment, phase, phaseProgress, strategy, healthScore, goldenAdherence, anomalyScore, topContributors }) {
  const getHealthColor = (score) => {
    if (score >= 80) return 'text-living-green'
    if (score >= 60) return 'text-amber-400'
    return 'text-soft-red'
  }

  return (
    <div className="max-w-4xl bg-slate-800/40 border border-slate-700/30 rounded-xl p-5">
      <div className="flex items-center justify-between mb-4">
        <h4 className="font-medium text-warm-white">Batch {batchId}</h4>
        <span className="text-sm text-teal-400 bg-teal-500/20 px-2 py-0.5 rounded">{equipment}</span>
      </div>
      
      <div className="space-y-3">
        <div>
          <div className="flex items-center justify-between text-sm mb-1">
            <span className="text-slate-400">Phase: {phase}</span>
            <span className="text-slate-300">{phaseProgress}%</span>
          </div>
          <div className="h-2 bg-slate-700/50 rounded-full overflow-hidden">
            <div 
              className="h-full bg-gradient-to-r from-teal-500 to-living-green rounded-full transition-all"
              style={{ width: `${phaseProgress}%` }}
            />
          </div>
        </div>
        
        <div className="grid grid-cols-2 gap-4 pt-2">
          <div>
            <p className="text-xs text-slate-500 mb-1">Strategy</p>
            <p className="text-sm text-slate-300">{strategy}</p>
          </div>
          <div>
            <p className="text-xs text-slate-500 mb-1">Health Score</p>
            <p className={cn("text-sm font-medium", getHealthColor(healthScore))}>{healthScore}/100</p>
          </div>
          <div>
            <p className="text-xs text-slate-500 mb-1">Golden Adherence</p>
            <p className="text-sm text-slate-300">{goldenAdherence}%</p>
          </div>
          <div>
            <p className="text-xs text-slate-500 mb-1">Anomaly Score</p>
            <p className="text-sm text-slate-300">{anomalyScore?.toFixed(2) || '--'}</p>
          </div>
        </div>
        
        {topContributors && topContributors.length > 0 && (
          <div className="pt-2 border-t border-slate-700/30">
            <p className="text-xs text-slate-500 mb-2">Top contributors (stable):</p>
            <div className="flex flex-wrap gap-2">
              {topContributors.map((c, i) => (
                <span key={i} className="text-xs text-slate-400 bg-slate-700/30 px-2 py-1 rounded">
                  {c.name}: {(c.contribution * 100).toFixed(0)}%
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function InsightCard({ analysisType, content, recommendations, timestamp, compact }) {
  const typeStyles = {
    fault_trend: { bg: 'bg-soft-red/10', border: 'border-soft-red/30', text: 'text-soft-red' },
    fault_prediction: { bg: 'bg-soft-red/10', border: 'border-soft-red/30', text: 'text-soft-red' },
    equipment_health: { bg: 'bg-teal-500/10', border: 'border-teal-500/30', text: 'text-teal-400' },
    equipment_utilization: { bg: 'bg-teal-500/10', border: 'border-teal-500/30', text: 'text-teal-400' },
    strategy_optimization: { bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400' },
    compliance_monitoring: { bg: 'bg-purple-500/10', border: 'border-purple-500/30', text: 'text-purple-400' },
    kpi_summary: { bg: 'bg-slate-500/10', border: 'border-slate-500/30', text: 'text-slate-400' },
  }

  const style = typeStyles[analysisType] || typeStyles.kpi_summary
  const formatType = (type) => type?.replace(/_/g, ' ') || 'Analysis'

  if (compact) {
    return (
      <div className={cn("rounded-lg border px-4 py-3", style.bg, style.border)}>
        <div className="flex items-center justify-between mb-1">
          <span className={cn("text-xs font-medium uppercase tracking-wide", style.text)}>
            {formatType(analysisType)}
          </span>
          {timestamp && (
            <span className="text-xs text-slate-500">
              {new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </span>
          )}
        </div>
        <p className="text-sm text-slate-300 line-clamp-2">{content}</p>
      </div>
    )
  }

  return (
    <div className={cn("max-w-4xl rounded-xl border p-5", style.bg, style.border)}>
      <div className="flex items-center justify-between mb-3">
        <span className={cn("text-xs font-medium uppercase tracking-wide", style.text)}>
          {formatType(analysisType)}
        </span>
        {timestamp && (
          <span className="text-xs text-slate-500">
            {new Date(timestamp).toLocaleString()}
          </span>
        )}
      </div>
      <p className="text-slate-300 leading-relaxed">{content}</p>
      {recommendations && (
        <div className="mt-3 pt-3 border-t border-white/10">
          <p className="text-xs text-slate-500 mb-1">Recommendation:</p>
          <p className="text-sm text-amber-400">{recommendations}</p>
        </div>
      )}
    </div>
  )
}

function PoetryCard({ style, title, content, timestamp }) {
  return (
    <div className="max-w-4xl bg-gradient-to-br from-slate-800/60 to-slate-900/60 border border-slate-700/30 rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <span className="text-xs text-amber-400/80 uppercase tracking-wider">
            {style === 'walt_whitman' ? 'In the spirit of Whitman' : 'In the spirit of Black Elk'}
          </span>
          {title && <h4 className="text-warm-white font-medium mt-1">{title}</h4>}
        </div>
        {timestamp && (
          <span className="text-xs text-slate-500">
            {new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </span>
        )}
      </div>
      <div className="font-serif text-slate-300 leading-relaxed whitespace-pre-line italic">
        {content}
      </div>
    </div>
  )
}

function MetricsCard({ title, metrics }) {
  return (
    <div className="max-w-4xl bg-slate-800/40 border border-slate-700/30 rounded-xl p-5">
      <h4 className="font-medium text-warm-white mb-4">{title}</h4>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {metrics.map((m, i) => (
          <div key={i} className="text-center p-3 bg-slate-900/40 rounded-lg">
            <p className={cn("text-2xl font-semibold", m.color || 'text-warm-white')}>{m.value}</p>
            <p className="text-xs text-slate-500 mt-1">{m.label}</p>
          </div>
        ))}
      </div>
    </div>
  )
}

export default function ContextCard({ type, ...props }) {
  switch (type) {
    case 'alert':
      return <AlertCard {...props} />
    case 'batch':
      return <BatchCard {...props} />
    case 'insight':
      return <InsightCard {...props} />
    case 'poetry':
      return <PoetryCard {...props} />
    case 'metrics':
      return <MetricsCard {...props} />
    default:
      return null
  }
}

