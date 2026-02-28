import { cn } from '../lib/utils'

export default function Header({ mode, setMode, autonomousStatus, onToggleAutonomous, onOpenSettings, healthScore, onCheckHealth, onInjectAnomaly, onInjectGoodData }) {
  const isRunning = autonomousStatus?.is_running && !autonomousStatus?.is_paused
  
  const getHealthColor = (score) => {
    if (!score) return 'bg-slate-500'
    if (score >= 80) return 'bg-living-green'
    if (score >= 60) return 'bg-amber-500'
    return 'bg-soft-red'
  }

  return (
    <header className="border-b border-slate-700/30 bg-slate-900/80 backdrop-blur-md sticky top-0 z-50">
      <div className="px-6 py-3">
        <div className="flex items-center justify-between">
          {/* Logo & Health */}
          <div className="flex items-center gap-5">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-teal-400 to-teal-600 flex items-center justify-center shadow-lg shadow-teal-500/20">
                <svg className="w-6 h-6 text-slate-900" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
                </svg>
              </div>
              <div>
                <h1 className="text-lg font-semibold text-warm-white">MedOps NBA</h1>
                <p className="text-xs text-slate-400">Next Best Action</p>
              </div>
            </div>
            
            {healthScore && (
              <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 bg-slate-800/50 rounded-lg">
                <div className={cn("w-2 h-2 rounded-full", getHealthColor(healthScore.score))} />
                <span className="text-sm text-slate-300">
                  Health: <span className="font-medium text-warm-white">{healthScore.score}</span>/100
                </span>
              </div>
            )}
          </div>

          {/* Mode Toggle */}
          <div className="flex items-center gap-1 bg-slate-800/50 rounded-xl p-1">
            <button
              onClick={() => setMode('quick')}
              className={cn(
                "px-4 py-2 rounded-lg text-sm font-medium transition-all",
                mode === 'quick'
                  ? "bg-teal-500 text-slate-900 shadow-lg shadow-teal-500/25"
                  : "text-slate-400 hover:text-warm-white hover:bg-slate-700/50"
              )}
              title="Fast answers to specific questions. Best for counts, lookups, simple comparisons."
            >
              Quick Query
            </button>
            <button
              onClick={() => setMode('deep')}
              className={cn(
                "px-4 py-2 rounded-lg text-sm font-medium transition-all",
                mode === 'deep'
                  ? "bg-teal-500 text-slate-900 shadow-lg shadow-teal-500/25"
                  : "text-slate-400 hover:text-warm-white hover:bg-slate-700/50"
              )}
              title="Thorough investigation with multiple data sources. Best for root cause, predictions, reports."
            >
              Deep Analysis
            </button>
          </div>

          {/* Actions & Autonomous Status */}
          <div className="flex items-center gap-2">
            <button
              onClick={onCheckHealth}
              className="px-3 py-2 rounded-lg text-xs font-medium bg-blue-500/20 text-blue-400 hover:bg-blue-500/30 transition-all"
              title="Run health check now"
            >
              Check Health
            </button>
            <button
              onClick={onInjectGoodData}
              className="px-3 py-2 rounded-lg text-xs font-medium bg-green-500/20 text-green-400 hover:bg-green-500/30 transition-all"
              title="Inject healthy encounter data"
            >
              Inject Good
            </button>
            <button
              onClick={onInjectAnomaly}
              className="px-3 py-2 rounded-lg text-xs font-medium bg-amber-500/20 text-amber-400 hover:bg-amber-500/30 transition-all"
              title="Inject anomalous encounter data for testing"
            >
              Inject Anomaly
            </button>

            <div className="flex items-center gap-2 bg-slate-800/50 rounded-xl px-4 py-2">
              <div className={cn(
                "w-2.5 h-2.5 rounded-full transition-all",
                isRunning ? "bg-living-green autonomous-pulse" : "bg-slate-500"
              )} />
              <span className="text-sm text-slate-300">Auto</span>
              <button
                onClick={onToggleAutonomous}
                className={cn(
                  "px-2.5 py-1 rounded-lg text-xs font-medium transition-all",
                  isRunning
                    ? "bg-soft-red/20 text-soft-red hover:bg-soft-red/30"
                    : "bg-living-green/20 text-living-green hover:bg-living-green/30"
                )}
              >
                {isRunning ? 'Stop' : 'Start'}
              </button>
            </div>

            <button
              onClick={onOpenSettings}
              className="p-2 text-slate-400 hover:text-warm-white hover:bg-slate-800/50 rounded-lg transition-all"
              title="Settings"
            >
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
            </button>
          </div>
        </div>
      </div>
    </header>
  )
}
