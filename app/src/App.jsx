import { useState, useEffect, useCallback } from 'react'
import Header from './components/Header'
import ConversationView from './components/ConversationView'
import DashboardPanel from './components/DashboardPanel'
import SettingsPanel from './components/SettingsPanel'

function App() {
  const [mode, setMode] = useState('deep') // 'quick' or 'deep'
  const [autonomousStatus, setAutonomousStatus] = useState(null)
  const [showSettings, setShowSettings] = useState(false)
  const [healthScore, setHealthScore] = useState(null)
  const [pendingQuery, setPendingQuery] = useState(null)

  const fetchAutonomousStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/autonomous/status')
      if (res.ok) {
        const data = await res.json()
        setAutonomousStatus(data)
      }
    } catch (err) {
      console.error('Failed to fetch autonomous status:', err)
    }
  }, [])

  const fetchHealthScore = useCallback(async () => {
    try {
      const res = await fetch('/api/health/score')
      if (res.ok) {
        const data = await res.json()
        setHealthScore(data)
      }
    } catch (err) {
      // Health endpoint may not exist yet
    }
  }, [])

  useEffect(() => {
    fetchAutonomousStatus()
    fetchHealthScore()

    const interval = setInterval(() => {
      fetchAutonomousStatus()
      fetchHealthScore()
    }, 30000)

    return () => clearInterval(interval)
  }, [fetchAutonomousStatus, fetchHealthScore])

  const toggleAutonomous = async () => {
    const endpoint = autonomousStatus?.is_running 
      ? '/api/autonomous/stop' 
      : '/api/autonomous/start'
    try {
      await fetch(endpoint, { method: 'POST' })
      fetchAutonomousStatus()
    } catch (err) {
      console.error('Autonomous toggle failed:', err)
    }
  }

  const checkHealthNow = async () => {
    try {
      await fetch('/api/autonomous/check-now', { method: 'POST' })
      setTimeout(fetchHealthScore, 3000)
    } catch (err) {
      console.error('Health check trigger failed:', err)
    }
  }

  const injectAnomaly = async () => {
    try {
      const res = await fetch('/api/data/inject-anomaly', { method: 'POST' })
      if (res.ok) {
        setTimeout(fetchHealthScore, 2000)
      }
    } catch (err) {
      console.error('Anomaly injection failed:', err)
    }
  }

  const injectGoodData = async () => {
    try {
      const res = await fetch('/api/data/inject-good', { method: 'POST' })
      if (res.ok) {
        setTimeout(fetchHealthScore, 2000)
      }
    } catch (err) {
      console.error('Good data injection failed:', err)
    }
  }

  const refreshData = () => {
    fetchHealthScore()
  }

  return (
    <div className="h-screen flex flex-col bg-gradient-main text-warm-white overflow-hidden">
      <Header 
        mode={mode}
        setMode={setMode}
        autonomousStatus={autonomousStatus}
        onToggleAutonomous={toggleAutonomous}
        onOpenSettings={() => setShowSettings(true)}
        healthScore={healthScore}
        onCheckHealth={checkHealthNow}
        onInjectAnomaly={injectAnomaly}
        onInjectGoodData={injectGoodData}
      />
      
      <main className="flex-1 flex overflow-hidden">
        <div className="flex-1 overflow-hidden">
          <ConversationView 
            mode={mode}
            healthScore={healthScore}
            onRefresh={refreshData}
            pendingQuery={pendingQuery}
            onPendingQueryHandled={() => setPendingQuery(null)}
          />
        </div>
        <DashboardPanel onQuerySelect={(q) => setPendingQuery(q)} />
      </main>

      {showSettings && (
        <SettingsPanel 
          autonomousStatus={autonomousStatus}
          onClose={() => setShowSettings(false)}
          onSave={fetchAutonomousStatus}
        />
      )}
    </div>
  )
}

export default App
