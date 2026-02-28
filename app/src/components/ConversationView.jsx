import { useState, useRef, useEffect } from 'react'
import ReactMarkdown from 'react-markdown'
import ContextCard from './ContextCard'
import { cn } from '../lib/utils'

function LoadingDots({ stage }) {
  const stageLabels = {
    starting: 'Checking prerequisites...',
    routing: 'Deciding next step...',
    planning: 'Creating analysis plan...',
    retrieving: 'Gathering evidence from data sources...',
    analyzing: 'Interpreting results...',
    clarifying: 'Asking for clarification...',
    responding: 'Preparing final response...',
  }
  const label = stage ? stageLabels[stage] || stage : null
  return (
    <div className="flex items-center gap-3">
      <div className="flex gap-1.5">
        <div className="w-2 h-2 rounded-full bg-teal-400 loading-dot" />
        <div className="w-2 h-2 rounded-full bg-teal-400 loading-dot" />
        <div className="w-2 h-2 rounded-full bg-teal-400 loading-dot" />
      </div>
      {label && <span className="text-sm text-slate-400">{label}</span>}
    </div>
  )
}

function Message({ message, isUser }) {
  return (
    <div className={cn("message-enter max-w-4xl", isUser ? "ml-auto" : "")}>
      <div className={cn(
        "rounded-2xl px-5 py-4",
        isUser 
          ? "bg-teal-500/20 border border-teal-500/30" 
          : "bg-slate-800/40 border border-slate-700/30"
      )}>
        {isUser ? (
          <p className="whitespace-pre-wrap text-warm-white leading-relaxed">{message.content}</p>
        ) : (
          <div className="prose prose-invert prose-sm max-w-none prose-headings:text-warm-white prose-p:text-warm-white prose-strong:text-teal-400 prose-ul:text-warm-white prose-li:text-warm-white">
            <ReactMarkdown>{message.content}</ReactMarkdown>
          </div>
        )}
        
        {message.card && (
          <div className="mt-4">
            <ContextCard {...message.card} />
          </div>
        )}
        
        {message.cards && message.cards.map((card, i) => (
          <div key={i} className="mt-4">
            <ContextCard {...card} />
          </div>
        ))}
        
        {message.suggestions && (
          <div className="mt-4 flex flex-wrap gap-2">
            {message.suggestions.map((s, i) => (
              <button key={i} onClick={s.onClick}
                className="px-3 py-1.5 text-sm bg-slate-700/50 text-slate-300 rounded-lg hover:bg-slate-600/50 hover:text-white transition-all">
                {s.label}
              </button>
            ))}
          </div>
        )}
        
        {message.tool_calls && message.tool_calls.length > 0 && (
          <div className="mt-3 flex flex-wrap gap-1.5">
            {message.tool_calls.map((tool, i) => (
              <span key={i} className="text-xs px-2 py-1 bg-teal-500/20 text-teal-300 rounded-md">{tool}</span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function WelcomeMessage({ healthScore }) {
  const getGreeting = () => {
    const hour = new Date().getHours()
    if (hour < 12) return 'Good morning'
    if (hour < 17) return 'Good afternoon'
    return 'Good evening'
  }

  const getHealthColor = (score) => {
    if (!score) return 'text-slate-400'
    if (score >= 80) return 'text-living-green'
    if (score >= 60) return 'text-amber-400'
    return 'text-soft-red'
  }

  const getHealthLabel = (score) => {
    if (!score) return 'Unknown'
    if (score >= 80) return 'Healthy'
    if (score >= 60) return 'Attention needed'
    return 'Critical'
  }

  return (
    <div className="space-y-4 message-enter max-w-4xl">
      <div>
        <p className="text-xl text-warm-white mb-2">
          {getGreeting()}. Operations health: {' '}
          <span className={cn("font-semibold", getHealthColor(healthScore?.score))}>
            {healthScore?.score || '--'}/100
          </span>
          {' '}
          <span className="text-slate-400">({getHealthLabel(healthScore?.score)})</span>
        </p>
        
        {healthScore?.summary && (
          <p className="text-slate-300 leading-relaxed">{healthScore.summary}</p>
        )}
        
        {!healthScore?.summary && (
          <p className="text-slate-300 leading-relaxed">
            Ready to help you monitor hospital operations, analyze trends, and recommend actions.
            Ask me anything about your medical logistics data.
          </p>
        )}
      </div>
    </div>
  )
}

export default function ConversationView({ mode, healthScore, onRefresh, pendingQuery, onPendingQueryHandled }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [loadingStage, setLoadingStage] = useState(null)
  const messagesEndRef = useRef(null)
  const inputRef = useRef(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => { scrollToBottom() }, [messages])

  useEffect(() => {
    if (pendingQuery && !isLoading) {
      sendMessage(pendingQuery)
      onPendingQueryHandled?.()
    }
  }, [pendingQuery])

  const sendDeepStream = async (text, history) => {
    const res = await fetch('/api/agent/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, mode: 'rag', history, stream: true }),
    })
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      const lines = buffer.split('\n')
      buffer = lines.pop()
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue
        const event = JSON.parse(line.slice(6))
        if (event.stage === 'done') {
          setLoadingStage(null)
          const assistantMessage = {
            role: 'assistant', content: event.response || 'No response',
            tool_calls: event.tool_calls || [],
          }
          setMessages(prev => [...prev, assistantMessage])
          if (event.tool_calls?.includes('write_analysis')) onRefresh?.()
          return
        }
        if (event.stage === 'error') {
          setLoadingStage(null)
          setMessages(prev => [...prev, { role: 'assistant', content: `Deep analysis error: ${event.message}` }])
          return
        }
        setLoadingStage(event.stage)
      }
    }
  }

  const sendMessage = async (messageText) => {
    const text = messageText || input.trim()
    if (!text || isLoading) return
    const userMessage = { role: 'user', content: text }
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)
    setLoadingStage(null)
    try {
      const history = messages.map(m => ({ role: m.role, content: m.content }))
      if (mode !== 'quick') {
        await sendDeepStream(text, history)
      } else {
        const res = await fetch('/api/agent/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: text, mode: 'orchestrator', history }),
        })
        const data = await res.json()
        const assistantMessage = {
          role: 'assistant', content: data.response || data.error || 'No response',
          tool_calls: data.tool_calls || [], card: data.card, cards: data.cards,
        }
        setMessages(prev => [...prev, assistantMessage])
        if (data.tool_calls?.includes('write_analysis')) onRefresh?.()
      }
    } catch (err) {
      setMessages(prev => [...prev, { role: 'assistant', content: `I encountered an error: ${err.message}. Please try again.` }])
    } finally {
      setIsLoading(false)
      setLoadingStage(null)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage() }
  }

  const suggestions = [
    { label: "Why did drug costs spike in November?", onClick: () => sendMessage("Why did drug costs spike in November for Hospital A?") },
    { label: "How to reduce LOS?", onClick: () => sendMessage("What specific actions can I take to reduce length of stay in Hospital A?") },
    { label: "Monday discharge patterns", onClick: () => sendMessage("Why is LOS higher for patients discharged on Mondays?") },
    { label: "Reduce ED wait times", onClick: () => sendMessage("How can I reduce wait times in the Emergency Department?") },
    { label: "Contract labor in Cardiology", onClick: () => sendMessage("How can I lower the use of contract labor in the cardiology department?") },
  ]

  return (
    <div className="h-full flex flex-col overflow-hidden">
      <div className="flex-1 min-h-0 overflow-y-auto custom-scrollbar px-6 py-8">
        <div className="max-w-5xl mx-auto space-y-6">
          <WelcomeMessage healthScore={healthScore} />
          {messages.map((msg, idx) => (
            <Message key={idx} message={msg} isUser={msg.role === 'user'} />
          ))}
          {isLoading && (
            <div className="max-w-4xl">
              <div className="bg-slate-800/40 border border-slate-700/30 rounded-2xl px-5 py-4">
                <LoadingDots stage={loadingStage} />
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      </div>

      <div className="flex-shrink-0 border-t border-slate-700/30 bg-slate-900/50 backdrop-blur-sm px-6 py-4">
        <div className="max-w-5xl mx-auto">
          {messages.length === 0 && (
            <div className="flex flex-wrap gap-2 mb-4">
              {suggestions.map((s, i) => (
                <button key={i} onClick={s.onClick}
                  className="px-4 py-2 text-sm bg-slate-800/60 text-slate-300 rounded-xl border border-slate-700/40 hover:bg-slate-700/60 hover:text-white hover:border-teal-500/30 transition-all">
                  {s.label}
                </button>
              ))}
            </div>
          )}
          <div className="flex gap-3">
            <textarea ref={inputRef} value={input} onChange={(e) => setInput(e.target.value)} onKeyDown={handleKeyDown}
              placeholder="Ask me anything..." rows={1}
              className="flex-1 bg-slate-800/60 border border-slate-700/40 rounded-xl px-5 py-3.5 text-warm-white placeholder-slate-500 resize-none focus:outline-none focus:border-teal-500/50 focus:ring-2 focus:ring-teal-500/20 transition-all" />
            <button onClick={() => sendMessage()} disabled={!input.trim() || isLoading}
              className={cn("px-5 rounded-xl font-medium transition-all flex items-center justify-center",
                input.trim() && !isLoading
                  ? "bg-teal-500 text-slate-900 hover:bg-teal-400 shadow-lg shadow-teal-500/25"
                  : "bg-slate-700/50 text-slate-500 cursor-not-allowed"
              )}>
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
              </svg>
            </button>
          </div>
          <div className="mt-3 flex items-center justify-center gap-3 text-xs text-slate-500">
            <span>{mode === 'quick' ? 'Quick Query' : 'Deep Analysis'} mode &middot; Press Enter to send</span>
            {messages.length > 0 && (
              <button onClick={() => setMessages([])}
                className="text-slate-500 hover:text-red-400 transition-colors">
                Clear chat
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
