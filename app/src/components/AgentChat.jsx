import { useState, useRef, useEffect } from 'react'
import { cn } from '../lib/utils'

function LoadingDots() {
  return (
    <div className="flex gap-1">
      <div className="w-2 h-2 rounded-full bg-medops-400 loading-dot" />
      <div className="w-2 h-2 rounded-full bg-medops-400 loading-dot" />
      <div className="w-2 h-2 rounded-full bg-medops-400 loading-dot" />
    </div>
  )
}

function Message({ message, isUser }) {
  return (
    <div className={cn(
      "message-enter flex gap-3",
      isUser ? "flex-row-reverse" : ""
    )}>
      <div className={cn(
        "w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0",
        isUser ? "bg-blue-500/20" : "bg-medops-500/20"
      )}>
        {isUser ? (
          <svg className="w-4 h-4 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
          </svg>
        ) : (
          <svg className="w-4 h-4 text-medops-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
          </svg>
        )}
      </div>
      <div className={cn(
        "flex-1 p-3 rounded-xl text-sm",
        isUser 
          ? "bg-blue-500/10 text-blue-100" 
          : "bg-slate-800/50 text-slate-200"
      )}>
        <p className="whitespace-pre-wrap">{message.content}</p>
        {message.tool_calls && message.tool_calls.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1">
            {message.tool_calls.map((tool, i) => (
              <span key={i} className="text-xs px-2 py-0.5 bg-slate-700/50 rounded text-slate-400">
                {tool}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

export default function AgentChat({ mode, onAnalysisCreated }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const sendMessage = async () => {
    if (!input.trim() || isLoading) return

    const userMessage = { role: 'user', content: input.trim() }
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      const history = messages.map(m => ({
        role: m.role,
        content: m.content,
      }))

      const res = await fetch('/api/agent/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: userMessage.content,
          mode,
          history,
        }),
      })

      const data = await res.json()
      
      const assistantMessage = {
        role: 'assistant',
        content: data.response || data.error || 'No response',
        tool_calls: data.tool_calls || [],
      }
      
      setMessages(prev => [...prev, assistantMessage])

      // If write_analysis was called, refresh the analysis data
      if (data.tool_calls?.includes('write_analysis')) {
        onAnalysisCreated?.()
      }
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `Error: ${err.message}`,
      }])
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const clearChat = () => {
    setMessages([])
  }

  return (
    <div className="glass-card flex flex-col h-[calc(100vh-180px)] min-h-[500px]">
      {/* Header */}
      <div className="p-4 border-b border-slate-700/50 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <svg className="w-5 h-5 text-medops-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
          </svg>
          <span className="font-medium text-white">Agent Chat</span>
          <span className={cn(
            "text-xs px-2 py-0.5 rounded-full",
            mode === 'orchestrator' 
              ? "bg-amber-500/20 text-amber-400"
              : "bg-medops-500/20 text-medops-400"
          )}>
            {mode}
          </span>
        </div>
        <button
          onClick={clearChat}
          className="text-xs text-slate-400 hover:text-white transition-colors"
        >
          Clear
        </button>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center text-slate-500">
            <svg className="w-12 h-12 mb-3 opacity-50" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
            </svg>
            <p className="text-sm">Ask about hospital operations, LOS, costs, or request analysis</p>
            <div className="mt-4 flex flex-wrap gap-2 max-w-xs justify-center">
              {[
                "What is the average LOS at Hospital A?",
                "Compare hospitals by readmission rate",
                "Analyze drug cost trends",
              ].map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => setInput(suggestion)}
                  className="text-xs px-3 py-1.5 bg-slate-800/50 text-slate-400 rounded-lg hover:bg-slate-700/50 hover:text-white transition-colors"
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        ) : (
          messages.map((msg, idx) => (
            <Message key={idx} message={msg} isUser={msg.role === 'user'} />
          ))
        )}
        
        {isLoading && (
          <div className="flex gap-3">
            <div className="w-8 h-8 rounded-lg bg-medops-500/20 flex items-center justify-center">
              <svg className="w-4 h-4 text-medops-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <div className="flex-1 p-3 rounded-xl bg-slate-800/50">
              <LoadingDots />
            </div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-4 border-t border-slate-700/50">
        <div className="flex gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about hospital data, request analysis..."
            rows={1}
            className="flex-1 bg-slate-800/50 border border-slate-700/50 rounded-xl px-4 py-3 text-sm text-white placeholder-slate-500 resize-none focus:outline-none focus:border-medops-500/50 focus:ring-1 focus:ring-medops-500/25"
          />
          <button
            onClick={sendMessage}
            disabled={!input.trim() || isLoading}
            className={cn(
              "px-4 rounded-xl font-medium transition-all",
              input.trim() && !isLoading
                ? "bg-medops-500 text-white hover:bg-medops-600"
                : "bg-slate-700/50 text-slate-500 cursor-not-allowed"
            )}
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  )
}

