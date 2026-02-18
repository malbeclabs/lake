import { useState } from 'react'
import { Play, Square, AlertCircle, CheckCircle, Loader2 } from 'lucide-react'
import { api, type ServiceInfo } from '@/lib/api'
import { cn } from '@/lib/utils'

interface ServiceCardProps {
  service: ServiceInfo
  onUpdate: () => void
}

export function ServiceCard({ service, onUpdate }: ServiceCardProps) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleStart = async () => {
    setLoading(true)
    setError(null)
    try {
      await api.startService(service.name)
      setTimeout(onUpdate, 500)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start service')
    } finally {
      setLoading(false)
    }
  }

  const handleStop = async () => {
    setLoading(true)
    setError(null)
    try {
      await api.stopService(service.name)
      setTimeout(onUpdate, 500)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to stop service')
    } finally {
      setLoading(false)
    }
  }

  const getStatusColor = () => {
    switch (service.status) {
      case 'running': return 'text-green-500'
      case 'stopped': return 'text-gray-500'
      case 'failed': return 'text-red-500'
      case 'starting': return 'text-yellow-500'
      default: return 'text-gray-500'
    }
  }

  const getStatusIcon = () => {
    switch (service.status) {
      case 'running': return <CheckCircle className="w-4 h-4" />
      case 'failed': return <AlertCircle className="w-4 h-4" />
      case 'starting': return <Loader2 className="w-4 h-4 animate-spin" />
      default: return <Square className="w-4 h-4" />
    }
  }

  return (
    <div className="border border-border p-5 bg-card transition-colors">
      <div className="flex items-start justify-between mb-3">
        <div>
          <h3 className="text-sm font-medium capitalize">{service.name}</h3>
          <div className={cn('flex items-center gap-1.5 mt-1', getStatusColor())}>
            {getStatusIcon()}
            <span className="text-xs font-medium capitalize">{service.status}</span>
          </div>
        </div>
        <div className="flex gap-1.5">
          {service.status === 'running' ? (
            <button
              onClick={handleStop}
              disabled={loading}
              className="p-1.5 bg-destructive text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50 transition-colors"
              title="Stop"
            >
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Square className="w-3.5 h-3.5" />}
            </button>
          ) : (
            <button
              onClick={handleStart}
              disabled={loading}
              className="p-1.5 bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
              title="Start"
            >
              {loading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
            </button>
          )}
        </div>
      </div>

      {service.uptime && service.status === 'running' && (
        <div className="text-xs text-muted-foreground">Uptime: {service.uptime}</div>
      )}
      {service.pid > 0 && (
        <div className="text-xs text-muted-foreground">PID: {service.pid}</div>
      )}
      {service.error && (
        <div className="mt-2 text-xs text-destructive">{service.error}</div>
      )}
      {error && (
        <div className="mt-2 text-xs text-destructive">{error}</div>
      )}

    </div>
  )
}
