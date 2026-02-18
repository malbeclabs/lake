import { useEffect, useState } from 'react'
import { Play, Square, Loader2 } from 'lucide-react'
import { api, type ServiceInfo } from '@/lib/api'
import { ServiceCard } from '@/components/ServiceCard'

export function Dashboard() {
  const [services, setServices] = useState<ServiceInfo[]>([])
  const [loading, setLoading] = useState(false)
  const [nextAction, setNextAction] = useState<string>('')

  const loadStatus = async () => {
    try {
      const status = await api.getStatus()
      setServices(status)
    } catch (err) {
      console.error('Failed to load status:', err)
    }
  }

  const loadNextAction = async () => {
    try {
      const action = await api.getNextAction()
      setNextAction(action)
    } catch (err) {
      console.error('Failed to load next action:', err)
    }
  }

  useEffect(() => {
    loadStatus()
    loadNextAction()

    const interval = setInterval(() => {
      loadStatus()
      loadNextAction()
    }, 2000)

    return () => clearInterval(interval)
  }, [])

  const handleStartAll = async () => {
    setLoading(true)
    try {
      await api.startAll()
      setTimeout(loadStatus, 500)
    } catch (err) {
      console.error('Failed to start all:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleStopAll = async () => {
    setLoading(true)
    try {
      await api.stopAll()
      setTimeout(loadStatus, 500)
    } catch (err) {
      console.error('Failed to stop all:', err)
    } finally {
      setLoading(false)
    }
  }

  const runningCount = services.filter((s) => s.status === 'running').length
  const sortedServices = [...services].sort((a, b) => a.name.localeCompare(b.name))

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Services</h1>
          <p className="text-muted-foreground mt-1">Manage your DoubleZero Data services</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleStartAll}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors text-sm"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            Start All
          </button>
          <button
            onClick={handleStopAll}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-destructive text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50 transition-colors text-sm"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Square className="w-4 h-4" />}
            Stop All
          </button>
        </div>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="p-4 border border-border bg-card transition-colors">
          <div className="text-xs text-muted-foreground">Running Services</div>
          <div className="text-2xl font-bold mt-1">{runningCount} / {services.length}</div>
        </div>
        <div className="p-4 border border-border bg-card transition-colors">
          <div className="text-xs text-muted-foreground">Next Scheduled Action</div>
          <div className="text-sm font-medium mt-1">{nextAction || 'Scheduling disabled'}</div>
        </div>
        <div className="p-4 border border-border bg-card transition-colors">
          <div className="text-xs text-muted-foreground">Total Services</div>
          <div className="text-2xl font-bold mt-1">{services.length}</div>
        </div>
      </div>

      {/* Service Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {sortedServices.map((service) => (
          <ServiceCard
            key={service.name}
            service={service}
            onUpdate={loadStatus}
          />
        ))}
      </div>

    </div>
  )
}
