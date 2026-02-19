// API client for Control Center

export type ServiceStatus = 'stopped' | 'starting' | 'running' | 'failed'

export type LogLevel = 'ERROR' | 'WARN' | 'INFO' | 'DEBUG' | 'TRACE' | 'UNKNOWN'

export interface ServiceInfo {
  name: string
  status: ServiceStatus
  pid: number
  startedAt?: string
  stoppedAt?: string
  error?: string
  uptime?: string
}

export interface LogEntry {
  timestamp: string
  service: string
  stream: 'stdout' | 'stderr'
  level: LogLevel
  line: string
}

export interface HistogramBucket {
  time: string
  error: number
  warn: number
  info: number
  debug: number
  unknown: number
}

export interface ScheduleConfig {
  enabled: boolean
  startTime: string
  stopTime: string
}

export interface Config {
  port: number
  schedule: ScheduleConfig
  services: Record<string, string>
  bindHost?: string
}

class API {
  private baseURL: string

  constructor(baseURL: string = '') {
    this.baseURL = baseURL
  }

  async getStatus(): Promise<ServiceInfo[]> {
    const res = await fetch(`${this.baseURL}/api/status`)
    if (!res.ok) throw new Error(`Failed to get status: ${res.statusText}`)
    return res.json()
  }

  async startService(name: string): Promise<void> {
    const res = await fetch(`${this.baseURL}/api/services/${name}/start`, {
      method: 'POST',
    })
    if (!res.ok) {
      let message = `Failed to start ${name}`
      try {
        const data = await res.json()
        if (data.error) message = data.error
      } catch {
        message = res.statusText || message
      }
      throw new Error(message)
    }
  }

  async stopService(name: string): Promise<void> {
    const res = await fetch(`${this.baseURL}/api/services/${name}/stop`, {
      method: 'POST',
    })
    if (!res.ok) {
      let message = `Failed to stop ${name}`
      try {
        const data = await res.json()
        if (data.error) message = data.error
      } catch {
        message = res.statusText || message
      }
      throw new Error(message)
    }
  }

  async startAll(): Promise<void> {
    const res = await fetch(`${this.baseURL}/api/services/start-all`, {
      method: 'POST',
    })
    if (!res.ok) throw new Error('Failed to start all services')
  }

  async stopAll(): Promise<void> {
    const res = await fetch(`${this.baseURL}/api/services/stop-all`, {
      method: 'POST',
    })
    if (!res.ok) throw new Error('Failed to stop all services')
  }

  async getRecentLogs(
    service: string = 'all',
    level: string = 'all',
    limit: number = 1000,
    from?: string,
    to?: string
  ): Promise<LogEntry[]> {
    const params = new URLSearchParams({ service, level, limit: String(limit) })
    if (from) params.set('from', from)
    if (to) params.set('to', to)
    const res = await fetch(`${this.baseURL}/api/logs?${params}`)
    if (!res.ok) throw new Error('Failed to get logs')
    return res.json()
  }

  async getLogHistogram(
    service: string = 'all',
    level: string = 'all',
    from: string,
    to: string,
    interval: number = 300
  ): Promise<HistogramBucket[]> {
    const params = new URLSearchParams({
      service,
      level,
      from,
      to,
      interval: String(interval),
    })
    const res = await fetch(`${this.baseURL}/api/logs/histogram?${params}`)
    if (!res.ok) throw new Error('Failed to get log histogram')
    return res.json()
  }

  async getConfig(): Promise<Config> {
    const res = await fetch(`${this.baseURL}/api/config`)
    if (!res.ok) throw new Error('Failed to get config')
    return res.json()
  }

  async updateConfig(config: Config): Promise<void> {
    const res = await fetch(`${this.baseURL}/api/config`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    })
    if (!res.ok) throw new Error('Failed to update config')
  }

  async getNextAction(): Promise<string> {
    const res = await fetch(`${this.baseURL}/api/schedule/next`)
    if (!res.ok) throw new Error('Failed to get next action')
    const data: { message: string; nextTime?: string } = await res.json()
    if (data.nextTime) {
      const localTime = new Date(data.nextTime).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      return `${data.message} (at ${localTime})`
    }
    return data.message
  }

  // Create SSE connection for log streaming
  createLogStream(
    service: string = 'all',
    level: string = 'all'
  ): EventSource {
    const params = new URLSearchParams({ service, level })
    return new EventSource(`${this.baseURL}/api/logs/stream?${params}`)
  }
}

export const api = new API()
