import { useEffect, useState } from 'react'
import { Save, Loader2 } from 'lucide-react'
import { api, type Config } from '@/lib/api'

// Convert "HH:MM" UTC to "HH:MM" in the browser's local timezone
function utcToLocalTime(utcHHMM: string): string {
  const [h, m] = utcHHMM.split(':').map(Number)
  const d = new Date()
  d.setUTCHours(h, m, 0, 0)
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

// Convert "HH:MM" in the browser's local timezone to "HH:MM" UTC
function localToUtcTime(localHHMM: string): string {
  const [h, m] = localHHMM.split(':').map(Number)
  const d = new Date()
  d.setHours(h, m, 0, 0)
  return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`
}

export function Settings() {
  const [config, setConfig] = useState<Config | null>(null)
  const [loading, setLoading] = useState(false)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    loadConfig()
  }, [])

  const loadConfig = async () => {
    try {
      const cfg = await api.getConfig()
      // Config stores times in UTC; convert to local for display
      cfg.schedule.startTime = utcToLocalTime(cfg.schedule.startTime)
      cfg.schedule.stopTime = utcToLocalTime(cfg.schedule.stopTime)
      setConfig(cfg)
    } catch (err) {
      console.error('Failed to load config:', err)
    }
  }

  const handleSave = async () => {
    if (!config) return

    setLoading(true)
    setSaved(false)
    try {
      // Convert local times back to UTC before saving
      const configToSave = {
        ...config,
        schedule: {
          ...config.schedule,
          startTime: localToUtcTime(config.schedule.startTime),
          stopTime: localToUtcTime(config.schedule.stopTime),
        },
      }
      await api.updateConfig(configToSave)
      setSaved(true)
      setTimeout(() => setSaved(false), 3000)
    } catch (err) {
      console.error('Failed to save config:', err)
    } finally {
      setLoading(false)
    }
  }

  if (!config) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6 max-w-2xl">
      <div>
        <h1 className="text-3xl font-bold">Settings</h1>
        <p className="text-muted-foreground mt-1">
          Configure scheduling and service options
        </p>
      </div>

      {/* Schedule Configuration */}
      <div className="space-y-4 p-6 border border-border rounded-lg bg-card transition-colors">
        <h2 className="text-xl font-semibold">Schedule Configuration</h2>

        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <input
              type="checkbox"
              id="schedule-enabled"
              checked={config.schedule.enabled}
              onChange={(e) =>
                setConfig({
                  ...config,
                  schedule: { ...config.schedule, enabled: e.target.checked },
                })
              }
              className="w-4 h-4 rounded border-input"
            />
            <label htmlFor="schedule-enabled" className="font-medium">
              Enable automatic scheduling
            </label>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">
                Start Time
              </label>
              <input
                type="time"
                value={config.schedule.startTime}
                onChange={(e) =>
                  setConfig({
                    ...config,
                    schedule: { ...config.schedule, startTime: e.target.value },
                  })
                }
                disabled={!config.schedule.enabled}
                className="w-full px-3 py-2 rounded-md border border-input bg-background disabled:opacity-50"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">
                Stop Time
              </label>
              <input
                type="time"
                value={config.schedule.stopTime}
                onChange={(e) =>
                  setConfig({
                    ...config,
                    schedule: { ...config.schedule, stopTime: e.target.value },
                  })
                }
                disabled={!config.schedule.enabled}
                className="w-full px-3 py-2 rounded-md border border-input bg-background disabled:opacity-50"
              />
            </div>
          </div>

          <p className="text-sm text-muted-foreground">
            Services will start at {config.schedule.startTime} and stop at{' '}
            {config.schedule.stopTime} daily ({Intl.DateTimeFormat().resolvedOptions().timeZone}).
          </p>
        </div>
      </div>

      {/* Save Button */}
      <div className="flex items-center gap-3">
        <button
          onClick={handleSave}
          disabled={loading}
          className="flex items-center gap-2 px-6 py-2 rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
        >
          {loading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <Save className="w-4 h-4" />
          )}
          Save Changes
        </button>

        {saved && (
          <span className="text-sm text-green-500 font-medium">
            ✓ Settings saved successfully
          </span>
        )}
      </div>
    </div>
  )
}
