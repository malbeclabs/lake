import { useState } from 'react'
import type { TopologyMetro } from '@/lib/api'

export function AddDeviceForm({
  metros,
  defaultMetroPk,
  onSubmit,
  onCancel,
}: {
  metros: TopologyMetro[]
  defaultMetroPk: string
  onSubmit: (v: {
    contributorPk: string
    metroPk: string
    code: string
    deviceType: string
  }) => void
  onCancel: () => void
}) {
  const [contributorPk, setContributorPk] = useState('')
  const [metroPk, setMetroPk] = useState(defaultMetroPk)
  const [code, setCode] = useState('')
  const [deviceType, setDeviceType] = useState('switch')
  const [touched, setTouched] = useState(false)

  const valid = contributorPk.trim() !== '' && metroPk !== '' && code.trim() !== ''

  const submit = () => {
    setTouched(true)
    if (!valid) return
    onSubmit({ contributorPk: contributorPk.trim(), metroPk, code: code.trim(), deviceType })
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-64 space-y-2">
      <div className="text-xs font-medium">New device</div>
      <label className="block text-xs text-muted-foreground">
        Metro
        <select
          value={metroPk}
          onChange={(e) => setMetroPk(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        >
          <option value="">Select…</option>
          {metros.map((m) => (
            <option key={m.pk} value={m.pk}>
              {m.code} — {m.name}
            </option>
          ))}
        </select>
      </label>
      <label className="block text-xs text-muted-foreground">
        Contributor pk
        <input
          value={contributorPk}
          onChange={(e) => setContributorPk(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Code
        <input
          value={code}
          onChange={(e) => setCode(e.target.value)}
          placeholder="nyc-x2"
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Type
        <select
          value={deviceType}
          onChange={(e) => setDeviceType(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        >
          <option value="switch">switch</option>
          <option value="router">router</option>
        </select>
      </label>
      {touched && !valid && (
        <p className="text-[11px] text-red-500">Metro, contributor and code are required.</p>
      )}
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          className="flex-1 px-2 py-1 text-xs rounded bg-accent text-accent-foreground hover:bg-accent/90"
        >
          Add device
        </button>
        <button onClick={onCancel} className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80">
          Cancel
        </button>
      </div>
    </div>
  )
}
