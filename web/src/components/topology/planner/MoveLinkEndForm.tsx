import { useState } from 'react'
import { UNSET_LATENCY_NS } from './estimator'

export function MoveLinkEndForm({
  linkCode,
  targetDeviceCode,
  defaultLatencyMs,
  defaultBandwidthGbps,
  onSubmit,
  onCancel,
}: {
  linkCode: string
  targetDeviceCode: string
  defaultLatencyMs: number
  defaultBandwidthGbps: number
  onSubmit: (latencyNs: number, bandwidthBps: number) => void
  onCancel: () => void
}) {
  // The real interface is TBD -- the contributor decides it later, so this form no
  // longer collects one. PlannerMap stages the change with a "TBD" placeholder.
  const [latencyMs, setLatencyMs] = useState(String(defaultLatencyMs))
  const [bandwidthGbps, setBandwidthGbps] = useState(String(defaultBandwidthGbps))

  const submit = () => {
    const ms = Number(latencyMs)
    const gbps = Number(bandwidthGbps)
    if (!Number.isFinite(ms) || ms <= 0 || !Number.isFinite(gbps) || gbps <= 0) return
    const latencyNs = Math.round(ms * 1e6)
    // 1e9 ns is the reserved "unset" sentinel (estimator UNSET_LATENCY_NS); an edge
    // carrying it is silently dropped by the impact engine, so reject it here too,
    // mirroring the guard in LinkEditForm.
    if (latencyNs === UNSET_LATENCY_NS) return
    onSubmit(latencyNs, Math.round(gbps * 1e9))
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-60 space-y-2">
      <div className="text-xs font-medium">
        Move {linkCode} → {targetDeviceCode}
      </div>
      <label className="block text-xs text-muted-foreground">
        Latency (ms)
        <input
          autoFocus
          type="number"
          step="0.001"
          value={latencyMs}
          onChange={(e) => setLatencyMs(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Bandwidth (Gbps)
        <input
          type="number"
          value={bandwidthGbps}
          onChange={(e) => setBandwidthGbps(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          className="flex-1 px-2 py-1 text-xs rounded bg-accent text-accent-foreground hover:bg-accent/90"
        >
          Confirm move
        </button>
        <button onClick={onCancel} className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80">
          Cancel
        </button>
      </div>
    </div>
  )
}
