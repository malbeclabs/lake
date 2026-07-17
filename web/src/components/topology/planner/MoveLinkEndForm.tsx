import { useState } from 'react'
import { UNSET_LATENCY_NS } from './estimator'

export function MoveLinkEndForm({
  linkCode,
  targetDeviceCode,
  defaultLatencyUs,
  defaultBandwidthGbps,
  onSubmit,
  onCancel,
}: {
  linkCode: string
  targetDeviceCode: string
  defaultLatencyUs: number
  defaultBandwidthGbps: number
  onSubmit: (ifaceName: string, latencyNs: number, bandwidthBps: number) => void
  onCancel: () => void
}) {
  const [iface, setIface] = useState('')
  const [latencyUs, setLatencyUs] = useState(String(defaultLatencyUs))
  const [bandwidthGbps, setBandwidthGbps] = useState(String(defaultBandwidthGbps))

  const submit = () => {
    const us = Number(latencyUs)
    const gbps = Number(bandwidthGbps)
    if (!iface.trim() || !Number.isFinite(us) || us <= 0 || !Number.isFinite(gbps) || gbps <= 0)
      return
    const latencyNs = Math.round(us * 1000)
    // 1e9 ns is the reserved "unset" sentinel (estimator UNSET_LATENCY_NS); an edge
    // carrying it is silently dropped by the impact engine, so reject it here too,
    // mirroring the guard in LinkEditForm.
    if (latencyNs === UNSET_LATENCY_NS) return
    onSubmit(iface.trim(), latencyNs, Math.round(gbps * 1e9))
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-60 space-y-2">
      <div className="text-xs font-medium">
        Move {linkCode} → {targetDeviceCode}
      </div>
      <label className="block text-xs text-muted-foreground">
        New interface
        <input
          autoFocus
          value={iface}
          onChange={(e) => setIface(e.target.value)}
          placeholder="Ethernet1"
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Latency (µs)
        <input
          type="number"
          value={latencyUs}
          onChange={(e) => setLatencyUs(e.target.value)}
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
