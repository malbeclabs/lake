import { useState, useEffect } from 'react'
import type { DraftLink } from './draft'
import { UNSET_LATENCY_NS } from './estimator'

export function LinkEditForm({
  link,
  onSubmit,
  onCancel,
}: {
  link: DraftLink
  onSubmit: (latencyNs: number, bandwidthBps: number) => void
  onCancel: () => void
}) {
  const [latencyUs, setLatencyUs] = useState(String(link.latency_us ?? 0))
  const [bandwidthGbps, setBandwidthGbps] = useState(
    String((link.bandwidth_bps ?? 0) / 1e9)
  )
  const [error, setError] = useState<string | null>(null)

  // Re-seed the fields whenever a different link is selected. Without this, the
  // form would keep one link's edited state and stage it under the next link's pk
  // (cross-link data corruption). Keyed on link.pk so in-progress typing on the
  // SAME link is never clobbered by an unrelated draft rebuild.
  useEffect(() => {
    setLatencyUs(String(link.latency_us ?? 0))
    setBandwidthGbps(String((link.bandwidth_bps ?? 0) / 1e9))
    setError(null)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [link.pk])

  const submit = () => {
    const us = Number(latencyUs)
    const gbps = Number(bandwidthGbps)
    if (!Number.isFinite(us) || us <= 0) {
      setError('Latency must be a positive number.')
      return
    }
    if (!Number.isFinite(gbps) || gbps <= 0) {
      setError('Bandwidth must be a positive number.')
      return
    }
    const latencyNs = Math.round(us * 1000)
    // 1e9 ns is the reserved "unset" sentinel; an edge carrying it is silently
    // dropped by the impact engine, so reject it here (SC / estimator UNSET_LATENCY_NS).
    if (latencyNs === UNSET_LATENCY_NS) {
      setError('That latency (1,000,000 µs) is reserved as the unset value. Choose another.')
      return
    }
    setError(null)
    onSubmit(latencyNs, Math.round(gbps * 1e9))
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-56 space-y-2">
      <div className="text-xs font-medium">Edit {link.code}</div>
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
      {error && (
        <div role="alert" className="text-[11px] text-red-600 dark:text-red-400">
          {error}
        </div>
      )}
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          className="flex-1 px-2 py-1 text-xs rounded bg-accent text-accent-foreground hover:bg-accent/90"
        >
          Save
        </button>
        <button
          onClick={onCancel}
          className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80"
        >
          Cancel
        </button>
      </div>
    </div>
  )
}
