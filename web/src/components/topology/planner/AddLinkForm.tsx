import { useState } from 'react'
import { UNSET_LATENCY_NS } from './estimator'

export function AddLinkForm({
  sourceCode,
  targetCode,
  suggestedLatencyUs,
  estimateSource,
  onSubmit,
  onCancel,
}: {
  sourceCode: string
  targetCode: string
  suggestedLatencyUs: number
  estimateSource: 'copied' | 'great_circle' | 'manual'
  onSubmit: (v: {
    latencyNs: number
    bandwidthBps: number
    sideAIface: string
    sideZIface: string
    estimateSource: 'copied' | 'great_circle' | 'manual'
    linkType: 'WAN' | 'DZX'
  }) => void
  onCancel: () => void
}) {
  const [latencyUs, setLatencyUs] = useState(String(suggestedLatencyUs || ''))
  const [bandwidthGbps, setBandwidthGbps] = useState('10')
  const [sideAIface, setSideAIface] = useState('')
  const [sideZIface, setSideZIface] = useState('')
  const [linkType, setLinkType] = useState<'WAN' | 'DZX'>('WAN')
  const [touched, setTouched] = useState(false)

  const us = Number(latencyUs)
  const gbps = Number(bandwidthGbps)
  const latencyNs = Math.round(us * 1000)
  // Global constraint: 1e9 ns is the reserved "unset" sentinel (api unsetLatencyNs /
  // estimator.UNSET_LATENCY_NS). An edge carrying it is silently dropped by the
  // impact engine, so a new link must never be allowed to save it either -- same
  // guard as LinkEditForm and MoveLinkEndForm.
  const sentinelBlocked = Number.isFinite(us) && us > 0 && latencyNs === UNSET_LATENCY_NS
  const valid =
    Number.isFinite(us) &&
    us > 0 &&
    !sentinelBlocked &&
    Number.isFinite(gbps) &&
    gbps > 0 &&
    sideAIface.trim() !== '' &&
    sideZIface.trim() !== ''

  const submit = () => {
    setTouched(true)
    if (!valid) return
    onSubmit({
      latencyNs,
      bandwidthBps: Math.round(gbps * 1e9),
      sideAIface: sideAIface.trim(),
      sideZIface: sideZIface.trim(),
      // If the operator changed the pre-filled value, it becomes a manual estimate.
      estimateSource: us === suggestedLatencyUs ? estimateSource : 'manual',
      linkType,
    })
  }

  return (
    <div className="bg-card border border-border rounded-md shadow-lg p-3 w-64 space-y-2">
      <div className="text-xs font-medium">
        New link {sourceCode} ↔ {targetCode}
      </div>
      <div className="grid grid-cols-2 gap-2">
        <label className="block text-xs text-muted-foreground">
          {sourceCode} iface
          <input
            value={sideAIface}
            onChange={(e) => setSideAIface(e.target.value)}
            placeholder="Ethernet1"
            className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
          />
        </label>
        <label className="block text-xs text-muted-foreground">
          {targetCode} iface
          <input
            value={sideZIface}
            onChange={(e) => setSideZIface(e.target.value)}
            placeholder="Ethernet1"
            className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
          />
        </label>
      </div>
      <label className="block text-xs text-muted-foreground">
        Latency (µs) — required{' '}
        <span className="text-[10px] uppercase tracking-wide">({estimateSource})</span>
        <input
          type="number"
          value={latencyUs}
          onChange={(e) => setLatencyUs(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Bandwidth (Gbps) — required
        <input
          type="number"
          value={bandwidthGbps}
          onChange={(e) => setBandwidthGbps(e.target.value)}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        />
      </label>
      <label className="block text-xs text-muted-foreground">
        Link type
        <select
          value={linkType}
          onChange={(e) => setLinkType(e.target.value as 'WAN' | 'DZX')}
          className="mt-1 w-full px-2 py-1 text-sm bg-muted border border-border rounded"
        >
          <option value="WAN">WAN</option>
          <option value="DZX">DZX</option>
        </select>
      </label>
      {touched && sentinelBlocked && (
        <p role="alert" className="text-[11px] text-red-500">
          That latency (1,000,000 µs) is reserved as the unset value. Choose another.
        </p>
      )}
      {touched && !valid && !sentinelBlocked && (
        <p className="text-[11px] text-red-500">
          Latency, bandwidth and both interfaces are required.
        </p>
      )}
      <div className="flex gap-2 pt-1">
        <button
          onClick={submit}
          className="flex-1 px-2 py-1 text-xs rounded bg-accent text-accent-foreground hover:bg-accent/90"
        >
          Add link
        </button>
        <button onClick={onCancel} className="px-2 py-1 text-xs rounded bg-muted hover:bg-muted/80">
          Cancel
        </button>
      </div>
    </div>
  )
}
